package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/voyager-db/raftx/quorum"
	pb "github.com/voyager-db/raftx/raftpb"
)

// -----------------------------------------------------------------------------
// Helpers (white-box) — restart-style bootstrap so internal transitions are safe
// -----------------------------------------------------------------------------

// makeRestartedFastRaft seeds Storage with a snapshot+hardstate FIRST (per docs),
// then constructs a raft with EnableFastPath=true. This avoids unstable bounds
// panics when the leader appends its initial no-op.
func makeRestartedFastRaft(t *testing.T, self uint64, voters []uint64) *raft {
	t.Helper()

	ms := NewMemoryStorage()
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index: 1, Term: 1,
			ConfState: pb.ConfState{Voters: voters},
		},
	}
	if err := ms.ApplySnapshot(snap); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	ms.SetHardState(pb.HardState{Term: 1, Commit: 1})

	cfg := &Config{
		ID:                self,
		ElectionTick:      10,
		HeartbeatTick:     1,
		Storage:           ms,
		MaxSizePerMsg:     1 << 20,
		MaxInflightMsgs:   256,
		CheckQuorum:       true,
		PreVote:           true,
		ReadOnlyOption:    ReadOnlySafe,
		EnableFastPath:    true, // turn Fast Raft on
		StepDownOnRemoval: true,
	}
	r := newRaft(cfg)
	// newRaft() already read InitialState; enable fast features in the log.
	r.raftLog.enableFastRaft()
	return r
}

// -----------------------------------------------------------------------------
// REGRESSION TESTS (white-box): unknown-voter hints must NOT count as votes
// -----------------------------------------------------------------------------

func TestFastRaft_IgnoreUnknownVoterInHints(t *testing.T) {
	// 5-voter config to mirror the Fast-Raft paper examples (fast quorum logic).
	voters := []uint64{1, 2, 3, 4, 5}
	r := makeRestartedFastRaft(t, 1, voters)

	// Seed one self-approved hint captured while a follower.
	const idx = uint64(5)
	h := pb.SelfApprovedHint{
		Index:       idx,
		Term:        2,
		EntryDigest: []byte("digest-xyz"),
	}
	r.selfApprovedHints = append(r.selfApprovedHints, h)

	// Become leader; this copies hints into possibleEntries via processSelfApprovedHint.
	r.becomeCandidate()
	r.becomeLeader()

	ctr := r.possibleEntries[idx]
	if ctr == nil {
		t.Fatalf("expected counter at index %d", idx)
	}

	// 1) Recording a vote from an UNKNOWN id (0) must be rejected by the counter.
	if accepted := ctr.RecordVote(0 /* unknown id */, string(h.EntryDigest)); accepted {
		t.Fatalf("counter accepted a vote from unknown voter id=0")
	}

	// 2) Tracker must not have a phantom Progress for id=0.
	if _, ok := r.trk.Progress[0]; ok {
		t.Fatalf("phantom voter id=0 appeared in tracker.Progress")
	}

	// 3) With only a single hint (no real votes), there is no quorum.
	tally := ctr.Tally()
	if tally.HasClassicQuorum {
		t.Fatalf("classic quorum should NOT be satisfied by a single hint")
	}
	if tally.HasFastQuorum {
		t.Fatalf("fast quorum should NOT be satisfied by a single hint")
	}
}

func TestFastRaft_NoGrowthFromRepeatedHints(t *testing.T) {
	voters := []uint64{1, 2, 3, 4, 5}

	// Repeat: each round simulates "fresh bootstrap → follower collected hints → new leader".
	for round := 0; round < 10; round++ {
		r := makeRestartedFastRaft(t, 1, voters) // fresh node at (term,commit)=(1,1)

		// Baseline: ensure there is no Progress[0] before we start.
		if _, ok := r.trk.Progress[0]; ok {
			t.Fatalf("precondition: unexpected Progress for id=0 (round %d)", round)
		}

		// Add many hints across indices (what we 'collected as follower').
		for i := 0; i < 50; i++ {
			r.selfApprovedHints = append(r.selfApprovedHints, pb.SelfApprovedHint{
				Index:       uint64(10 + round*100 + i),
				Term:        2,
				EntryDigest: []byte("digest"),
			})
		}

		// Elect this node leader for the round.
		r.becomeCandidate()
		r.becomeLeader()

		// Must never create a phantom voter with id=0.
		if _, ok := r.trk.Progress[0]; ok {
			t.Fatalf("phantom Progress for id=0 detected after round %d", round)
		}

		// Spot-check a few counters: hints alone must not satisfy any quorum.
		for off := 0; off < 5; off++ {
			idx := uint64(10 + round*100 + off)
			if ctr := r.possibleEntries[idx]; ctr != nil {
				tl := ctr.Tally()
				if tl.HasClassicQuorum || tl.HasFastQuorum {
					t.Fatalf("round %d idx %d: quorum unexpectedly satisfied by hints", round, idx)
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------
// DOCS-PATTERN BOOTSTRAP (black-box): StartNode a 3-node cluster from scratch
// and ensure it elects a leader and commits a proposal with EnableFastPath=true.
// -----------------------------------------------------------------------------

// small harness that wires 3 StartNode nodes together in-process.
type nodeRunner struct {
	id   uint64
	n    Node
	st   *MemoryStorage
	apMu sync.Mutex
	appl uint64 // last applied index (for smoke assertions)
}

type cluster struct {
	nodes     map[uint64]*nodeRunner
	deliverCh chan pb.Message
	stopCh    chan struct{}
	wg        sync.WaitGroup

	mu      sync.Mutex
	leader  uint64
	started bool
}

func newCluster(t *testing.T, ids ...uint64) *cluster {
	t.Helper()
	c := &cluster{
		nodes:     make(map[uint64]*nodeRunner),
		deliverCh: make(chan pb.Message, 1024),
		stopCh:    make(chan struct{}),
	}
	// Start all nodes with the full peer list (as in docs).
	peers := make([]Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, Peer{ID: id})
	}

	for _, id := range ids {
		st := NewMemoryStorage()
		cfg := &Config{
			ID:              id,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         st,
			MaxSizePerMsg:   4096,
			MaxInflightMsgs: 256,
			CheckQuorum:     true,
			PreVote:         true,
			// Fast Raft ON to resemble the system under test.
			EnableFastPath: true,
		}
		n := StartNode(cfg, peers)
		r := &nodeRunner{id: id, n: n, st: st}
		c.nodes[id] = r

		// Ready loop per node.
		c.wg.Add(1)
		go func(rn *nodeRunner) {
			defer c.wg.Done()
			for {
				select {
				case <-c.stopCh:
					return
				case rd := <-rn.n.Ready():
					// persist Entries then HardState (per docs)
					if len(rd.Entries) > 0 {
						if err := rn.st.Append(rd.Entries); err != nil {
							t.Errorf("append: %v", err)
						}
					}
					if !IsEmptyHardState(rd.HardState) {
						rn.st.SetHardState(rd.HardState)
					}
					// deliver messages to cluster router
					for _, m := range rd.Messages {
						select {
						case c.deliverCh <- m:
						case <-c.stopCh:
							return
						}
					}
					// apply committed entries
					if !IsEmptySnap(rd.Snapshot) {
						if err := rn.st.ApplySnapshot(rd.Snapshot); err != nil {
							t.Errorf("apply snapshot: %v", err)
						}
					}
					for _, ent := range rd.CommittedEntries {
						switch ent.Type {
						case pb.EntryConfChange:
							var cc pb.ConfChange
							_ = cc.Unmarshal(ent.Data)
							rn.n.ApplyConfChange(cc)
						case pb.EntryConfChangeV2:
							var cc pb.ConfChangeV2
							_ = cc.Unmarshal(ent.Data)
							rn.n.ApplyConfChange(cc)
						default:
							// record last applied index
							rn.apMu.Lock()
							if ent.Index > rn.appl {
								rn.appl = ent.Index
							}
							rn.apMu.Unlock()
						}
					}
					rn.n.Advance()
				}
			}
		}(r)
	}

	// router goroutine: forwards messages to recipients; tracks leader via heartbeats.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stopCh:
				return
			case m := <-c.deliverCh:
				// track leader from heartbeats/apps
				if m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp {
					c.mu.Lock()
					c.leader = m.From
					c.mu.Unlock()
				}
				dst := c.nodes[m.To]
				if dst == nil {
					// unknown dst (and importantly ensure no id==0 ever shows up)
					if m.To == 0 {
						// This should never happen; make it obvious if it does.
						panic("message addressed to node 0")
					}
					continue
				}
				_ = dst.n.Step(context.Background(), m)
			}
		}
	}()

	c.started = true
	return c
}

func (c *cluster) stop() {
	if !c.started {
		return
	}
	close(c.stopCh)
	c.wg.Wait()
}

func (c *cluster) tick() {
	for _, r := range c.nodes {
		r.n.Tick()
	}
}

func (c *cluster) currentLeader() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *cluster) applied(id uint64) uint64 {
	r := c.nodes[id]
	r.apMu.Lock()
	defer r.apMu.Unlock()
	return r.appl
}

// -----------------------------------------------------------------------------
// Smoke test: StartNode bootstrap + propose/commit under EnableFastPath=true.
// -----------------------------------------------------------------------------

func TestFastRaft_Bootstrap3Nodes_StartNode_Smoke(t *testing.T) {
	c := newCluster(t, 0x01, 0x02, 0x03)
	defer c.stop()

	// Tick until a leader emerges.
	deadline := time.Now().Add(3 * time.Second)
	for c.currentLeader() == 0 && time.Now().Before(deadline) {
		c.tick()
		time.Sleep(2 * time.Millisecond)
	}
	ld := c.currentLeader()
	if ld == 0 {
		t.Fatalf("no leader elected")
	}

	// Propose a few entries from the (observed) leader and wait for them to apply.
	const toPropose = 3
	for i := 0; i < toPropose; i++ {
		_ = c.nodes[ld].n.Propose(context.Background(), []byte("x"))
	}

	// Drive the system until everyone applies >0 entries (should be fast).
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allOK := true
		for id := range c.nodes {
			if c.applied(id) == 0 {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		c.tick()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("entries did not apply to all nodes in time; applied: n1=%d n2=%d n3=%d",
		c.applied(0x01), c.applied(0x02), c.applied(0x03))
}

// -----------------------------------------------------------------------------
// (Optional) tiny accessors used by the white-box tests
// -----------------------------------------------------------------------------

// majoritySize returns the number of voters in a MajorityConfig.
func majoritySize(cfg quorum.MajorityConfig) int {
	n := 0
	for range cfg {
		n++
	}
	return n
}

func TestFastRaft_FastPropIndicesShouldAdvanceWhenCommitStalled(t *testing.T) {
	r := makeRestartedFastRaft(t, 1, []uint64{1, 2, 3, 4, 5})

	// Become leader.
	r.becomeCandidate()
	r.becomeLeader()

	// NEW: enable fast path by committing the leader's no-op in this term.
	// We still keep commit "stalled" after this initial bootstrap commit.
	r.raftLog.commitTo(r.raftLog.lastIndex())

	// Call broadcastFastProp twice with one entry each.
	r.broadcastFastProp([]pb.Entry{{Type: pb.EntryNormal, Data: []byte("a")}})
	firstIdx := r.raftLog.committed + 1

	// Capture the actual assigned index via possibleEntries keys.
	if _, ok := r.possibleEntries[firstIdx]; !ok {
		t.Fatalf("expected counter at %d after first fast prop", firstIdx)
	}

	r.broadcastFastProp([]pb.Entry{{Type: pb.EntryNormal, Data: []byte("b")}})

	// Expect the *second* proposal to use a *new* index (monotonic).
	secondIdx := firstIdx + 1
	if _, ok := r.possibleEntries[secondIdx]; !ok {
		t.Fatalf("second fast prop reused index %d; expected new index %d", firstIdx, secondIdx)
	}
}


func TestFastRaft_FastProp_NoDigestBlowup_AndWindowBound(t *testing.T) {
	// Fresh 5-voter group; leader elected; commit will remain stalled.
	r := makeRestartedFastRaft(t, 1, []uint64{1, 2, 3, 4, 5})
	r.becomeCandidate()
	r.becomeLeader()

	r.raftLog.commitTo(r.raftLog.lastIndex())

	// Now the seed proposal will go through the fast path.
	r.broadcastFastProp([]pb.Entry{{Type: pb.EntryNormal, Data: []byte("seed")}})
	firstIdx := r.raftLog.committed + 1
	if _, ok := r.possibleEntries[firstIdx]; !ok {
		t.Fatalf("expected counter at %d after seed fast prop", firstIdx)
	}

	// Flood N proposals with unique payloads, while commit is stuck.
	const N = 5000
	for i := 0; i < N; i++ {
		payload := []byte(fmt.Sprintf("p-%08d", i))
		r.broadcastFastProp([]pb.Entry{{Type: pb.EntryNormal, Data: payload}})
	}

	// Gather distribution stats.
	perFirst := 0
	if m := r.fastPropStore[firstIdx]; m != nil {
		perFirst = len(m) // digest count at the first index
	}
	totalDigests := 0
	totalIndices := 0
	maxPerIndex := 0
	minIdx, maxIdx := uint64(^uint64(0)), uint64(0)

	for idx, m := range r.fastPropStore {
		if m == nil {
			continue
		}
		totalIndices++
		if idx < minIdx {
			minIdx = idx
		}
		if idx > maxIdx {
			maxIdx = idx
		}
		n := len(m)
		totalDigests += n
		if n > maxPerIndex {
			maxPerIndex = n
		}
	}

	// 1) No per-index blow-up (dog-pile).
	// Expect very few digests per index (self-vote + maybe a small retry).
	const perIndexCeil = 8
	if perFirst > perIndexCeil || maxPerIndex > perIndexCeil {
		t.Fatalf("digest concentration too high: firstIdx=%d perFirst=%d maxPerIndex=%d",
			firstIdx, perFirst, maxPerIndex)
	}

	// 2) Indices must advance monotonically and cover a wide range.
	if totalIndices < 100 {
		t.Fatalf("indices did not advance as expected; only %d distinct indices populated", totalIndices)
	}
	if maxIdx < minIdx {
		t.Fatalf("bad index range: minIdx=%d maxIdx=%d", minIdx, maxIdx)
	}

	// 3) State must be bounded by the fast window.
	// We allow a little slack to avoid test brittleness.
	const slack = 64
	wantMax := fastWindow + slack
	if totalIndices > wantMax {
		t.Fatalf("too many indices retained: got %d, want <= %d (fastWindow=%d)",
			totalIndices, wantMax, fastWindow)
	}
	if totalDigests > wantMax {
		t.Fatalf("too many digests retained: got %d, want <= %d (fastWindow=%d)",
			totalDigests, wantMax, fastWindow)
	}

	// 4) Optional: the spread should be contiguous-ish within the window.
	if int(maxIdx-minIdx+1) < totalIndices {
		t.Fatalf("unexpected holes: range [%d,%d] len=%d but totalIndices=%d",
			minIdx, maxIdx, maxIdx-minIdx+1, totalIndices)
	}
}

func TestFastRaft_FastStateBoundedWithoutCommit(t *testing.T) {
	r := makeRestartedFastRaft(t, 1, []uint64{1, 2, 3, 4, 5})
	r.becomeCandidate()
	r.becomeLeader()

	// Flood proposals; no votes -> commit never advances.
	const N = 50000
	for i := 0; i < N; i++ {
		r.broadcastFastProp([]pb.Entry{{Type: pb.EntryNormal, Data: []byte("x")}})
	}

	// Enforce a bound ~fastWindow on indices and memory.
	// possibleEntries should have at most ~fastWindow keys around committed+1.
	const slack = 64
	if len(r.possibleEntries) > fastWindow+slack {
		t.Fatalf("possibleEntries grew unbounded: got %d, want <= %d", len(r.possibleEntries), fastWindow+slack)
	}
}


// ===== Boot-stability tests: gate, backpressure, follower bound =====

// helper: count messages of a given type in the raft's outbound queues.
func countMsgs(r *raft, mt pb.MessageType) int {
	n := 0
	for _, m := range r.msgs {
		if m.Type == mt {
			n++
		}
	}
	for _, m := range r.msgsAfterAppend {
		if m.Type == mt {
			n++
		}
	}
	return n
}

// 1) Gate: no fast-broadcast before the leader has a commit in its term.
//    - Before committing the leader no-op, proposals must go via classic (no MsgFastProp).
//    - After committing in this term, fast proposals appear.
func TestFastRaft_GateFastUntilLeaderTermCommitted(t *testing.T) {
	r := makeRestartedFastRaft(t, 1, []uint64{1, 2, 3})

	// Elect leader; leader no-op is appended but NOT yet committed in this term.
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()
	r.msgs, r.msgsAfterAppend = nil, nil

	// Propose once (goes through stepLeader -> broadcastFastProp).
	// Gate should force classic replication (no fast props yet).
	if err := r.Step(pb.Message{
		Type: pb.MsgProp, From: r.id, Entries: []pb.Entry{{Type: pb.EntryNormal, Data: []byte("boot-1")}},
	}); err != nil {
		t.Fatalf("prop boot-1: %v", err)
	}
	if n := countMsgs(r, pb.MsgFastProp); n != 0 {
		t.Fatalf("unexpected fast proposals before leader-term commit: %d", n)
	}
	if n := countMsgs(r, pb.MsgApp); n == 0 {
		t.Fatalf("expected classic MsgApp before leader-term commit")
	}

	// Now simulate that the leader has committed in its term (boot barrier).
	r.raftLog.commitTo(r.raftLog.lastIndex())
	r.msgs, r.msgsAfterAppend = nil, nil

	// Propose again; now fast path is allowed.
	if err := r.Step(pb.Message{
		Type: pb.MsgProp, From: r.id, Entries: []pb.Entry{{Type: pb.EntryNormal, Data: []byte("boot-2")}},
	}); err != nil {
		t.Fatalf("prop boot-2: %v", err)
	}
	if n := countMsgs(r, pb.MsgFastProp); n == 0 {
		t.Fatalf("expected fast proposals after leader-term commit")
	}
}

// 2) Backpressure: when the fast window is nearly full, split the batch.
//    - Only the remaining capacity goes fast (possibleEntries grows by <= cap).
//    - Overflow routes via classic (MsgApp present), keeping progress.
func TestFastRaft_BackpressureSplitsBatch(t *testing.T) {
	r := makeRestartedFastRaft(t, 1, []uint64{1, 2, 3})

	// Become leader and commit no-op so fast path is enabled.
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()
	r.raftLog.commitTo(r.raftLog.lastIndex())

	frontier := r.raftLog.committed + 1
	// Leave exactly two slots of capacity in the fast window.
	r.fastNext = frontier + fastWindow - 2

	// Batch of 5 (2 should go fast, 3 overflow classic).
	batch := make([]pb.Entry, 5)
	for i := range batch {
		batch[i] = pb.Entry{Type: pb.EntryNormal, Data: []byte("x")}
	}
	r.msgs, r.msgsAfterAppend = nil, nil

	if err := r.Step(pb.Message{Type: pb.MsgProp, From: r.id, Entries: batch}); err != nil {
		t.Fatalf("prop batch: %v", err)
	}

	// Count how many fast indices materialized near the tail.
	fastCreated := 0
	for idx := range r.possibleEntries {
		// anything >= original r.fastNext-5 and within window is counted
		if idx >= frontier && idx < frontier+uint64(fastWindow) {
			fastCreated++
		}
	}
	if fastCreated > 2 {
		t.Fatalf("backpressure failed: created %d fast indices, want <= 2", fastCreated)
	}
	// Expect classic replication for the overflow.
	if n := countMsgs(r, pb.MsgApp); n == 0 {
		t.Fatalf("expected classic MsgApp for overflow when fast window is full")
	}
}

// 3) Follower bound: pendingSelf stays capped and within active window.
//    - Flood out-of-order fast proposals far ahead.
//    - Verify pendingSelf size <= cap and indices are within [committed+1, committed+fastWindow].
func TestFastRaft_FollowerPendingSelfBound(t *testing.T) {
	r := makeRestartedFastRaft(t, 2, []uint64{1, 2, 3}) // follower id=2
	// Simulate a leader id for routing votes (not used heavily here).
	r.lead = 1

	// Flood far-future proposals (beyond the contiguous tail).
	start := r.raftLog.lastIndex() + fastWindow + 100
	for i := 0; i < 5000; i++ {
		e := pb.Entry{
			Type:  pb.EntryNormal,
			Index: start + uint64(i),
			Term:  r.Term,
			Data:  []byte("flood"),
		}
		m := pb.Message{Type: pb.MsgFastProp, From: r.lead, To: r.id, Entries: []pb.Entry{e}}
		r.handleFastProp(m)
	}

	// Assert follower buffer cap (adjust if you picked a different cap than 1024).
	if len(r.pendingSelf) > 1024 {
		t.Fatalf("pendingSelf grew unbounded: got %d, want <= 1024", len(r.pendingSelf))
	}
	// And ensure buffered indexes remain within the active window.
	floor := r.raftLog.committed + 1
	ceil := floor + fastWindow
	for idx := range r.pendingSelf {
		if idx < floor || idx > ceil {
			t.Fatalf("pendingSelf contains out-of-window index %d not in [%d,%d]", idx, floor, ceil)
		}
	}
}
