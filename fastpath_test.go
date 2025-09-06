package raft

import (
	"math"
	"testing"

	"github.com/voyager-db/raftx/quorum"
	pb "github.com/voyager-db/raftx/raftpb"
	"github.com/voyager-db/raftx/tracker"
)

// ---------- helpers ----------

func baseConfigFast(id uint64) *Config {
	return &Config{
		ID:                  id,
		ElectionTick:        10,
		HeartbeatTick:       1,
		Storage:             newTestMemoryStorage(), // your in-mem storage helper
		MaxInflightMsgs:     256,
		MaxSizePerMsg:       math.MaxUint64,
		EnableFastPath:      true,
		FastOpenIndexWindow: 2,
		PreVote:             true,
		CheckQuorum:         false,
	}
}

func u64p(v uint64) *uint64 { return &v }

// Append n entries and mark them committed.
func mustAppendCommitted(r *raft, n int) {
	last := r.raftLog.lastIndex()
	var ents []pb.Entry
	for i := 0; i < n; i++ {
		ents = append(ents, pb.Entry{Index: last + 1 + uint64(i), Term: r.Term})
	}
	r.raftLog.append(ents...)
	r.raftLog.commitTo(r.raftLog.lastIndex())
}

// Append n entries without changing commit.
func mustAppendUncommitted(r *raft, n int) {
	last := r.raftLog.lastIndex()
	var ents []pb.Entry
	for i := 0; i < n; i++ {
		ents = append(ents, pb.Entry{Index: last + 1 + uint64(i), Term: r.Term})
	}
	r.raftLog.append(ents...)
}

// Prime a 1-voter config so becomeLeader() has a Progress entry.
func primeSingleVoter(r *raft, id uint64) {
	cfg := tracker.Config{}
	cfg.Voters[0] = quorum.MajorityConfig{r.id: struct{}{}}
	pm := tracker.ProgressMap{
		r.id: &tracker.Progress{
			Match:     r.raftLog.lastIndex(),
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.trk.MaxInflight, r.trk.MaxInflightBytes),
		},
	}
	r.switchToConfig(cfg, pm)
}

// ---------- Milestone 3: follower self-insert ----------

func TestFollowerSelfInsertWithinWindow(t *testing.T) {
	r := newRaft(baseConfigFast(2))
	r.becomeFollower(1, None)
	mustAppendCommitted(r, 5) // committed==5
	li := r.raftLog.lastIndex()
	idx := li + 1

	// Fast proposal at idx
	m := pb.Message{
		Type:  pb.MsgFastProp,
		Index: idx,
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("v1"),
			ContentId: []byte("cid-v1"),
		}},
	}
	if err := r.Step(m); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := r.raftLog.lastIndex(); got != idx {
		t.Fatalf("lastIndex=%d want %d", got, idx)
	}
	ents, _ := r.raftLog.slice(idx, idx+1, noLimit)
	if len(ents) != 1 {
		t.Fatalf("missing self-insert at %d", idx)
	}
	if r.raftLog.committed != li {
		t.Fatalf("commit changed: %d -> %d", li, r.raftLog.committed)
	}
}

func TestFollowerNormalizesIndexAndAppendsOnlyIfContiguous(t *testing.T) {
	cfg := baseConfigFast(2)
	cfg.EnableFastPath = true
	r := newRaft(cfg)

	// Follower that knows there's a leader, with some committed prefix.
	r.becomeFollower(1, 1)
	mustAppendCommitted(r, 5) // committed = 5, so follower's k = 6

	// Make the log non-contiguous wrt k: lastIndex != k-1
	// Append two speculative entries so li+1 != k.
	r.raftLog.append(pb.Entry{Index: 6, Term: 0, Type: pb.EntryNormal})
	r.raftLog.append(pb.Entry{Index: 7, Term: 0, Type: pb.EntryNormal})
	// Now committed=5, k=6, but li=7 (so idx==6 != li+1==8) → follower must NOT append.

	m := pb.Message{
		Type:  pb.MsgFastProp,
		From:  1,     // simulate leader broadcast
		Index: 10000, // ignored by follower
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			ContentId: []byte("cid-out"),
			Data:      []byte("x"),
		}},
	}
	liBefore := r.raftLog.lastIndex()
	if err := r.Step(m); err != nil {
		t.Fatalf("step: %v", err)
	}
	liAfter := r.raftLog.lastIndex()
	if liAfter != liBefore {
		t.Fatalf("follower appended but should not (li before %d after %d)", liBefore, liAfter)
	}

	// It should still send a vote to the leader for its normalized k (= 6).
	found := false
	for _, msg := range r.msgs {
		if msg.Type == pb.MsgFastVote && msg.To == r.lead {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("follower did not send MsgFastVote")
	}
}

func TestFollowerDoesNotOverwriteOnFastProp(t *testing.T) {
	r := newRaft(baseConfigFast(2))
	r.becomeFollower(1, None)
	mustAppendCommitted(r, 5)
	mustAppendUncommitted(r, 1) // lastIndex=6

	m := pb.Message{
		Type: pb.MsgFastProp, Index: 6,
		Entries: []pb.Entry{{Type: pb.EntryNormal, ContentId: []byte("different")}},
	}
	if err := r.Step(m); err != nil {
		t.Fatalf("step: %v", err)
	}
	ents, _ := r.raftLog.slice(6, 7, noLimit)
	if len(ents) != 1 {
		t.Fatalf("unexpected slice len at 6")
	}
}

// ---------- Milestone 4: vote plumbing & aggregation ----------
func setVoters(r *raft, ids ...uint64) {
	// Rebuild the tracker with a fresh config (minimal helper for tests).
	r.trk = tracker.MakeProgressTracker(r.trk.MaxInflight, r.trk.MaxInflightBytes)
	for _, id := range ids {
		r.trk.Config.Voters[0][id] = struct{}{}
		r.trk.Progress[id] = &tracker.Progress{}
	}
}

func TestFollowerSendsFastVoteToLeaderAndLeaderTallies(t *testing.T) {
	// Leader (1)
	cfgL := baseConfigFast(1)
	rl := newRaft(cfgL)
	setVoters(rl, 1, 2)

	// Make 1 leader with some committed prefix.
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 5) // committed=5, so leader k=6

	// Follower (2) that knows its leader and has the same committed prefix.
	cfgF := baseConfigFast(2)
	cfgF.EnableFastPath = true
	rf := newRaft(cfgF)
	setVoters(rf, 1, 2)
	rf.becomeFollower(rl.Term, rl.id)
	rf.lead = rl.id
	mustAppendCommitted(rf, 5)

	// Build a fast proposal broadcast to the follower (index ignored now).
	prop := pb.Message{
		Type:  pb.MsgFastProp,
		From:  rl.id, // simulate leader broadcast
		Index: 0,     // ignored by follower; it will vote at its own k
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("v1"),
			ContentId: []byte("cid-v1"),
		}},
	}
	if err := rf.Step(prop); err != nil {
		t.Fatalf("follower step: %v", err)
	}

	// Collect the vote emitted by the follower.
	var vote pb.Message
	found := false
	for _, msg := range rf.msgs {
		if msg.Type == pb.MsgFastVote {
			vote = msg
			found = true
			break
		}
	}
	if !found {
		for _, msg := range rf.msgsAfterAppend {
			if msg.Type == pb.MsgFastVote {
				vote = msg
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("follower did not send MsgFastVote")
	}
	if vote.To != rl.id {
		t.Fatalf("vote sent to %d want %d", vote.To, rl.id)
	}
	// We no longer require ref.Index to be set; leader ignores it.
	if len(vote.FastVotes) != 1 || vote.FastVotes[0] == nil || string(vote.FastVotes[0].ContentId) != "cid-v1" {
		t.Fatalf("vote content bad: %#v", vote.FastVotes)
	}

	// Deliver vote to the leader.
	if err := rl.Step(vote); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	// Leader tallies at its own k (= committed+1), not at the follower's idx.
	k := rl.raftLog.committed + 1
	if rl.possibleEntries == nil || rl.possibleEntries[k] == nil {
		t.Fatalf("no tally bucket for leader k=%d", k)
	}
	set, ok := rl.possibleEntries[k]["cid-v1"]
	if !ok || len(set) != 1 {
		t.Fatalf("unexpected tally at k=%d for cid-v1: %#v", k, rl.possibleEntries[k])
	}
}

func TestLeaderTallyDedupAndMove(t *testing.T) {
	rl := newRaft(baseConfigFast(1))
	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 3)

	idx := rl.raftLog.committed + 1
	voter := uint64(2)

	sendVote := func(cid string) {
		_ = rl.Step(pb.Message{
			Type: pb.MsgFastVote,
			From: voter,
			FastVotes: []*pb.EntryRef{{
				Index:     u64p(idx),
				ContentId: []byte(cid),
			}},
		})
	}

	sendVote("cid-a")
	sendVote("cid-a")
	if got := len(rl.possibleEntries[idx]["cid-a"]); got != 1 {
		t.Fatalf("expected 1 vote for cid-a, got %d", got)
	}

	sendVote("cid-b")
	if len(rl.possibleEntries[idx]["cid-a"]) != 0 || len(rl.possibleEntries[idx]["cid-b"]) != 1 {
		t.Fatalf("move failed: A=%d B=%d",
			len(rl.possibleEntries[idx]["cid-a"]), len(rl.possibleEntries[idx]["cid-b"]))
	}
}

func TestLeaderCachesFastPropPayload(t *testing.T) {
	cfg := baseConfigFast(1)
	rl := newRaft(cfg)

	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 3) // committed = 3

	// Build a fast-prop from a non-leader sender; leader will cache but not fallback.
	msg := pb.Message{
		Type:  pb.MsgFastProp,
		From:  2, // ← not the leader (avoid fallback)
		Index: 0, // ← ignored by leader now
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("payload"),
			ContentId: []byte("cid-x"),
		}},
	}
	if err := rl.Step(msg); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	// Leader always buckets at its current k; recompute after Step.
	k := rl.raftLog.committed + 1
	bucket := rl.proposalCache[k]
	if bucket == nil {
		t.Fatalf("cache bucket missing at k=%d", k)
	}
	e, ok := bucket["cid-x"]
	if !ok || string(e.Data) != "payload" {
		t.Fatalf("cache miss or wrong payload: ok=%v data=%q", ok, e.Data)
	}
}

func TestLeaderFastPropFallbackInstallsLeaderPayloadAndClearsCache(t *testing.T) {
	cfg := baseConfigFast(1)
	cfg.EnableFastPath = true
	rl := newRaft(cfg)

	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 3)

	// Self fast-prop (From=None → normalized to leader). Fallback should install at k, then clear cache.
	msg := pb.Message{
		Type:  pb.MsgFastProp,
		From:  None, // local self-prop
		Index: 0,    // ignored by leader
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("payload"),
			ContentId: []byte("cid-x"),
		}},
	}
	if err := rl.Step(msg); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	// After fallback, leader should have appended at current k and cleared proposalCache[k].
	k := rl.raftLog.lastIndex() // single-node: append moves lastIndex == k

	// cache should be cleared
	if rl.proposalCache[k] != nil {
		t.Fatalf("expected proposalCache[%d] to be cleared", k)
	}

	// log[k] should be the leader-approved payload
	ents, _ := rl.raftLog.slice(k, k+1, noLimit)
	if len(ents) != 1 || string(ents[0].Data) != "payload" || getOrigin(&ents[0]) != pb.EntryOriginLeader {
		t.Fatalf("log at k=%d not leader-approved payload: %+v", k, ents)
	}
}

func TestLeaderCachesAtLeaderK_IgnoresMsgIndex(t *testing.T) {
	cfg := baseConfigFast(1) // or your helper
	rl := newRaft(cfg)

	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 3) // committed = 3

	// Send a fast-prop from a non-leader sender; leader caches but does not fallback.
	msg := pb.Message{
		Type:  pb.MsgFastProp,
		From:  2,   // not the leader
		Index: 999, // ignored by leader
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("payload-A"),
			ContentId: []byte("cid-A"),
		}},
	}
	if err := rl.Step(msg); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	k := rl.raftLog.committed + 1 // leader buckets at current k
	bucket := rl.proposalCache[k]
	if bucket == nil {
		t.Fatalf("cache bucket missing at k=%d", k)
	}
	e, ok := bucket["cid-A"]
	if !ok || string(e.Data) != "payload-A" {
		t.Fatalf("cache miss or wrong payload: ok=%v data=%q", ok, e.Data)
	}
}

func TestLeaderFallback_InstallsLeaderPayload_AndClearsCache(t *testing.T) {
	cfg := baseConfigFast(1)
	rl := newRaft(cfg)

	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 5) // committed = 5, so k=6

	// Pre-arrival non-leader proposal to simulate concurrent traffic
	nonLeader := pb.Message{
		Type:  pb.MsgFastProp,
		From:  2, // follower
		Index: 0, // ignored by leader now
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("follower-payload"),
			ContentId: []byte("cid-X"),
		}},
	}
	if err := rl.Step(nonLeader); err != nil {
		t.Fatalf("leader step (non-leader): %v", err)
	}

	// Pre-cache leader payload (what etcd does immediately after ProposeFast).
	rl.CacheLeaderFastPayload([]byte("leader-payload"), []byte("cid-X"))

	// Now send leader’s own fast-prop; fallback should install at k and clear cache.
	self := pb.Message{
		Type:  pb.MsgFastProp,
		From:  None, // local self-prop; normalized to r.id
		Index: 0,
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("leader-payload"), // same logical content
			ContentId: []byte("cid-X"),
		}},
	}
	if err := rl.Step(self); err != nil {
		t.Fatalf("leader step (self): %v", err)
	}

	k := rl.raftLog.lastIndex() // single-node: append advances lastIndex == k
	// Cache should be cleared after installing leader-approved decision
	if rl.proposalCache[k] != nil {
		t.Fatalf("expected proposalCache[%d] to be cleared", k)
	}
	// Log[k] must be the leader payload
	ents, _ := rl.raftLog.slice(k, k+1, noLimit)
	if len(ents) != 1 || string(ents[0].Data) != "leader-payload" || getOrigin(&ents[0]) != pb.EntryOriginLeader {
		t.Fatalf("log at k=%d not leader-approved payload: %+v", k, ents)
	}
}
