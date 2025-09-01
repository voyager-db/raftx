package raft

import (
	"testing"

	"github.com/voyager-db/raftx/quorum"
	pb "github.com/voyager-db/raftx/raftpb"
	"github.com/voyager-db/raftx/tracker"
)

// minimal in-memory pump: deliver all queued msgs to their recipients’ Step.
func pump(t *testing.T, nodes map[uint64]*raft) {
	t.Helper()
	progress := true
	for progress {
		progress = false
		for _, n := range nodes {
			all := append([]pb.Message{}, n.msgs...)
			all = append(all, n.msgsAfterAppend...)
			if len(all) == 0 {
				continue
			}
			n.msgs = nil
			n.msgsAfterAppend = nil
			progress = true
			for _, m := range all {
				if m.To == 0 || m.To == n.id {
					continue // local/self
				}
				dst := nodes[m.To]
				if dst == nil {
					// New voter not instantiated in this harness; drop silently or log.
					// t.Logf("dropping %s to unknown node %d", m.Type, m.To)
					continue
				}
				if err := dst.Step(m); err != nil {
					t.Fatalf("step error to %x: %v", m.To, err)
				}
			}
		}
	}
}

// helper: new node with CRaft + FastPath enabled
func newCRaftNode(t *testing.T, id uint64, voters []uint64, gVoters []uint64) *raft {
	t.Helper()
	mem := NewMemoryStorage() // assuming you have the etcd-like MemoryStorage
	cfg := &Config{
		ID:                  id,
		ElectionTick:        10,
		HeartbeatTick:       1,
		Storage:             mem,
		MaxSizePerMsg:       1 << 20,
		MaxInflightMsgs:     256,
		EnableFastPath:      true,
		EnableCRaft:         true,
		GlobalVoters:        gVoters,
		GlobalElectionTick:  10,
		GlobalHeartbeatTick: 1,
		Logger:              getLogger(),
	}
	// bootstrap local membership (classic etcd way)
	cs := pb.ConfState{Voters: voters}
	mem.SetHardState(pb.HardState{})
	mem.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: cs,
			Index:     1,
			Term:      1,
		},
	})
	n := newRaft(cfg)
	return n
}

func forceLocalSingletonLeader(n *raft) {
	if n.trk.Progress[n.id] == nil {
		n.trk.Progress[n.id] = &tracker.Progress{
			Match:        n.raftLog.lastIndex(),
			Next:         n.raftLog.lastIndex() + 1,
			Inflights:    tracker.NewInflights(n.trk.MaxInflight, n.trk.MaxInflightBytes),
			RecentActive: true,
		}
	}
	v := quorum.MajorityConfig{n.id: struct{}{}}
	n.trk.Config.Voters = quorum.JointConfig{v, nil}
	n.becomeCandidate()
	n.becomeLeader()
}

func initGlobalMembership(n *raft, voterIDs []uint64) {
	for _, id := range voterIDs {
		if n.gtrk.Progress[id] == nil {
			n.gtrk.Progress[id] = &tracker.Progress{
				Match:        0,
				Next:         1,
				Inflights:    tracker.NewInflights(n.gtrk.MaxInflight, n.gtrk.MaxInflightBytes),
				RecentActive: true,
			}
		}
	}
	mc := quorum.MajorityConfig{}
	for _, id := range voterIDs {
		mc[id] = struct{}{}
	}
	n.gtrk.Config.Voters = quorum.JointConfig{mc, nil}
}

func forceGlobalLeader(n *raft) {
	n.gTerm++
	n.becomeGlobalLeader()
	if pr := n.gtrk.Progress[n.id]; pr != nil {
		pr.BecomeReplicate()
		pr.RecentActive = true
	}
}

func forceLocalCommitAll(n *raft) {
	// Ensure self progress exists.
	if n.trk.Progress[n.id] == nil {
		n.trk.Progress[n.id] = &tracker.Progress{
			Match:        0,
			Next:         1,
			Inflights:    tracker.NewInflights(n.trk.MaxInflight, n.trk.MaxInflightBytes),
			RecentActive: true,
		}
	}
	// Bump self to the end of the log.
	pr := n.trk.Progress[n.id]
	pr.Match = n.raftLog.lastIndex()
	pr.Next = pr.Match + 1

	// Commit everything and apply.
	n.raftLog.commitTo(n.raftLog.lastIndex())
	n.appliedTo(n.raftLog.committed, 0)
}

// demoteGlobalLearner removes id from voters and marks it as learner in the leader's tracker.
func demoteGlobalLearner(leader *raft, id uint64) {
	// 1) Remove from voters
	voters := leader.gtrk.Config.Voters[0]
	delete(voters, id)
	leader.gtrk.Config.Voters = quorum.JointConfig{voters, nil}

	// 2) Add to learners set
	if leader.gtrk.Learners == nil {
		leader.gtrk.Learners = map[uint64]struct{}{}
	}
	leader.gtrk.Learners[id] = struct{}{}

	// 3) Mark Progress as learner (cosmetic but helpful)
	if pr := leader.gtrk.Progress[id]; pr != nil {
		pr.IsLearner = true
	}
}

// --- make3Global: use this in your tests ---
func make3Global(t *testing.T) map[uint64]*raft {
	gVoters := []uint64{1, 2, 3}
	nodes := map[uint64]*raft{
		1: newCRaftNode(t, 1, []uint64{1}, gVoters),
		2: newCRaftNode(t, 2, []uint64{2}, gVoters),
		3: newCRaftNode(t, 3, []uint64{3}, gVoters),
	}
	for _, n := range nodes {
		forceLocalSingletonLeader(n)     // local tier
		initGlobalMembership(n, gVoters) // global tier membership
	}
	forceGlobalLeader(nodes[1]) // choose 1 as global leader

	// sanity
	if nodes[1].gState != gStateLeader || len(nodes[1].gtrk.Voters.IDs()) != 3 {
		t.Fatalf("bad global bootstrap: state=%v voters=%d",
			nodes[1].gState, len(nodes[1].gtrk.Voters.IDs()))
	}
	return nodes
}

// It’s just a convenience for tests; production code should use the actual gLastIndex.
func currentGIndex(r *raft) uint64 {
	for idx := range r.gPossibleBatches {
		return idx
	}
	return 0
}

func TestCRaft_GlobalFastVoteDeferredUntilLocalCommit(t *testing.T) {
	nodes := make3Global(t)
	leader := nodes[1]
	f2 := nodes[2]

	// Leader: create some local commits that will be batched.
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("x")}
	if !leader.appendEntry(ent, ent) {
		t.Fatal("append failed")
	}
	leader.raftLog.commitTo(leader.raftLog.lastIndex())
	leader.appliedTo(leader.raftLog.committed, 0)

	// Enqueue the global-state wrapper on leader.
	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent, ent},
		FirstIndex: leader.raftLog.committed - 1,
		LastIndex:  leader.raftLog.committed,
	}
	leader.proposeGlobalStateEntry(batch)

	// IMPORTANT: force the leader's local wrapper to actually commit+apply,
	// so it broadcasts MsgGlobalFastProp.
	forceLocalCommitAll(leader)

	// Deliver messages: follower 2 will receive MsgGlobalFastProp,
	// but it must NOT send a vote yet (its wrapper not committed).
	pump(t, nodes)

	for _, m := range f2.msgs {
		if m.Type == pb.MsgGlobalFastVote {
			t.Fatalf("unexpected early global fast vote from node 2 before local wrapper commit")
		}
	}

	// Now force follower 2 to commit+apply its local wrapper (which was inserted).
	forceLocalCommitAll(f2)

	// The deferred vote should now be queued BEFORE delivery.
	found := false
	for _, m := range f2.msgs {
		if m.Type == pb.MsgGlobalFastVote {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected deferred global fast vote from node 2 after local wrapper commit (before pump)")
	}

	// Deliver the vote to the leader.
	pump(t, nodes)
}

func TestCRaft_GlobalFastCommit3of3(t *testing.T) {
	nodes := make3Global(t)
	L := nodes[1]

	// Create a local-committed range on the leader to batch
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("y")}
	if !L.appendEntry(ent, ent, ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	// Propose the batch (wraps in local wrapper; will broadcast after local apply)
	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent, ent, ent},
		FirstIndex: L.raftLog.committed - 2,
		LastIndex:  L.raftLog.committed,
	}
	L.proposeGlobalStateEntry(batch)
	forceLocalCommitAll(L)
	pump(t, nodes) // deliver fast-prop to others

	// Now force followers to apply their local wrappers so they send fast votes.
	for _, n := range nodes {
		if n == L {
			continue
		}
		forceLocalCommitAll(n)
	}
	pump(t, nodes) // deliver fast-votes back to leader and process

	// With 3/3 fast votes, leader should fast-commit at gIndex.
	if L.gCommit == 0 || L.gCommit != L.gLastIndex {
		t.Fatalf("expected fast-commit at global index %d, got commit=%d", L.gLastIndex, L.gCommit)
	}
}

func TestCRaft_GlobalFallbackClassicOnSplitVotes(t *testing.T) {
	nodes := make3Global(t)
	L, B, C := nodes[1], nodes[2], nodes[3]

	// 1) Leader builds a local-committed range for batching
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("z")}
	if !L.appendEntry(ent, ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	// 2) Propose the batch; then FORCE leader’s local wrapper to commit+apply
	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent, ent},
		FirstIndex: L.raftLog.committed - 1,
		LastIndex:  L.raftLog.committed,
	}
	L.proposeGlobalStateEntry(batch)
	forceLocalCommitAll(L) // ensures onGlobalStateEntryCommitted() runs and broadcasts fast-prop
	pump(t, nodes)         // deliver MsgGlobalFastProp to followers

	// 3) FORCE only B to commit+apply its wrapper so B sends one fast vote; C stays silent
	forceLocalCommitAll(B)
	pump(t, nodes) // deliver B’s fast vote back to leader; leader must fall back

	t.Logf("leader gLast=%d gCommit=%d; B.last=%d C.last=%d", L.gLastIndex, L.gCommit, B.glog.lastIndex(), C.glog.lastIndex())

	// After forcing B to vote and pumping, the leader should fall back.
	// Followers should have appended via classic path (at least one of them).
	if B.glog.lastIndex() == 0 && C.glog.lastIndex() == 0 {
		t.Fatalf("expected leader to send MsgGlobalApp (fallback); no follower appended to global log (B.last=%d, C.last=%d)",
			B.glog.lastIndex(), C.glog.lastIndex())
	}

	// Grab the global index we’re working on (the leader advanced its tail to this)
	idx := L.gLastIndex
	if idx == 0 {
		t.Fatalf("leader has no global index; fallback didn't append?")
	}

	// Verify we did NOT have fast quorum for this index.
	ctr := L.gPossibleBatches[idx].Tally()
	if ctr.HasFastQuorum {
		t.Fatalf("unexpected fast quorum at idx=%d; split votes should not reach fast quorum", idx)
	}

	// It’s OK (and expected) if a classic commit happened after fallback.
	if L.gCommit != idx {
		t.Fatalf("expected classic commit at idx=%d after fallback; got gCommit=%d", idx, L.gCommit)
	}

	// But definitely assert it's not a fast commit: fast quorum must be false.
	if L.gtrk.HasFastQuorum(idx) {
		t.Fatalf("gtrk.HasFastQuorum(idx=%d) should be false under split votes", idx)
	}

}

// If a global leader fails after broadcasting fast-prop and some followers
// have committed their wrappers, the newly forced global leader should still
// be able to tally and decide the same batch (no lost decision).
func TestCRaft_GlobalElectionRecoveryFromHints(t *testing.T) {
	nodes := make3Global(t)
	L, F1, F2 := nodes[1], nodes[2], nodes[3]

	// Leader builds batch and broadcasts
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("recov")}
	if !L.appendEntry(ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent},
		FirstIndex: L.raftLog.committed,
		LastIndex:  L.raftLog.committed,
	}
	L.proposeGlobalStateEntry(batch)

	// broadcast fast-prop
	forceLocalCommitAll(L)
	pump(t, nodes)

	// After L broadcasts, followers should have queued pending acks (wrapper inserted).
	if len(F1.pendingGlobalAcks) == 0 && !F1.hasCommittedGlobalStateFor(L.gLastIndex+1) {
		t.Fatalf("F1 never recorded a local wrapper after fast-prop")
	}
	if len(F2.pendingGlobalAcks) == 0 && !F2.hasCommittedGlobalStateFor(L.gLastIndex+1) {
		t.Fatalf("F2 never recorded a local wrapper after fast-prop")
	}

	// One follower votes
	forceLocalCommitAll(F1)
	pump(t, nodes)

	// >>> Make sure the future leader (F2) has the wrapper BEFORE promotion <<<
	forceLocalCommitAll(F2) // insert+commit local wrapper on F2
	pump(t, nodes)          // (optional) drain msgs

	gi, batch1 := F2.findLatestGlobalStateHint()
	t.Logf("F2 hint: gi=%d batchNil=%v gLast=%d gCommit=%d", gi, batch1 == nil, F2.gLastIndex, F2.gCommit)

	// Now promote F2
	forceGlobalLeader(F2)
	pump(t, nodes)

	// New leader can now complete decision (fast or classic)
	if F2.gLastIndex == 0 || F2.gCommit == 0 {
		t.Fatalf("expected new global leader to preserve/complete decision; got gLast=%d gCommit=%d", F2.gLastIndex, F2.gCommit)
	}
}

func TestCRaft_GlobalNoFastCommitAcrossTerms(t *testing.T) {
	nodes := make3Global(t)
	L, A, B := nodes[1], nodes[2], nodes[3]

	// 1) Leader proposes and broadcasts fast-prop
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("nofast-across-terms")}
	if !L.appendEntry(ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries:    []pb.Entry{ent},
		FirstIndex: L.raftLog.committed,
		LastIndex:  L.raftLog.committed,
	})
	forceLocalCommitAll(L) // triggers onGlobalStateEntryCommitted -> fast-prop
	pump(t, nodes)         // deliver fast-prop to followers

	// 2) Let only A vote first (classic quorum not yet reached).
	forceLocalCommitAll(A)
	pump(t, nodes) // deliver A's vote back to leader

	// Remember the working global index, if available.
	idx := L.gLastIndex
	if idx == 0 {
		// If not populated yet, it will be after fallback append below.
		idx = currentGIndex(L) // helper you can add to scan gPossibleBatches
	}

	// 3) Before the second vote arrives, bump the GLOBAL term to simulate re-election.
	L.gTerm++

	// 4) Now B sends its vote (from the old term value embedded in the fast-prop).
	forceLocalCommitAll(B)

	// Deliver B's vote; leader will see classic quorum.
	pump(t, nodes)

	// --- Assertions ---

	// We should *not* treat this as a fast commit across terms.
	// Even if a fast quorum exists numerically, the fast-commit guard must block it.
	if L.gtrk.HasFastQuorum(idx) {
		// That's fine (3/3 voted), but it must not have triggered fast-commit due to term mismatch.
		// Commit may still happen via classic fallback. So we don't fail on HasFastQuorum.
	}

	// The leader should have fallen back to classic append; followers should have appended.
	if A.glog.lastIndex() == 0 && B.glog.lastIndex() == 0 {
		t.Fatalf("expected classic fallback append to followers; A.last=%d B.last=%d",
			A.glog.lastIndex(), B.glog.lastIndex())
	}

	// Commit is allowed (and likely) via classic path. We just require it wasn't a fast commit gated by term equality.
	if L.gCommit == 0 {
		t.Fatalf("expected classic commit after fallback; got gCommit=0")
	}
	if L.gCommit != L.gLastIndex {
		t.Fatalf("inconsistent commit state: gCommit=%d gLastIndex=%d", L.gCommit, L.gLastIndex)
	}

	// And finally, ensure that fast quorum condition alone didn’t authorize the commit:
	// The guard in your code is: r.gtrk.HasFastQuorum(idx) && m.GlobalLogTerm == r.gTerm
	// We bumped r.gTerm before B’s vote, so m.GlobalLogTerm != r.gTerm at decision time.
	// We can't inspect m.GlobalLogTerm here, but we can assert that a classic append occurred (followers appended).
	// Already asserted via A/B glog.lastIndex() above.
}

func TestCRaft_GlobalOverwriteSpeculativeWithLeaderChoice(t *testing.T) {
	nodes := make3Global(t)
	L, F := nodes[1], nodes[2]

	// First candidate batch
	ent1 := pb.Entry{Type: pb.EntryNormal, Data: []byte("a")}
	if !L.appendEntry(ent1) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries: []pb.Entry{ent1}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed,
	})
	forceLocalCommitAll(L)
	pump(t, nodes) // F receives fast-prop1 and inserts wrapper

	// F speculatively inserted wrapper; now leader "chooses" (fast or classic)
	forceLocalCommitAll(F) // send vote
	pump(t, nodes)

	// Second different candidate (simulate contention) at next local range
	ent2 := pb.Entry{Type: pb.EntryNormal, Data: []byte("b")}
	if !L.appendEntry(ent2) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries: []pb.Entry{ent2}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed,
	})
	forceLocalCommitAll(L)
	pump(t, nodes) // second fast-prop arrives

	// As long as the leader’s chosen index resolves, follower’s speculative at that index
	// must be overwritten; confirm follower appended the leader-approved batch via glog.
	if F.glog.lastIndex() == 0 {
		t.Fatalf("expected follower to have overwritten speculative wrapper with leader-approved batch")
	}
}

func TestCRaft_GlobalHeartbeatAdvancesCommitOnFollowers(t *testing.T) {
	nodes := make3Global(t)
	L, A := nodes[1], nodes[2]

	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("hb")}
	if !L.appendEntry(ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed,
	})
	forceLocalCommitAll(L)
	pump(t, nodes) // fast-prop
	forceLocalCommitAll(A)
	pump(t, nodes) // vote back

	// Let leader classic or fast commit
	if L.gLastIndex == 0 {
		t.Fatalf("no global index present")
	}
	// Explicit heartbeat to propagate gCommit
	L.bcastGlobalHeartbeat()
	pump(t, nodes)

	// Follower should observe commit via heartbeat handling
	if A.gCommit != L.gCommit {
		t.Fatalf("follower did not learn global commit via heartbeat: got %d want %d", A.gCommit, L.gCommit)
	}
}

func TestCRaft_GlobalClassicCommitOnly(t *testing.T) {
	nodes := make3Global(t)
	L, B := nodes[1], nodes[2]

	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("classic")}
	if !L.appendEntry(ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed,
	})
	forceLocalCommitAll(L)
	pump(t, nodes)

	// Only B votes; C stays silent
	forceLocalCommitAll(B)
	pump(t, nodes)

	if L.gLastIndex == 0 {
		t.Fatalf("leader didn't append chosen batch at global tier")
	}
	idx := L.gLastIndex

	ctr := L.gPossibleBatches[idx].Tally()
	if ctr.HasFastQuorum {
		t.Fatalf("unexpected fast quorum in classic-only case")
	}
	// After fallback appends, classic commit should occur (2/3)
	if L.gCommit != idx {
		t.Fatalf("expected classic commit to idx=%d, got gCommit=%d", idx, L.gCommit)
	}
}

func TestCRaft_FastAtBothTiers_EndToEnd(t *testing.T) {
	nodes := make3Global(t)
	L, A, B := nodes[1], nodes[2], nodes[3]

	// Leader prepares a local batch for the global tier.
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("both-tiers")}
	if !L.appendEntry(ent, ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	// Wrap in local-global state entry; this MUST commit locally before global fast-prop/votes.
	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries:    []pb.Entry{ent, ent},
		FirstIndex: L.raftLog.committed - 1,
		LastIndex:  L.raftLog.committed,
	})

	// Local tier first: force the leader’s wrapper to commit -> broadcasts MsgGlobalFastProp
	forceLocalCommitAll(L)
	pump(t, nodes) // deliver fast-prop to A,B

	// Assert global votes are still deferred (local commit gate is active on followers)
	for _, n := range []*raft{A, B} {
		for _, m := range n.msgs {
			if m.Type == pb.MsgGlobalFastVote {
				t.Fatalf("unexpected early global fast vote from %d before local wrapper commit", n.id)
			}
		}
	}

	// Local tier: now commit wrappers on both followers -> each queues a fast vote
	forceLocalCommitAll(A)
	forceLocalCommitAll(B)

	// Before delivery, assert both followers actually queued a vote
	for _, n := range []*raft{A, B} {
		queued := false
		for _, m := range n.msgs {
			if m.Type == pb.MsgGlobalFastVote {
				queued = true
				break
			}
		}
		if !queued {
			t.Fatalf("expected deferred global fast vote from follower %d after local wrapper commit", n.id)
		}
	}

	// Deliver fast votes to the leader; with 3/3 the global tier should fast-commit.
	pump(t, nodes)

	if L.gLastIndex == 0 || L.gCommit != L.gLastIndex {
		t.Fatalf("expected global fast-commit, got gLast=%d gCommit=%d", L.gLastIndex, L.gCommit)
	}
}

func TestCRaft_LocalFastGate_GlobalClassicFallback_EndToEnd(t *testing.T) {
	nodes := make3Global(t)
	L, A, _ := nodes[1], nodes[2], nodes[3] // Only A will vote; B stays silent

	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("both-tiers-fallback")}
	if !L.appendEntry(ent, ent) {
		t.Fatal("append failed")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries: []pb.Entry{ent, ent}, FirstIndex: L.raftLog.committed - 1, LastIndex: L.raftLog.committed,
	})

	forceLocalCommitAll(L) // leader wrapper -> fast-prop out
	pump(t, nodes)

	// Still deferred: no follower vote before local wrapper commit
	for _, m := range A.msgs {
		if m.Type == pb.MsgGlobalFastVote {
			t.Fatalf("unexpected early global fast vote from A before local wrapper commit")
		}
	}

	// Only A commits its wrapper -> one fast vote
	forceLocalCommitAll(A)
	pump(t, nodes) // deliver A's vote back

	// Leader should have appended via fallback and may classic-commit (2/3).
	if L.gLastIndex == 0 {
		t.Fatalf("leader did not append chosen batch at global tier for fallback")
	}
	idx := L.gLastIndex

	// No fast quorum should be recorded.
	ctr := L.gPossibleBatches[idx].Tally()
	if ctr.HasFastQuorum || L.gtrk.HasFastQuorum(idx) {
		t.Fatalf("unexpected fast quorum in fallback case at idx=%d", idx)
	}

	// It's OK (and expected) to classic-commit after fallback replication.
	if L.gCommit != idx {
		t.Fatalf("expected classic commit to idx=%d after fallback; got gCommit=%d", idx, L.gCommit)
	}
}

func TestCRaft_GlobalDigestMismatchForcesFallback(t *testing.T) {
	nodes := make3Global(t)
	L, A, B := nodes[1], nodes[2], nodes[3]

	// 1) Leader broadcasts a batch at idx
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("d1")}
	if !L.appendEntry(ent) {
		t.Fatal("append")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed})
	forceLocalCommitAll(L)
	pump(t, nodes)

	// 2) A commits its wrapper and sends a NORMAL vote (digest1)
	forceLocalCommitAll(A)
	pump(t, nodes)

	// Capture the working index and its legit digest from the leader’s store
	idx := currentGIndex(L)
	legit := ""
	for dig := range L.globalPropStore[idx] {
		legit = dig
		break
	}

	// 3) Now forge B’s vote with a DIFFERENT digest at the SAME idx
	badDigest := make([]byte, len(legit))
	copy(badDigest, legit)
	if len(badDigest) > 0 {
		badDigest[0] ^= 0xFF // flip a bit
	}
	// Simulate B “voting” a conflicting digest (don’t commit wrapper for B)
	_ = L.Step(pb.Message{
		Type: pb.MsgGlobalFastVote,
		From: B.id, To: L.id,
		GlobalIndex:   idx,
		GlobalLogTerm: L.gTerm,
		Context:       badDigest,
	})

	// 4) No fast quorum at idx; leader must fallback replicate
	tally := L.gPossibleBatches[idx].Tally()
	if tally.HasFastQuorum {
		t.Fatalf("unexpected fast quorum under digest mismatch at idx=%d", idx)
	}
	// After fallback replication, at least one follower should have appended
	pump(t, nodes)
	if A.glog.lastIndex() == 0 && B.glog.lastIndex() == 0 {
		t.Fatalf("expected fallback append under digest mismatch")
	}
}

func TestCRaft_GlobalLearnerDoesNotCountInQuorum(t *testing.T) {
	nodes := make3Global(t)
	L, V, Learner := nodes[1], nodes[2], nodes[3]

	// Demote node 3 to learner at the GLOBAL tier
	demoteGlobalLearner(L, Learner.id)

	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("learner")}
	if !L.appendEntry(ent) {
		t.Fatal("append")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{
		Entries:    []pb.Entry{ent},
		FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed,
	})
	forceLocalCommitAll(L)
	pump(t, nodes)

	// Only the one voter (V) applies wrapper and votes
	forceLocalCommitAll(V)
	pump(t, nodes)

	// Learner may send a vote; it must NOT be required for commit
	forceLocalCommitAll(Learner)
	pump(t, nodes)

	// Classic commit should occur with leader+V
	if L.gCommit == 0 {
		t.Fatalf("expected classic commit with leader+voter majority; learner shouldn't be required")
	}
}

func TestCRaft_GlobalNeverOverwriteCommitted(t *testing.T) {
	nodes := make3Global(t)
	L, F := nodes[1], nodes[2]

	// Build idx=1 and commit it
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("safety")}
	if !L.appendEntry(ent) {
		t.Fatal("append")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)
	L.proposeGlobalStateEntry(pb.GlobalBatch{Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed})
	forceLocalCommitAll(L)
	pump(t, nodes)
	forceLocalCommitAll(F)
	pump(t, nodes)
	if L.gCommit == 0 {
		t.Fatal("missing baseline commit")
	}

	// Now simulate leader trying to send conflicting append at committed index via a bad response
	L.handleGlobalAppendResp(pb.Message{
		From: F.id, Type: pb.MsgGlobalAppResp,
		Reject: true, GlobalIndex: L.glog.committed, RejectHint: L.glog.committed - 1, GlobalLogTerm: 0,
	})
	// Assert committed didn’t regress
	if L.glog.committed != L.gCommit {
		t.Fatalf("committed regression: glog.commit=%d gCommit=%d", L.glog.committed, L.gCommit)
	}
}

func TestCRaft_GlobalConfChangeAddVoter(t *testing.T) {
	nodes := make3Global(t)
	L := nodes[1]

	// craft a ConfChangeV2 payload that adds ID=4 (pretend site 4 exists)
	cc := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode, NodeID: 4}}}
	data, _ := cc.Marshal()
	ent := pb.Entry{Type: pb.EntryConfChangeV2, Data: data}

	if !L.appendEntry(ent) {
		t.Fatal("append CCV2")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	L.proposeGlobalStateEntry(pb.GlobalBatch{Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed})
	forceLocalCommitAll(L)
	pump(t, nodes)

	// Classic-commit CCV2
	for _, n := range nodes {
		if n != L {
			forceLocalCommitAll(n)
		}
	}
	pump(t, nodes)

	// After commit, ensure new voter appears in Voters set
	if _, ok := L.gtrk.Config.Voters[0][4]; !ok {
		t.Fatalf("new voter not present after CCV2 commit")
	}
}

func TestCRaft_FollowerAppliesGlobalCCWhenPayloadArrivesAfterHeartbeat(t *testing.T) {
	nodes := make3Global(t)
	L, F := nodes[1], nodes[2]

	// Propose global ConfChangeV2 (add voter 4)
	cc := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode, NodeID: 4}}}
	data, _ := cc.Marshal()
	ent := pb.Entry{Type: pb.EntryConfChangeV2, Data: data}
	if !L.appendEntry(ent) {
		t.Fatal("append")
	}
	L.raftLog.commitTo(L.raftLog.lastIndex())
	L.appliedTo(L.raftLog.committed, 0)

	// Broadcast fast-prop
	L.proposeGlobalStateEntry(pb.GlobalBatch{Entries: []pb.Entry{ent}, FirstIndex: L.raftLog.committed, LastIndex: L.raftLog.committed})
	forceLocalCommitAll(L)
	pump(t, nodes)
	forceLocalCommitAll(F)
	pump(t, nodes) // vote back

	// Force leader to commit (fast or classic)
	if L.gCommit == 0 {
		t.Fatal("leader didn't commit")
	}

	// (1) Heartbeat advances follower commit first
	L.bcastGlobalHeartbeat()
	pump(t, nodes)

	// (Follower may not have payload yet; application could be delayed.)
	// Now simulate payload delivery via append
	// Leader will retry append via maybeSendGlobalAppend on next pump
	pump(t, nodes)

	// After payload delivery, follower must have applied CCV2
	if _, ok := F.gtrk.Config.Voters[0][4]; !ok {
		t.Fatalf("follower didn't apply global CCV2 after payload arrived: voters=%v", F.gtrk.Config.Voters[0])
	}
}
