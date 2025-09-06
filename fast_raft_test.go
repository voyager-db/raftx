// fast_raft_test.go
package raft

import (
	"bytes"
	"testing"

	pb "github.com/voyager-db/raftx/raftpb"
)

// --- small harness ----------------------------------------------------------

func mkVoters(n int) []uint64 {
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
	}
	return ids
}

type raftOpts struct {
	enableFastPath   bool
	enableFastCommit bool
	fastWindow       int
	preVote          bool
	checkQuorum      bool
}

// newLeader returns a single raft node configured with the given voters,
// already bumped into leader state. We do not spin up a full network;
// tests directly manipulate Progress/commit.
// fast_raft_test.go (update newLeader)

func newLeader(t *testing.T, voters []uint64, id uint64, opts raftOpts) *raft {
	t.Helper()

	ms := NewMemoryStorage()

	// Seed with a fresh snapshot newer than the implicit zero-snapshot.
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: voters},
			Index:     1,
			Term:      1,
		},
	}
	if err := ms.ApplySnapshot(snap); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	cfg := &Config{
		ID:            id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       ms,
		CheckQuorum:   opts.checkQuorum,
		PreVote:       opts.preVote,

		// ✅ Fast Raft knobs used by the tests
		EnableFastPath:      opts.enableFastPath,
		FastOpenIndexWindow: max(1, opts.fastWindow),

		// ✅ Required / sensible defaults for tests
		MaxInflightMsgs:          256,     // must be > 0
		MaxSizePerMsg:            1 << 20, // 1 MiB
		MaxCommittedSizePerReady: 1 << 20, // keep apply window reasonable
		// MaxInflightBytes left at 0 (means “no limit”) which validate() upgrades to noLimit
	}
	r := newRaft(cfg)
	r.enableFastCommit = opts.enableFastCommit

	r.becomeCandidate()
	r.becomeLeader()
	return r
}

func appendLeaderEntries(t *testing.T, r *raft, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !r.appendEntry(pb.Entry{Data: []byte("x")}) {
			t.Fatalf("appendEntry failed")
		}
	}
}

func entryAt(t *testing.T, r *raft, idx uint64) pb.Entry {
	t.Helper()
	ents, err := r.raftLog.slice(idx, idx+1, noLimit)
	if err != nil || len(ents) != 1 {
		t.Fatalf("slice(%d): err=%v len=%d", idx, err, len(ents))
	}
	return ents[0]
}

func setMajorityMatchAt(r *raft, k uint64) {
	ids := r.trk.Config.Voters.IDs()
	need := len(ids)/2 + 1
	i := 0
	for id := range ids {
		if pr := r.trk.Progress[id]; pr != nil {
			pr.Match = k
			pr.Next = k + 1
			i++
			if i >= need {
				break
			}
		}
	}
}

func hasPossible(r *raft, idx uint64, cid string) bool {
	mp := r.possibleEntries[idx]
	if mp == nil {
		return false
	}
	_, ok := mp[cid]
	return ok
}

// --- tests ------------------------------------------------------------------

// M5: pick a decision at k=commit+1, replicate classically, and null duplicates.
func TestFastDecision_SelectsAtK_ReplicatesClassically(t *testing.T) {
	voters := mkVoters(5)
	r := newLeader(t, voters, 1, raftOpts{
		enableFastPath:   true,
		enableFastCommit: false,
	})
	// Build committed prefix [1..10].
	appendLeaderEntries(t, r, 10)
	r.raftLog.commitTo(10)

	k := r.raftLog.committed + 1 // 11

	// Pretend leader saw a proposal payload for cid "A" at k (so it can materialize it).
	if r.proposalCache[k] == nil {
		r.proposalCache[k] = make(map[string]pb.Entry)
	}
	r.proposalCache[k]["A"] = pb.Entry{
		Type:      pb.EntryNormal,
		ContentId: []byte("A"),
		Data:      []byte("value-A"),
		Origin:    pb.EntryOriginSelf.Enum(), // will be overwritten to Leader
	}

	// Also stage a duplicate content at another index to ensure it is nulled.
	r.possibleEntries[k+1] = map[string]map[uint64]struct{}{"A": {2: {}, 3: {}}}

	// Votes at k: A from 2,3,4; B from 5, (leader does not count here).
	r.possibleEntries[k] = map[string]map[uint64]struct{}{
		"A": {2: {}, 3: {}, 4: {}}, // 3 votes => CQ for 5 voters
		"B": {5: {}},
	}
	r.fastVoterChoice[k] = map[uint64]string{
		2: "A", 3: "A", 4: "A", 5: "B",
	}

	// Choose and install decision (classic only).
	r.maybeDecideAtKClassicOnly()

	// Assert leader appended leader-approved entry at k with cid "A".
	got := entryAt(t, r, k)
	if !bytes.Equal(got.ContentId, []byte("A")) {
		t.Fatalf("winner cid = %q, want %q", string(got.ContentId), "A")
	}
	if got.Origin == nil || *got.Origin != pb.EntryOriginLeader {
		t.Fatalf("origin at %d = %v, want Leader", k, got.Origin)
	}

	// Not fast committed: commit should still be 10.
	if r.raftLog.committed != 10 {
		t.Fatalf("commit=%d, want 10 (classic path only)", r.raftLog.committed)
	}

	// Classic replicate: simulate acks to majority and maybeCommit.
	setMajorityMatchAt(r, k)
	if !r.maybeCommit() {
		t.Fatalf("maybeCommit did not advance")
	}
	if r.raftLog.committed != k {
		t.Fatalf("commit=%d, want %d", r.raftLog.committed, k)
	}

	// Dedupe: content "A" should be removed from other buckets.
	if hasPossible(r, k+1, "A") {
		t.Fatalf("duplicate content 'A' was not nulled at idx=%d", k+1)
	}
}

// M6: fast-commit when fastMatchIndex majority observed and term guard holds.
func TestFastCommit_HappyPath_1RTT(t *testing.T) {
	voters := mkVoters(5)
	r := newLeader(t, voters, 1, raftOpts{
		enableFastPath:   true,
		enableFastCommit: true,
	})

	// Seed committed prefix, pick k.
	appendLeaderEntries(t, r, 5)
	r.raftLog.commitTo(5)
	k := r.raftLog.committed + 1 // 6

	// Materialize decision at k (same as M5, stripped down).
	if r.proposalCache[k] == nil {
		r.proposalCache[k] = make(map[string]pb.Entry)
	}
	r.proposalCache[k]["A"] = pb.Entry{
		Type:      pb.EntryNormal,
		ContentId: []byte("A"),
		Data:      []byte("value-A"),
	}
	r.possibleEntries[k] = map[string]map[uint64]struct{}{
		"A": {2: {}, 3: {}, 4: {}}, // CQ
	}
	r.fastVoterChoice[k] = map[uint64]string{2: "A", 3: "A", 4: "A"}

	// Decide and install leader entry at k.
	r.maybeDecideAtKClassicOnly()

	// Simulate that a fast quorum has already self-approved the same value:
	// set fastMatchIndex for majority >= k.
	r.fastMatchIndex = map[uint64]uint64{
		1: k, 2: k, 3: k, // 3 voters => FQ met
	}

	// Term guard must hold: the entry we installed is in current term by construction.
	ok := r.tryFastCommit(k)
	if !ok {
		t.Fatalf("tryFastCommit returned false; expected true")
	}
	if r.raftLog.committed != k {
		t.Fatalf("fast commit failed; commit=%d want %d", r.raftLog.committed, k)
	}
}

// Role transition hygiene + election self bag lifecycle.
func TestRoleTransitions_ClearLeaderOnlyAndElectionBag(t *testing.T) {
	voters := mkVoters(3)
	r := newLeader(t, voters, 1, raftOpts{
		enableFastPath: true,
	})

	// Populate leader-only maps.
	r.possibleEntries = map[uint64]map[string]map[uint64]struct{}{11: {"A": {2: {}}}}
	r.fastVoterChoice = map[uint64]map[uint64]string{11: {2: "A"}}
	r.proposalCache = map[uint64]map[string]pb.Entry{11: {"A": {ContentId: []byte("A")}}}
	r.fastMatchIndex = map[uint64]uint64{2: 11}

	// Also stash some election self state to ensure it gets cleared when stepping down.
	idx := uint64(12)
	r.electionSelf = map[uint64][]*pb.EntryRef{
		2: {&pb.EntryRef{Index: &idx, ContentId: []byte("X"), Origin: pb.EntryOriginSelf.Enum()}},
	}

	// Step down.
	r.becomeFollower(r.Term+1, None)

	if r.possibleEntries != nil || r.fastVoterChoice != nil || r.proposalCache != nil || r.fastMatchIndex != nil {
		t.Fatalf("leader-only fast state not cleared on becomeFollower")
	}
	if r.electionSelf != nil {
		t.Fatalf("electionSelf not cleared on becomeFollower")
	}

	// Start a campaign: bag must be (re)initialized.
	r.hup(campaignElection) // start campaign
	if r.electionSelf == nil {
		t.Fatalf("electionSelf not initialized on campaign start")
	}
}

// Snapshot/install across mixed origins must clamp lastLeaderIndex to ≤ snapshot index.
func TestSnapshot_InstallAcrossMixedOrigins_UpdatesFrontier(t *testing.T) {
	voters := mkVoters(3)
	r := newLeader(t, voters, 1, raftOpts{enableFastPath: true})

	// Append some leader entries; lastLeaderIndex should track the tail.
	appendLeaderEntries(t, r, 15)
	if r.lastLeaderIndex < 15 {
		t.Fatalf("lastLeaderIndex=%d, want >= 15", r.lastLeaderIndex)
	}

	// Apply a snapshot at 12; commit must be ≥ snapshot index first.
	r.raftLog.commitTo(12) // precondition for appliedSnap
	s := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 12, Term: r.Term}}
	r.appliedSnap(s)

	if r.lastLeaderIndex > 12 {
		t.Fatalf("lastLeaderIndex=%d, want <= 12 after snapshot", r.lastLeaderIndex)
	}

	// Ensure subsequent elections will use the new frontier (no direct assert here;
	// just make sure recompute didn't panic and value is within bound).
}

// When the *total* fast votes at k reach a classic quorum but the top content
// has < quorum (e.g., 2-2 split on 5 nodes), the leader should still choose a
// decision at k and replicate classically.
func TestFastDecision_SplitVotes_ClassicDecisionOnTotalQuorum(t *testing.T) {
	voters := mkVoters(5)
	r := newLeader(t, voters, 1, raftOpts{
		enableFastPath:   true,
		enableFastCommit: false, // keep it purely "decide + classic replicate"
	})

	// Build a committed prefix and choose k.
	appendLeaderEntries(t, r, 3)
	r.raftLog.commitTo(3)
	k := r.raftLog.committed + 1 // 4

	// Seed payloads the leader can materialize for each content id.
	if r.proposalCache[k] == nil {
		r.proposalCache[k] = make(map[string]pb.Entry)
	}
	r.proposalCache[k]["A"] = pb.Entry{
		Type:      pb.EntryNormal,
		ContentId: []byte("A"),
		Data:      []byte("value-A"),
	}
	r.proposalCache[k]["B"] = pb.Entry{
		Type:      pb.EntryNormal,
		ContentId: []byte("B"),
		Data:      []byte("value-B"),
	}

	// Split votes at k: A from 2,3; B from 4,5. Total voters at k = 4 (≥ CQ=3).
	r.possibleEntries[k] = map[string]map[uint64]struct{}{
		"A": {2: {}, 3: {}},
		"B": {4: {}, 5: {}},
	}
	r.fastVoterChoice[k] = map[uint64]string{
		2: "A", 3: "A", 4: "B", 5: "B",
	}

	// Run the decision loop: with the FIX, totalVotes>=CQ ⇒ decide at k.
	r.maybeDecideAtKClassicOnly()

	// Assert leader appended a leader-approved entry at k.
	ent := entryAt(t, r, k)
	if !bytes.Equal(ent.ContentId, []byte("A")) {
		// Tie-break is lexicographic, so "A" wins deterministically.
		t.Fatalf("decided cid at k=%d = %q, want %q", k, string(ent.ContentId), "A")
	}
	if ent.Origin == nil || *ent.Origin != pb.EntryOriginLeader {
		t.Fatalf("origin at k=%d = %v, want Leader", k, ent.Origin)
	}

	// Buckets at k should be cleared (no more voting at k after decision).
	if _, ok := r.possibleEntries[k]; ok {
		t.Fatalf("possibleEntries not cleared at k=%d", k)
	}
	if _, ok := r.fastVoterChoice[k]; ok {
		t.Fatalf("fastVoterChoice not cleared at k=%d", k)
	}
	if _, ok := r.proposalCache[k]; ok {
		t.Fatalf("proposalCache not cleared at k=%d", k)
	}

	// Not committed yet (we didn't simulate acks). Simulate classic acks and commit.
	setMajorityMatchAt(r, k)
	if !r.maybeCommit() {
		t.Fatalf("maybeCommit did not advance after classic majority matched")
	}
	if r.raftLog.committed != k {
		t.Fatalf("commit=%d, want %d", r.raftLog.committed, k)
	}
}

// A companion negative test: if total votes at k < CQ, leader must NOT decide.
func TestFastDecision_NotEnoughTotalVotes_NoDecision(t *testing.T) {
	voters := mkVoters(5)
	r := newLeader(t, voters, 1, raftOpts{
		enableFastPath:   true,
		enableFastCommit: false,
	})

	// After becomeLeader(), there's one empty entry. Fence k at the current tail+1.
	lastBefore := r.raftLog.lastIndex()
	r.raftLog.commitTo(lastBefore)
	k := lastBefore + 1

	// Only 2 total voters at k (below CQ=3 for 5 voters) -> must NOT decide.
	r.possibleEntries[k] = map[string]map[uint64]struct{}{
		"A": {2: {}},
		"B": {3: {}},
	}
	r.fastVoterChoice[k] = map[uint64]string{2: "A", 3: "B"}

	r.maybeDecideAtKClassicOnly()

	// Assert: the log tail did not grow (i.e. no append at k).
	lastAfter := r.raftLog.lastIndex()
	if lastAfter != lastBefore {
		t.Fatalf("unexpected append with totalVotes<CQ: lastIndex %d -> %d", lastBefore, lastAfter)
	}
	// Optional: no leader-approved frontier movement either.
	// (Keep this only if lastLeaderIndex should remain unchanged in your impl.)
	// if r.lastLeaderIndex != 0 && r.lastLeaderIndex > lastBefore {
	// 	t.Fatalf("unexpected lastLeaderIndex movement to %d", r.lastLeaderIndex)
	// }

	// Optional: decision buckets still present (we didn't pick a winner).
	if _, ok := r.possibleEntries[k]; !ok {
		t.Fatalf("possibleEntries[k] cleared despite no decision")
	}
}
