package raft

import (
	"testing"

	pb "github.com/voyager-db/raftx/raftpb"
)

// minimal helper: 1-node local leader + CRaft enabled; global state we set manually.
func newLocalLeaderForBatch(t *testing.T) *raft {
	t.Helper()
	mem := NewMemoryStorage()
	mem.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: []uint64{1}},
			Index:     1, Term: 1,
		},
	})
	r := newRaft(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         mem,
		MaxInflightMsgs: 256,
		MaxSizePerMsg:   1 << 20,
		EnableCRaft:     true,
		Logger:          getLogger(),
	})
	// Make it a real local leader so appendEntry works with tracker invariants.
	r.becomeCandidate()
	r.becomeLeader()
	// Set global tier to "leader" for the gate in maybeProposeGlobalBatch
	r.gState = gStateLeader
	return r
}

// Decode a GlobalStateEntry if the entry holds one.
func readGSE(t *testing.T, e pb.Entry) *pb.GlobalStateEntry {
	t.Helper()
	if len(e.Data) == 0 {
		return nil
	}
	var gse pb.GlobalStateEntry
	if err := gse.Unmarshal(e.Data); err == nil && (gse.GlobalIndex != 0 || gse.Batch != nil) {
		return &gse
	}
	return nil
}

// --- Tests ---

func TestMaybeProposeGlobalBatch_NoOp_WhenNotGlobalLeader(t *testing.T) {
	r := newLocalLeaderForBatch(t)
	// append and commit a local entry
	_ = r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("a")})
	r.raftLog.commitTo(r.raftLog.lastIndex())

	// not global leader -> no-op
	r.gState = gStateFollower
	r.maybeProposeGlobalBatch()

	// last entry must still be the user entry; no wrapper appended
	last := r.raftLog.lastIndex()
	e, _ := r.raftLog.entry(last)
	if gse := readGSE(t, e); gse != nil {
		t.Fatalf("unexpected global-state wrapper appended when not global leader")
	}
}

func TestMaybeProposeGlobalBatch_NoOp_WhenNoNewCommitted(t *testing.T) {
	r := newLocalLeaderForBatch(t)
	r.gState = gStateLeader

	// globLastLocalIncluded == committed -> no new entries to batch
	r.globLastLocalIncluded = r.raftLog.committed
	r.maybeProposeGlobalBatch()
	// Nothing should be appended; last should remain at snapshot index (1)
	last := r.raftLog.lastIndex()
	e, _ := r.raftLog.entry(last)
	if gse := readGSE(t, e); gse != nil {
		t.Fatalf("unexpected global-state wrapper appended when no new committed entries")
	}
}

func TestMaybeProposeGlobalBatch_ProposesCorrectRange_FastPathDisabled(t *testing.T) {
	r := newLocalLeaderForBatch(t)
	r.gState = gStateLeader

	// Disable fast path so proposeGlobalStateEntry appends wrapper synchronously.
	r.enableFastPath = false

	// Append three local entries (indices 2..4) and commit them.
	_ = r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("m2")})
	_ = r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("m3")})
	_ = r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("m4")})
	r.raftLog.commitTo(r.raftLog.lastIndex())

	// Batch should start at globLastLocalIncluded+1; initially 0 -> lo=1+? Wait: globLastLocalIncluded default 0, lo=1
	// But FirstIndex should be the first *new committed after globLastLocalIncluded*.
	// Make explicit: set globLastLocalIncluded=1 => expect batch FirstIndex=2, LastIndex=4.
	r.globLastLocalIncluded = 1

	r.maybeProposeGlobalBatch()

	last := r.raftLog.lastIndex()
	e, _ := r.raftLog.entry(last)
	gse := readGSE(t, e)
	if gse == nil || gse.Batch == nil {
		t.Fatalf("expected global-state wrapper entry; got nil")
	}
	if gse.Batch.FirstIndex != 3 || gse.Batch.LastIndex != 5 || len(gse.Batch.Entries) != 3 {
		t.Fatalf("wrong batch range: got first=%d last=%d len=%d (want 3..5, len=3)",
			gse.Batch.FirstIndex, gse.Batch.LastIndex, len(gse.Batch.Entries))
	}
	// maybeProposeGlobalBatch should NOT bump globLastLocalIncluded; that happens on wrapper commit in onGlobalStateEntryCommitted.
	if r.globLastLocalIncluded != 1 {
		t.Fatalf("globLastLocalIncluded changed unexpectedly: %d", r.globLastLocalIncluded)
	}
}

func TestMaybeProposeGlobalBatch_ProposesWrapper_FastPathEnabled_LocalDelivery(t *testing.T) {
	r := newLocalLeaderForBatch(t)
	r.gState = gStateLeader
	r.enableFastPath = true // default, but be explicit

	// Append & commit one local entry (idx=2).
	_ = r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("x")})
	r.raftLog.commitTo(r.raftLog.lastIndex())
	r.globLastLocalIncluded = 1

	// With fast path, proposeGlobalStateEntry will broadcastFastProp, which delivers locally via handleFastProp.
	r.maybeProposeGlobalBatch()

	// Last entry should be the wrapper (appended by handleFastProp)
	last := r.raftLog.lastIndex()
	e, _ := r.raftLog.entry(last)
	if gse := readGSE(t, e); gse == nil {
		t.Fatalf("expected global-state wrapper appended via fast-prop local delivery")
	}
}
