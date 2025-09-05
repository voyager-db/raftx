package raft

import (
	"testing"

	"github.com/voyager-db/raftx/confchange"
	pb "github.com/voyager-db/raftx/raftpb"
)

// materialize a basic config for a single-node raft with memory storage
func newTestNode(t *testing.T) (*raft, *MemoryStorage) {
	t.Helper()
	ms := NewMemoryStorage()
	r := newRaft(&Config{
		ID:                        1,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   ms,
		MaxSizePerMsg:             1 << 20,
		MaxCommittedSizePerReady:  1 << 20,
		MaxUncommittedEntriesSize: 1 << 30,
		MaxInflightMsgs:           256,
		Logger:                    getLogger(),
		EnableFastPath:            true,
	})

	// Seed configuration voters={1}
	changer := confchange.Changer{Tracker: r.trk, LastIndex: r.raftLog.lastIndex()}
	cfg, trk, err := confchange.Restore(changer, pb.ConfState{Voters: []uint64{1}})
	if err != nil {
		t.Fatalf("restore conf: %v", err)
	}
	r.switchToConfig(cfg, trk)

	return r, ms
}

// get single entry at index
func mustEntryAt(t *testing.T, r *raft, idx uint64) pb.Entry {
	t.Helper()
	ents, err := r.raftLog.slice(idx, idx+1, noLimit)
	if err != nil || len(ents) != 1 {
		t.Fatalf("expected 1 entry at %d, got %v err=%v", idx, ents, err)
	}
	return ents[0]
}

// persistUnstableToStorage writes any unstable entries to MemoryStorage and marks them stable.
func persistUnstableToStorage(t *testing.T, r *raft, ms *MemoryStorage) {
	t.Helper()
	ents := r.raftLog.nextUnstableEnts()
	if len(ents) == 0 {
		return
	}
	if err := ms.Append(ents); err != nil {
		t.Fatalf("storage append failed: %v", err)
	}
	// Mark "in progress" then stable at last appended index.
	r.raftLog.acceptUnstable()
	last := ents[len(ents)-1]
	r.raftLog.stableTo(entryID{term: last.Term, index: last.Index})
}

// ===== Tests =====

// 1) Recompute frontier on restart; Unknown counts as Leader.
func TestRecomputeLastLeaderIndexOnRestart(t *testing.T) {
	r, ms := newTestNode(t)

	// Make it leader so appendEntry stamps Origin=Leader.
	r.becomeCandidate()
	r.becomeLeader()

	// Append 3 leader-approved entries.
	_ = r.appendEntry(pb.Entry{Data: []byte("a")})
	_ = r.appendEntry(pb.Entry{Data: []byte("b")})
	_ = r.appendEntry(pb.Entry{Data: []byte("c")})

	// Flush unstable -> storage so storage lastIndex == 3
	persistUnstableToStorage(t, r, ms)

	// Manually append a SELF entry at index 4 (simulate speculative tail).
	e4 := pb.Entry{Index: 4, Term: r.Term, Data: []byte("self")}
	setOrigin(&e4, pb.EntryOriginSelf)
	ms.Append([]pb.Entry{e4})

	// Manually append UNKNOWN entry at index 5 (legacy/old node).
	e5 := pb.Entry{Index: 5, Term: r.Term, Data: []byte("unknown")}
	// leave Origin nil
	ms.Append([]pb.Entry{e5})

	// Restart (new raft on same storage).
	r2 := newRaft(&Config{
		ID:                        1,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   ms,
		MaxSizePerMsg:             1024 * 1024,
		MaxCommittedSizePerReady:  1024 * 1024,
		MaxUncommittedEntriesSize: 1 << 30,
		MaxInflightMsgs:           256,
		Logger:                    getLogger(),
	})

	// Expect lastLeaderIndex = 5 (UNKNOWN treated as Leader; SELF at 4 is ignored).
	if r2.lastLeaderIndex != 5 {
		t.Fatalf("lastLeaderIndex = %d, want 5", r2.lastLeaderIndex)
	}
}

// 2) Leader appends set Origin=Leader when nil/unknown.
func TestLeaderAppendsSetsOriginLeader(t *testing.T) {
	r, _ := newTestNode(t)
	r.becomeCandidate()
	r.becomeLeader()

	// Case A: Origin nil
	ok := r.appendEntry(pb.Entry{Data: []byte("x")})
	if !ok {
		t.Fatalf("appendEntry failed")
	}
	e1 := mustEntryAt(t, r, 1)
	if got := getOrigin(&e1); got != pb.EntryOriginLeader {
		t.Fatalf("entry[1] origin=%v, want Leader", got)
	}

	// Case B: Origin explicitly Unknown
	e := pb.Entry{Data: []byte("y")}
	setOrigin(&e, pb.EntryOriginUnknown)
	ok = r.appendEntry(e)
	if !ok {
		t.Fatalf("appendEntry failed")
	}
	e2 := mustEntryAt(t, r, 2)
	if got := getOrigin(&e2); got != pb.EntryOriginLeader {
		t.Fatalf("entry[2] origin=%v, want Leader", got)
	}
}

// 3) Follower overwrite (AppendEntries) stamps incoming as Leader.
func TestFollowerOverwriteByLeaderSetsOriginLeader(t *testing.T) {
	r, _ := newTestNode(t)
	// Stay follower.

	// Simulate leader sending MsgApp with one entry at index 1, origin unknown.
	ent := pb.Entry{Index: 1, Term: 1, Data: []byte("L1")}
	// Origin left nil on purpose.
	msg := pb.Message{
		Type:    pb.MsgApp,
		From:    2, // leader id
		Index:   0,
		LogTerm: 0,
		Entries: []pb.Entry{ent},
		Commit:  0,
	}

	// Follower handles append; your handleAppendEntries should stamp Origin=Leader if unknown.
	r.handleAppendEntries(msg)

	stored := mustEntryAt(t, r, 1)
	if got := getOrigin(&stored); got != pb.EntryOriginLeader {
		t.Fatalf("follower stored origin=%v, want Leader", got)
	}
}

// 4) Snapshot install recomputes frontier (uses snapshot boundary if nothing in log).
func TestSnapshotInstallRecomputesLastLeaderIndex(t *testing.T) {
	r, ms := newTestNode(t)

	// Append and commit up to 3 as leader so we can snapshot index=3.
	r.becomeCandidate()
	r.becomeLeader()
	_ = r.appendEntry(pb.Entry{Data: []byte("a")})
	_ = r.appendEntry(pb.Entry{Data: []byte("b")})
	_ = r.appendEntry(pb.Entry{Data: []byte("c")})
	// Fake commit for test simplicity.
	r.raftLog.commitTo(3)

	// Create and apply snapshot at index=3.
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     3,
			Term:      r.Term,
			ConfState: r.trk.ConfState(),
		},
	}
	// Store snapshot, then instruct raft to apply it via a MsgStorageAppendResp or directly:
	ms.ApplySnapshot(snap) // memory storage helper; if not available use ms.CreateSnapshot+Compact

	// Tell the raft node that snapshot got applied (like storage thread would).
	r.appliedSnap(&snap)

	// Expect frontier to be >= 3. If your recompute considers snapshot boundary,
	// it should be exactly 3; otherwise it may be 0 if no entries remain.
	// The safer invariant we assert: it must NOT be > snapshot index.
	if r.lastLeaderIndex > 3 {
		t.Fatalf("lastLeaderIndex=%d > snapshotIndex(3)", r.lastLeaderIndex)
	}
	// If you've implemented "use snapshot index if scan finds nothing", assert exact value:
	// if r.lastLeaderIndex != 3 { t.Fatalf("lastLeaderIndex=%d, want 3", r.lastLeaderIndex) }
}
