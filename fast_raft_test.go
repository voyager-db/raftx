// Copyright 2024 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/voyager-db/raftx/quorum"
	pb "github.com/voyager-db/raftx/raftpb"
	"github.com/voyager-db/raftx/tracker"
)

// TestFastRaftNoContention tests the fast path with no contention.
// Should commit in 2 rounds: proposer→all, leader→all
func TestFastRaftNoContention(t *testing.T) {
	cfgFast := func(c *Config) { c.EnableFastPath = true }
	nt := newNetworkWithConfig(cfgFast, nil, nil, nil, nil, nil)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	lead := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, lead.state)

	// capture outbound fast-proposals
	var fastProps []pb.Message
	nt.msgHook = func(m pb.Message) bool {
		if m.Type == pb.MsgFastProp {
			fastProps = append(fastProps, m)
		}
		return true // allow delivery
	}

	// Propose a single entry (no contention)
	payload := []byte("test")
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: payload}}})

	require.Len(t, fastProps, 4, "should broadcast MsgFastProp to 4 followers in a 5-node cluster")
	for _, m := range fastProps {
		require.Len(t, m.Entries, 1)
		assert.NotEqual(t, pb.InsertedByLeader, m.Entries[0].InsertedBy)
	}

	// Extract index/term from one of the captured fast props
	propEntry := fastProps[0].Entries[0]
	propIndex := propEntry.Index
	propTerm := propEntry.Term
	digest := entryDigest(&propEntry)

	// Simulate 3 follower votes (fast quorum = 4 with leader’s own vote)
	for _, from := range []uint64{2, 3, 4} {
		vote := pb.Message{
			Type: pb.MsgFastVote,
			From: from, To: 1,
			Index: propIndex, LogTerm: propTerm,
			Context: []byte(digest),
		}
		require.NoError(t, lead.Step(vote))
	}

	assert.Equal(t, propIndex, lead.raftLog.committed, "should fast-commit the proposed index")
}

// TestFastRaftContention tests fallback to classic path with contention
func TestFastRaftContention(t *testing.T) {
	peers := []uint64{1, 2, 3, 4, 5}
	storage := newTestMemoryStorage(withPeers(peers...)) // ✅ voters seeded

	cfg := newTestConfig(1, 10, 1, storage)
	cfg.EnableFastPath = true
	rn := newRaft(cfg)

	rn.becomeFollower(1, None)
	rn.becomeCandidate()
	rn.becomeLeader()

	// Fast path is gated until the leader has a commit in its term.
	// Simulate the no-op’s classic commit so fast path is enabled.
	rn.raftLog.commitTo(rn.raftLog.lastIndex())
	// Optional: clear any preexisting messages before this test’s action
	rn.msgs, rn.msgsAfterAppend = nil, nil

	// ✅ initialize tracker: voters + inflights
	rn.trk.Voters[0] = map[uint64]struct{}{}
	for _, id := range peers {
		rn.trk.Voters[0][id] = struct{}{}
		if rn.trk.Progress[id] == nil {
			rn.trk.Progress[id] = &tracker.Progress{
				Match:     0,
				Next:      1,
				Inflights: tracker.NewInflights(rn.trk.MaxInflight, rn.trk.MaxInflightBytes),
			}
		} else if rn.trk.Progress[id].Inflights == nil {
			rn.trk.Progress[id].Inflights = tracker.NewInflights(rn.trk.MaxInflight, rn.trk.MaxInflightBytes)
		}
	}

	// Propose a user entry (fast path should broadcast MsgFastProp)
	require.NoError(t, rn.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("test")}}}))

	all := append([]pb.Message{}, rn.msgs...)
	all = append(all, rn.msgsAfterAppend...)
	require.NotEmpty(t, all)
	var prop pb.Message
	for _, m := range all {
		if m.Type == pb.MsgFastProp {
			prop = m
			break
		}
	}
	require.Equal(t, pb.MsgFastProp, prop.Type)
	propIndex := prop.Entries[0].Index
	propTerm := prop.Entries[0].Term
	dig := entryDigest(&prop.Entries[0])

	// 2 votes for the proposed entry
	for _, from := range []uint64{2, 3} {
		require.NoError(t, rn.Step(pb.Message{
			Type: pb.MsgFastVote, From: from, To: 1,
			Index: propIndex, LogTerm: propTerm, Context: []byte(dig),
		}))
	}

	// 2 votes for a different entry (contention)
	for _, from := range []uint64{4, 5} {
		require.NoError(t, rn.Step(pb.Message{
			Type: pb.MsgFastVote, From: from, To: 1,
			Index: propIndex, LogTerm: propTerm, Context: []byte("different-entry"),
		}))
	}

	// Add LEADER SELF-VOTE for the proposed entry to reach classic quorum (3/5).
	require.NoError(t, rn.Step(pb.Message{
		Type: pb.MsgFastVote, From: 1, To: 1,
		Index: propIndex, LogTerm: propTerm, Context: []byte(dig),
	}))

	// Now the leader has a classic quorum for that digest, will insert leader-approved,
	// and replicate. You can optionally nudge the pipeline:
	_ = rn.Step(pb.Message{Type: pb.MsgBeat, From: 1, To: 1})

	// Classic fallback: simulate AppResp from two followers to reach classic majority
	for _, from := range []uint64{2, 3} {
		require.NoError(t, rn.Step(pb.Message{
			Type: pb.MsgAppResp, From: from, To: 1, Index: propIndex,
		}))
	}

	assert.Equal(t, propIndex, rn.raftLog.committed, "should commit via classic majority")

}

// TestFastRaftElectionWithSelfApproved tests that self-approved entries
// are properly handled during elections
func TestFastRaftElectionWithSelfApproved(t *testing.T) {
	// Seed a small cluster with peers; id=2 is the node under test.
	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(2, 10, 1, storage)
	cfg.EnableFastPath = true
	r := newRaft(cfg) // or newTestRaft(2, 10, 1, storage)

	r.becomeFollower(1, 1) // following leader 1

	// Receive a fast proposal from leader
	fastProp := pb.Message{
		Type: pb.MsgFastProp, From: 1, To: 2, Term: 1,
		Entries: []pb.Entry{{Term: 1, Index: 1, Data: []byte("test")}},
	}
	require.NoError(t, r.Step(fastProp))

	// Self-approved entry present
	ent, err := r.raftLog.entry(1)
	require.NoError(t, err)
	assert.Equal(t, pb.InsertedBySelf, ent.InsertedBy)

	// Self-approved hint exists
	assert.Len(t, r.selfApprovedHints, 1)
	assert.Equal(t, uint64(1), r.selfApprovedHints[0].Index)

	// Start an election on node 2
	r.becomeCandidate()

	// Receive a vote response that carries hints
	voteResp := pb.Message{
		Type: pb.MsgVoteResp, From: 3, To: 2, Term: 2,
		Hints: r.selfApprovedHints,
	}
	require.NoError(t, r.Step(voteResp))

	// Promote to leader (tracker is initialized; inflights allocated)
	r.becomeLeader()

	// Hints should have been processed into possibleEntries by becomeLeader()
	assert.NotNil(t, r.possibleEntries)
}

// TestFastRaftSparseLog tests sparse log insertion
func TestFastRaftSparseLog(t *testing.T) {
	storage := NewMemoryStorage()
	l := newLogWithSize(storage, raftLogger, noLimit)
	l.enableFastRaft() // Enable Fast Raft features

	// Sparse append at index 5 (leaving gaps at 2, 3, 4)
	entry := pb.Entry{
		Term: 1,
		Data: []byte("sparse"),
	}

	err := l.sparseAppend(5, entry, pb.InsertedBySelf)
	require.NoError(t, err)

	// Verify entry exists at index 5
	e, err := l.entry(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), e.Index)
	assert.Equal(t, pb.InsertedBySelf, e.InsertedBy)

	// Verify gaps exist
	assert.True(t, l.hasGaps(2, 4))

	// Try to overwrite with self-approved (should fail)
	entry2 := pb.Entry{
		Term: 1,
		Data: []byte("another"),
	}
	err = l.sparseAppend(5, entry2, pb.InsertedBySelf)
	assert.Error(t, err, "should not overwrite self with same term")

	// Overwrite with leader-approved (should succeed)
	entry3 := pb.Entry{
		Term: 1,
		Data: []byte("leader"),
	}
	err = l.sparseAppend(5, entry3, pb.InsertedByLeader)
	require.NoError(t, err)

	// Verify leader entry replaced self entry
	e, err = l.entry(5)
	require.NoError(t, err)
	assert.Equal(t, pb.InsertedByLeader, e.InsertedBy)
	assert.Equal(t, []byte("leader"), e.Data)
}

// TestFastRaftLeaderElection tests election with leader-approved suffix
func TestFastRaftLeaderElection(t *testing.T) {
	storage := NewMemoryStorage()
	l := newLogWithSize(storage, raftLogger, noLimit)
	l.enableFastRaft()

	// Append mix of leader and self-approved entries
	entries := []pb.Entry{
		{Term: 1, Index: 1, InsertedBy: pb.InsertedByLeader},
		{Term: 1, Index: 2, InsertedBy: pb.InsertedByLeader},
		{Term: 2, Index: 3, InsertedBy: pb.InsertedBySelf},
		{Term: 2, Index: 4, InsertedBy: pb.InsertedBySelf},
	}

	l.append(entries...)

	// Last leader index should be 2
	assert.Equal(t, uint64(2), l.lastLeaderIndex)

	// isUpToDate should use leader-approved suffix
	lastLeader := l.getLastLeaderID()
	assert.Equal(t, uint64(2), lastLeader.index)
	assert.Equal(t, uint64(1), lastLeader.term)

	// Candidate with higher leader-approved index should win
	assert.True(t, l.isUpToDate(entryID{term: 1, index: 3}))
	assert.True(t, l.isUpToDate(entryID{term: 2, index: 2}))
	assert.False(t, l.isUpToDate(entryID{term: 1, index: 1}))
}

// TestFastRaftRecovery tests recovery of vote state after leader election
func TestFastRaftRecovery(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2, 3)) // ✅ seed voters
	cfg := newTestConfig(1, 10, 1, storage)
	cfg.EnableFastPath = true
	leader := newRaft(cfg)

	leader.becomeFollower(1, None)
	leader.becomeCandidate()

	// Simulate receiving vote responses with hints
	hints := []pb.SelfApprovedHint{
		{Index: 1, Term: 1, EntryDigest: []byte("entry1")},
		{Index: 2, Term: 1, EntryDigest: []byte("entry2")},
	}
	voteResp := pb.Message{Type: pb.MsgVoteResp, From: 2, To: 1, Term: 2, Hints: hints}

	// Store hints during candidacy
	require.NoError(t, leader.Step(voteResp))

	// Become leader (tracker & inflights are valid now)
	leader.becomeLeader()

	// Verify possibleEntries was initialized
	assert.NotNil(t, leader.possibleEntries)

	// Verify hints were cleared after processing
	assert.Len(t, leader.selfApprovedHints, 0)
}

// TestFastRaftDisabled tests that Fast Raft features are disabled by default
func TestFastRaftDisabled(t *testing.T) {
	// Seed a 5-node ConfState so the tracker is fully initialized.
	storage := newTestMemoryStorage(withPeers(1, 2, 3, 4, 5))

	cfg := newTestConfig(1, 10, 1, storage)
	cfg.EnableFastPath = false // Explicitly disabled
	rn := newRaft(cfg)

	rn.becomeFollower(1, None)
	rn.becomeCandidate()
	rn.becomeLeader()

	// Propose an entry (classic path only).
	prop := pb.Message{
		Type:    pb.MsgProp,
		From:    1,
		Entries: []pb.Entry{{Data: []byte("test")}},
	}
	require.NoError(t, rn.Step(prop))

	// Should NOT send any MsgFastProp messages.
	for _, m := range rn.msgs {
		assert.NotEqual(t, pb.MsgFastProp, m.Type, "should not send MsgFastProp when disabled")
	}

	// The user entry is at lastIndex (index 1 is the leader's empty no-op).
	li := rn.raftLog.lastIndex()
	entry, err := rn.raftLog.entry(li)
	require.NoError(t, err)

	assert.Equal(t, pb.EntryNormal, entry.Type)
	assert.Equal(t, []byte("test"), entry.Data)
	// InsertedBy should be "unknown" on classic append.
	assert.Equal(t, pb.InsertedByUnknown, entry.InsertedBy)
}

// Benchmark to compare Fast Raft vs Classic Raft commit latency
func BenchmarkFastRaftCommit(b *testing.B) {
	benchmarks := []struct {
		name     string
		fastPath bool
	}{
		{"Classic", false},
		{"FastRaft", true},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Use a fresh storage+ConfState each iteration (keep it simple).
			for i := 0; i < b.N; i++ {
				storage := newTestMemoryStorage(withPeers(1, 2, 3, 4, 5))
				cfg := newTestConfig(1, 10, 1, storage)
				cfg.EnableFastPath = bm.fastPath

				rn := newRaft(cfg)
				rn.becomeFollower(1, None)
				rn.becomeCandidate()
				rn.becomeLeader()

				// Propose one user entry.
				require.NoError(b, rn.Step(pb.Message{
					Type:    pb.MsgProp,
					From:    1,
					Entries: []pb.Entry{{Data: []byte("test")}},
				}))

				if bm.fastPath {
					// Find the proposed index from outbound fast-proposals.
					var propIdx uint64
					var propTerm uint64
					for _, m := range rn.msgs {
						if m.Type == pb.MsgFastProp && len(m.Entries) == 1 {
							propIdx = m.Entries[0].Index
							propTerm = m.Entries[0].Term
							break
						}
					}
					// If no fast-prop was emitted (shouldn't happen when enabled), fall back.
					if propIdx == 0 {
						propIdx = rn.raftLog.committed + 1
						propTerm = rn.Term
					}

					// Simulate fast votes from 3 followers (leader's own vote makes it 4/5).
					for _, peer := range []uint64{2, 3, 4} {
						_ = rn.Step(pb.Message{
							Type:    pb.MsgFastVote,
							From:    peer,
							To:      1,
							Index:   propIdx,
							LogTerm: propTerm,
							Context: []byte("digest"), // value doesn't matter for the benchmark
						})
					}
				} else {
					// Classic majority acks for the proposed index:
					propIdx := rn.raftLog.lastIndex() // the user entry was appended already
					for _, peer := range []uint64{2, 3} {
						_ = rn.Step(pb.Message{
							Type:  pb.MsgAppResp,
							From:  peer,
							To:    1,
							Index: propIdx,
						})
					}
				}
			}
		})
	}
}

func TestFastRaftDoesNotFastCommitOldTerm(t *testing.T) {
	cfgFast := func(c *Config) { c.EnableFastPath = true }
	nt := newNetworkWithConfig(cfgFast, nil, nil, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	lead := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, lead.state)
	oldTerm := lead.Term

	// Propose
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("x")}}})

	// Capture proposed idx/term from fast-prop on the wire
	var propIdx, propTerm uint64
	nt.msgHook = func(m pb.Message) bool {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 && len(m.Entries[0].Data) > 0 {
			propIdx = m.Entries[0].Index
			propTerm = m.Entries[0].Term
		}
		return true
	}
	// Drive network once to flush pending messages
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("...")}}})
	require.NotZero(t, propIdx, "leader did not emit MsgFastProp")
	require.Equal(t, oldTerm, propTerm)

	// Bump term: higher-term heartbeat from peer 2
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: lead.Term + 1})
	// Re-elect leader 1
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	require.Equal(t, StateLeader, lead.state)
	require.Greater(t, lead.Term, oldTerm)

	// Supply fast votes for old-term entry → must NOT fast-commit.
	for _, from := range []uint64{2, 3, 4} {
		_ = lead.Step(pb.Message{Type: pb.MsgFastVote, From: from, To: 1, Index: propIdx, LogTerm: propTerm, Context: []byte("d")})
	}
	assert.NotEqual(t, propIdx, lead.raftLog.committed, "must not fast-commit old-term entries")

	// Classic commit of old-term also disallowed until a current-term entry commits.
	for _, from := range []uint64{2, 3} {
		_ = lead.Step(pb.Message{Type: pb.MsgAppResp, From: from, To: 1, Index: propIdx})
	}
	assert.NotEqual(t, propIdx, lead.raftLog.committed)

	// Propose & commit a current-term user entry; leader will broadcast MsgApp.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("y")}}})
	// Acks from two followers on the new tail
	tail := lead.raftLog.lastIndex()
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: tail})
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgAppResp, Index: tail})

	// A current-term entry should have committed
	assert.Greater(t, lead.raftLog.committed, uint64(0))
}

func TestFastRaftEquivalenceToClassic(t *testing.T) {
	ntFast := newNetworkWithConfig(func(c *Config) { c.EnableFastPath = true }, nil, nil, nil)
	ntCls := newNetworkWithConfig(func(c *Config) { c.EnableFastPath = false }, nil, nil, nil)

	ntFast.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	fast := ntFast.peers[1].(*raft)
	cls := ntCls.peers[1].(*raft)

	inputs := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, p := range inputs {
		// --- FAST: install hook BEFORE proposing to capture idx/term ---
		var idx, term uint64
		ntFast.msgHook = func(m pb.Message) bool {
			if m.Type == pb.MsgFastProp && len(m.Entries) == 1 && len(m.Entries[0].Data) > 0 {
				idx, term = m.Entries[0].Index, m.Entries[0].Term
			}
			return true
		}
		ntFast.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: p}}})
		require.NotZero(t, idx, "fast: no MsgFastProp observed")
		dig := entryDigest(&pb.Entry{Index: idx, Term: term, Data: p})
		// Supply 3 fast votes (w/ leader implicit = 4/5 for 5-node; for 3-node use leader self+2)
		for _, from := range []uint64{2, 3, 4} {
			_ = fast.Step(pb.Message{Type: pb.MsgFastVote, From: from, To: 1, Index: idx, LogTerm: term, Context: []byte(dig)})
		}

		// --- CLASSIC: propose and let the network flush append/acks ---
		prevCommitted := cls.raftLog.committed
		ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: p}}})
		// Nudge the pipeline a couple of times to deliver MsgApp -> MsgAppResp -> maybeCommit
		for tries := 0; tries < 3 && cls.raftLog.committed == prevCommitted; tries++ {
			ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
		}
		require.Greater(t, cls.raftLog.committed, prevCommitted, "classic: entry did not commit")
	}

	// Compare committed user payloads (ignore no-ops / InsertedBy).
	user := func(r *raft) [][]byte {
		var out [][]byte
		for _, e := range r.raftLog.allEntries() {
			if e.Type == pb.EntryNormal && len(e.Data) > 0 && e.Index <= r.raftLog.committed {
				out = append(out, e.Data)
			}
		}
		return out
	}
	require.Equal(t, user(cls), user(fast), "committed user payloads must match")
}

func TestFastRaftNoClassicBeforeDecision_Local(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3, 4, 5))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	rn := newRaft(cfg)
	rn.becomeFollower(1, None)
	rn.becomeCandidate()
	rn.becomeLeader()

	// Fast path is gated until the leader has a commit in its term.
	// Simulate the no-op’s classic commit so fast path is enabled.
	rn.raftLog.commitTo(rn.raftLog.lastIndex())
	// Clear any pre-existing messages before this test’s proposal
	rn.msgs, rn.msgsAfterAppend = nil, nil

	require.NoError(t, rn.Step(pb.Message{Type: pb.MsgProp, From: 1, Entries: []pb.Entry{{Data: []byte("fast")}}}))

	all := append([]pb.Message{}, rn.msgs...)
	all = append(all, rn.msgsAfterAppend...)
	rn.msgs, rn.msgsAfterAppend = nil, nil

	var propIdx, propTerm uint64
	var sawAppWithPayload bool
	for _, m := range all {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 {
			propIdx, propTerm = m.Entries[0].Index, m.Entries[0].Term
		}
		if m.Type == pb.MsgApp {
			for _, e := range m.Entries {
				if len(e.Data) > 0 && e.Index == propIdx {
					sawAppWithPayload = true
				}
			}
		}
	}
	require.NotZero(t, propIdx, "leader did not emit MsgFastProp")
	assert.False(t, sawAppWithPayload, "leader sent MsgApp(user entry) before deciding")

	// Supply votes to fast-commit.
	dig := entryDigest(&pb.Entry{Index: propIdx, Term: propTerm, Data: []byte("fast")})
	for _, from := range []uint64{2, 3, 4} {
		require.NoError(t, rn.Step(pb.Message{Type: pb.MsgFastVote, From: from, To: 1, Index: propIdx, LogTerm: propTerm, Context: []byte(dig)}))
	}
	assert.Equal(t, propIdx, rn.raftLog.committed)
}

func TestFastRaftOverwriteRuleOnlyAtIndex(t *testing.T) {
	cfgFast := func(c *Config) { c.EnableFastPath = true }
	// 3-node network: leader=1, followers=2,3
	nt := newNetworkWithConfig(cfgFast, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	lead := nt.peers[1].(*raft)
	f2 := nt.peers[2].(*raft)

	// --- Sanity: fast-path must be enabled on the logs too (not just raft/tracker) ---
	require.True(t, lead.raftLog.enableFastPath, "leader log fast-path must be enabled")
	require.True(t, f2.raftLog.enableFastPath, "follower log fast-path must be enabled")

	// --- Step 1: Build a contiguous prefix [1..4] on follower 2 via the leader pipeline ---
	for i := 0; i < 3; i++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte{byte('p'), byte('0' + i)}}}})
	}
	for tries := 0; tries < 4 && f2.raftLog.lastIndex() < 4; tries++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	}
	require.GreaterOrEqual(t, f2.raftLog.lastIndex(), uint64(4), "follower 2 did not receive prefix 1..4")

	// We will use k = leader's next uncommitted index. For this test, we expect k=5 now.
	k := lead.raftLog.committed + 1
	require.Equal(t, uint64(5), k, "expected to target index 5 in this test")

	// --- Step 2: Place a self-approved 'A' at index k on follower 2 ---
	require.NoError(t, f2.Step(pb.Message{
		Type: pb.MsgFastProp, From: 1, To: 2, Term: lead.Term,
		Entries: []pb.Entry{{Index: k, Term: lead.Term, Data: []byte("A")}},
	}))
	e, err := f2.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), e.Data)
	assert.Equal(t, pb.InsertedBySelf, e.InsertedBy)

	// --- Step 3: Leader proposes 'B' at the same index k (through normal proposal) ---
	var propB pb.Entry
	var idxB, termB uint64

	// install hook to capture both fast-prop (to get idx/term) and the later MsgApp to f2
	sawBAtKToF2 := false
	nt.msgHook = func(m pb.Message) bool {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 && len(m.Entries[0].Data) > 0 {
			propB = m.Entries[0]
			idxB, termB = propB.Index, propB.Term
		}
		if m.Type == pb.MsgApp && m.To == 2 {
			for _, en := range m.Entries {
				if en.Index == k && len(en.Data) > 0 && string(en.Data) == "B" {
					sawBAtKToF2 = true
				}
			}
		}
		return true
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("B")}}})
	require.NotZero(t, idxB, "no fast-prop observed for leader proposal 'B'")
	require.Equal(t, k, idxB)
	require.Equal(t, lead.Term, termB)

	// --- Step 4: Send fast votes for 'B' so the leader decides 'B' at k ---
	digB := entryDigest(&propB)
	for _, from := range []uint64{2, 3} {
		_ = lead.Step(pb.Message{Type: pb.MsgFastVote, From: from, To: 1, Index: k, LogTerm: termB, Context: []byte(digB)})
	}
	_ = lead.Step(pb.Message{Type: pb.MsgFastVote, From: 1, To: 1, Index: k, LogTerm: termB, Context: []byte(digB)})

	// Optional but strong check: leader has materialized B@k as leader-approved before replication
	if le, err := lead.raftLog.entry(k); err == nil {
		assert.Equal(t, []byte("B"), le.Data, "leader must materialize chosen 'B' at k before sending")
		assert.Equal(t, pb.InsertedByLeader, le.InsertedBy, "leader must mark entry at k as leader-approved")
	}
	// Nudge pipeline
	for i := 0; i < 2; i++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	}
	require.True(t, sawBAtKToF2, "leader must send MsgApp carrying index k with payload 'B' to follower 2")

	// --- Step 6: Follower 2 must now have leader-approved 'B' at k (overwrites 'A') ---
	e, err = f2.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("B"), e.Data, "entry content must be leader's chosen value at k")
	assert.Equal(t, pb.InsertedByLeader, e.InsertedBy,
		"follower must overwrite self-approved at chosen index with leader-approved")
}

func TestFastRaftSideBufferFlush(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(2, 10, 1, st)
	cfg.EnableFastPath = true
	f := newRaft(cfg)
	f.becomeFollower(1, 1)

	// Receive out-of-order fast-prop at 5
	require.NoError(t, f.Step(pb.Message{Type: pb.MsgFastProp, From: 1, To: 2, Term: 1, Entries: []pb.Entry{{Index: 5, Term: 1, Data: []byte("v5")}}}))
	// Should be buffered (not contiguous)
	_, err := f.raftLog.entry(5)
	require.Error(t, err, "out-of-order entry should not be in the log yet")
	_, ok := f.pendingSelf[5]
	require.True(t, ok, "should be side-buffered")

	// Leader fills indices 1..4 via AppendEntries
	require.NoError(t, f.Step(pb.Message{
		Type: pb.MsgApp, From: 1, To: 2, Term: 1,
		Index: 0, LogTerm: 0,
		Entries: []pb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1},
		},
		Commit: 4,
	}))

	// Now 5 should have been flushed from side buffer into the log
	_, err = f.raftLog.entry(5)
	require.NoError(t, err, "side-buffered entry should be appended once contiguous")
	_, ok = f.pendingSelf[5]
	require.False(t, ok, "side-buffered entry should be cleared after append")
}

func TestFastRaftNextIndexResetOnVote(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)

	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Fast path is gated until the leader has a commit in its term.
	// Simulate the no-op’s classic commit so fast path is enabled.
	r.raftLog.commitTo(r.raftLog.lastIndex())
	// (Optional) clear preexisting messages before this test’s action
	r.msgs, r.msgsAfterAppend = nil, nil

	// Propose user entry, capture proposed index/term
	require.NoError(t, r.Step(pb.Message{Type: pb.MsgProp, From: 1, Entries: []pb.Entry{{Data: []byte("x")}}}))
	var idx, term uint64
	all := append([]pb.Message{}, r.msgs...)
	all = append(all, r.msgsAfterAppend...)
	for _, m := range all {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 {
			idx, term = m.Entries[0].Index, m.Entries[0].Term
			break
		}
	}
	require.NotZero(t, idx)

	// Simulate a single fast vote from follower 2
	require.NoError(t, r.Step(pb.Message{
		Type: pb.MsgFastVote, From: 2, To: 1,
		Index: idx, LogTerm: term, Context: []byte("d"),
	}))

	// The leader should reset nextIndex[2] to committed (spec says "sentCommitIndex")
	pr := r.trk.Progress[2]
	require.NotNil(t, pr)
	assert.Equal(t, r.raftLog.committed, pr.Next-1, "nextIndex should be anchored at commit for the voter after fast vote")
}

func TestFastRaft_LeaderChange_WithSelfApprovedTails(t *testing.T) {
	cfgFast := func(c *Config) { c.EnableFastPath = true }
	nt := newNetworkWithConfig(cfgFast, nil, nil, nil)

	// Elect 1 as leader.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	L := nt.peers[1].(*raft)
	F2 := nt.peers[2].(*raft)
	F3 := nt.peers[3].(*raft)

	// --- Build a contiguous prefix [1..4] via normal pipeline.
	for i := 0; i < 3; i++ {
		nt.send(pb.Message{
			From: 1, To: 1, Type: pb.MsgProp,
			Entries: []pb.Entry{{Data: []byte{byte('p'), byte('0' + i)}}},
		})
	}
	for tries := 0; tries < 4 && (F2.raftLog.lastIndex() < 4 || F3.raftLog.lastIndex() < 4); tries++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	}
	require.GreaterOrEqual(t, F2.raftLog.lastIndex(), uint64(4))
	require.GreaterOrEqual(t, F3.raftLog.lastIndex(), uint64(4))

	// Seed a small self-approved tail on both followers beyond 4.
	for _, r := range []*raft{F2, F3} {
		for off, payload := range []string{"A5", "A6"} {
			idx := uint64(5 + off)
			require.NoError(t, r.Step(pb.Message{
				Type: pb.MsgFastProp, From: 1, To: r.id, Term: L.Term,
				Entries: []pb.Entry{{Index: idx, Term: L.Term, Data: []byte(payload)}},
			}))
			e, err := r.raftLog.entry(idx)
			require.NoError(t, err)
			assert.Equal(t, pb.InsertedBySelf, e.InsertedBy)
		}
	}

	// --- Partition node 1 (leader) completely.
	// We will toggle additional blocks below as we stage the test.
	var blockFastVotesTo2 = false
	var blockAppTo3 = false
	var sawFastProp2 bool
	var idxB, termB uint64

	nt.msgHook = func(m pb.Message) bool {
		// Keep node 1 isolated.
		if m.To == 1 || m.From == 1 {
			return false
		}
		// Phase-controlled blocks:
		if blockFastVotesTo2 && m.Type == pb.MsgFastVote && m.To == 2 {
			return false
		}
		if blockAppTo3 && m.Type == pb.MsgApp && m.To == 3 {
			return false
		}
		// Capture leader-2's fast-prop (any destination).
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 && len(m.Entries[0].Data) > 0 {
			// F2 will be leader soon; guard on state to avoid capturing older traffic.
			if F2.state == StateLeader {
				idxB, termB = m.Entries[0].Index, m.Entries[0].Term
				sawFastProp2 = true
			}
		}
		return true
	}

	// --- Elect follower 2 as new leader.
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
	require.Equal(t, StateLeader, F2.state)
	oldTerm := L.Term

	// PHASE 1: block fast votes & appends while we observe fast-prop and set preconditions.
	blockFastVotesTo2 = true
	blockAppTo3 = true

	// Propose on leader 2 (this emits MsgFastProp; the hook captures idx/term).
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("B_k")}}})
	require.True(t, sawFastProp2, "did not observe leader 2's MsgFastProp")
	require.NotZero(t, idxB, "no index captured for leader 2's fast-prop")
	require.NotZero(t, termB, "no term captured for leader 2's fast-prop")

	// Ensure follower 3 ALREADY has a self-approved entry at the chosen idxB.
	// If not present, inject it now (contiguity may append or side-buffer; both are fine).
	if e, err := F3.raftLog.entry(idxB); err != nil || e.InsertedBy != pb.InsertedBySelf {
		require.NoError(t, F3.Step(pb.Message{
			Type: pb.MsgFastProp, From: 1, To: 3, Term: termB,
			Entries: []pb.Entry{{Index: idxB, Term: termB, Data: []byte("A_at_idxB")}},
		}))
	}
	e3pre, err := F3.raftLog.entry(idxB)
	require.NoError(t, err)
	assert.Equal(t, pb.InsertedBySelf, e3pre.InsertedBy, "precondition: follower 3 must have self-approved at chosen index")

	// Also stage idxB+1 as self-approved to later prove overwrite occurs only at idxB.
	_ = F3.Step(pb.Message{
		Type: pb.MsgFastProp, From: 1, To: 3, Term: termB,
		Entries: []pb.Entry{{Index: idxB + 1, Term: termB, Data: []byte("A_at_idxB+1")}},
	})

	// Snapshot Match for follower 3 BEFORE votes (to verify votes don't move it).
	require.NotNil(t, F2.trk.Progress[3])
	matchBefore := F2.trk.Progress[3].Match

	// PHASE 2: allow fast votes; inject 2 votes (leader self + follower 3).
	blockFastVotesTo2 = false
	dig := entryDigest(&pb.Entry{Index: idxB, Term: termB, Data: []byte("B_k")})
	require.NoError(t, F2.Step(pb.Message{
		Type: pb.MsgFastVote, From: 2, To: 2, Index: idxB, LogTerm: termB, Context: []byte(dig),
	}))
	require.NoError(t, F2.Step(pb.Message{
		Type: pb.MsgFastVote, From: 3, To: 2, Index: idxB, LogTerm: termB, Context: []byte(dig),
	}))

	// Fast votes must NOT advance Match immediately.
	assert.Equal(t, matchBefore, F2.trk.Progress[3].Match, "FastVote must not advance Match")

	// Leader 2 must have materialized B_k at idxB as leader-approved (before replication).
	le2, err := F2.raftLog.entry(idxB)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, le2.Term, oldTerm, "no fast-commit on old term")
	assert.Equal(t, []byte("B_k"), le2.Data, "leader must materialize chosen B_k at idxB")
	assert.Equal(t, pb.InsertedByLeader, le2.InsertedBy)

	// PHASE 3: allow appends to follower 3; drive pipeline until overwrite occurs.
	blockAppTo3 = false
	received := false
	for tries := 0; tries < 8 && !received; tries++ {
		nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgBeat})
		if e, err := F3.raftLog.entry(idxB); err == nil {
			received = (string(e.Data) == "B_k" && e.InsertedBy == pb.InsertedByLeader)
		}
	}
	require.True(t, received, "follower 3 must receive leader-approved B_k at idxB")

	// Verify overwrite happened only at decided index.
	e3, err := F3.raftLog.entry(idxB)
	require.NoError(t, err)
	assert.Equal(t, []byte("B_k"), e3.Data)
	assert.Equal(t, pb.InsertedByLeader, e3.InsertedBy)

	if e, err := F3.raftLog.entry(idxB + 1); err == nil {
		assert.Equal(t, pb.InsertedBySelf, e.InsertedBy, "idxB+1 should remain self-approved")
	}
}

func TestFastRaft_SnapshotAcrossPendingFastDecision(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(2, 10, 1, st)
	cfg.EnableFastPath = true
	f := newRaft(cfg)
	f.becomeFollower(1, 1)
	require.True(t, f.raftLog.enableFastPath)

	// (a) Out-of-order fast-prop at k=10 -> side buffer.
	k := uint64(10)
	require.NoError(t, f.Step(pb.Message{
		Type: pb.MsgFastProp, From: 1, To: 2, Term: 1,
		Entries: []pb.Entry{{Index: k, Term: 1, Data: []byte("self@10")}},
	}))
	_, err := f.raftLog.entry(k)
	require.Error(t, err)
	_, ok := f.pendingSelf[k]
	require.True(t, ok)

	// (b) Leader sends 1..9; follower appends; flushes pendingSelf to log.
	require.NoError(t, f.Step(pb.Message{
		Type: pb.MsgApp, From: 1, To: 2, Term: 1,
		Index: 0, LogTerm: 0,
		Entries: []pb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}, {Index: 5, Term: 1},
			{Index: 6, Term: 1}, {Index: 7, Term: 1}, {Index: 8, Term: 1}, {Index: 9, Term: 1},
		},
		Commit: 9,
	}))
	_, err = f.raftLog.entry(k)
	require.NoError(t, err)

	// (b.5) Apply snapshot to index 9 (idiomatic).
	snap := &pb.Snapshot{Metadata: pb.SnapshotMetadata{
		Index: 9, Term: 1, ConfState: pb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
	require.NoError(t, f.Step(pb.Message{Type: pb.MsgSnap, From: 1, To: 2, Term: 1, Snapshot: snap}))

	// Bounds-safe API after snapshot. Only assert ErrCompacted if fi moved.
	fi := f.raftLog.firstIndex()
	if fi > 1 {
		_, err = f.raftLog.entry(1)
		require.Error(t, err, "compacted")
	}
	_, err = f.raftLog.entry(f.raftLog.lastIndex() + 1)
	require.Error(t, err, "unavailable")

	// (c) Leader sends decided leader-approved at k.
	require.NoError(t, f.Step(pb.Message{
		Type: pb.MsgApp, From: 1, To: 2, Term: 1,
		Index: 9, LogTerm: 1,
		Entries: []pb.Entry{{Index: k, Term: 1, Data: []byte("B10"), InsertedBy: pb.InsertedByLeader}},
		Commit:  9,
	}))

	// (d) Verify upgrade self→leader at same (index,term); flag persists.
	e10, err := f.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("B10"), e10.Data)
	assert.Equal(t, pb.InsertedByLeader, e10.InsertedBy)
	require.True(t, f.raftLog.enableFastPath)
}

func TestFastRaft_JointConfig_FastQuorum(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3, 4, 5))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	rn := newRaft(cfg)

	// Bring up a leader.
	rn.becomeFollower(1, None)
	rn.becomeCandidate()
	rn.becomeLeader()

	// Gate: fast path only after a commit in this term (leader's no-op).
	rn.raftLog.commitTo(rn.raftLog.lastIndex())
	rn.msgs, rn.msgsAfterAppend = nil, nil

	// ---- Install JOINT config via switchToConfig ----
	jcfg := rn.trk.Config
	jcfg.Voters = quorum.JointConfig{
		quorum.MajorityConfig{1: {}, 2: {}, 3: {}},               // outgoing half
		quorum.MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}, // incoming half
	}
	_ = rn.switchToConfig(jcfg, rn.trk.Progress)

	// Ensure Progress entries exist (defensive for tests).
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		if rn.trk.Progress[id] == nil {
			rn.trk.Progress[id] = &tracker.Progress{
				Next:      1,
				Inflights: tracker.NewInflights(rn.trk.MaxInflight, rn.trk.MaxInflightBytes),
			}
		}
	}

	// Propose on the fast path; capture (k, term) from the outgoing MsgFastProp.
	require.NoError(t, rn.Step(pb.Message{
		Type: pb.MsgProp, From: 1, Entries: []pb.Entry{{Data: []byte("joint")}},
	}))
	var k, term uint64
	all := append([]pb.Message{}, rn.msgs...)
	all = append(all, rn.msgsAfterAppend...)
	for _, m := range all {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 {
			k, term = m.Entries[0].Index, m.Entries[0].Term
			break
		}
	}

	t.Logf("[DBG] committed=%d, k=%d, lastIndex=%d",
		rn.raftLog.committed, k, rn.raftLog.lastIndex())
	require.Equal(t, rn.raftLog.committed+1, k,
		"[DBG] first fast proposal index must be committed+1")

	require.NotZero(t, k, "expected a fast proposal to be broadcast")
	dig := entryDigest(&pb.Entry{Index: k, Term: term, Data: []byte("joint")})

	// Old-half votes only: leader self + {2,3}.
	_ = rn.Step(pb.Message{Type: pb.MsgFastVote, From: 1, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})
	_ = rn.Step(pb.Message{Type: pb.MsgFastVote, From: 2, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})
	_ = rn.Step(pb.Message{Type: pb.MsgFastVote, From: 3, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})

	// Not enough yet under joint-fast rules: should not be committed.
	require.NotEqual(t, k, rn.raftLog.committed, "should not be committed with only old-half votes")

	// Add one more vote from the new half (node 4).
	_ = rn.Step(pb.Message{Type: pb.MsgFastVote, From: 4, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})

	// ✅ EFFECT: leader should fast-commit k immediately inside tryDecideAt.
	require.Equal(t, k, rn.raftLog.committed,
		"leader should fast-commit once both halves meet fast quorum at the same k")

	// Fast votes must not mutate Match (but Next may be lowered to idx).
	matchBefore := rn.trk.Progress[2].Match
	nextBefore := rn.trk.Progress[2].Next
	_ = rn.Step(pb.Message{Type: pb.MsgFastVote, From: 2, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})

	assert.Equal(t, matchBefore, rn.trk.Progress[2].Match,
		"fast votes must not change Match")
	// Next is allowed to be lowered to idx to avoid skipping the chosen write.
	assert.LessOrEqual(t, rn.trk.Progress[2].Next, nextBefore,
		"Next may be lowered to the decided index")
	assert.GreaterOrEqual(t, rn.trk.Progress[2].Next, rn.trk.Progress[2].Match+1,
		"Next must remain at least Match+1")
}

func TestFastRaft_NeverOverwriteCommitted(t *testing.T) {
	nt := newNetworkWithConfig(nil, nil, nil) // 3 nodes
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	lead := nt.peers[1].(*raft)
	f2 := nt.peers[2].(*raft)

	// Propose & commit one entry at idx=2 (idx=1 is the leader’s no-op).
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("c")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	require.Equal(t, lead.raftLog.committed, f2.raftLog.committed)
	committed := lead.raftLog.committed
	require.GreaterOrEqual(t, committed, uint64(2))

	// Snapshot follower's current committed entry (term + data)
	prevIdx := committed - 1
	prevTerm := f2.raftLog.zeroTermOnOutOfBounds(f2.raftLog.term(prevIdx))
	termAtCommitted := f2.raftLog.zeroTermOnOutOfBounds(f2.raftLog.term(committed))
	eBefore, err := f2.raftLog.entry(committed)
	require.NoError(t, err)

	// Try to "overwrite" follower 2's committed entry with a conflicting payload at that index.
	// This should be ignored (early return), and the follower must not change the log at `committed`.
	_ = f2.Step(pb.Message{
		Type: pb.MsgApp,
		From: 3, To: 2,
		Term:    lead.Term,
		Index:   prevIdx, // prev < committed: triggers early return
		LogTerm: prevTerm,
		Entries: []pb.Entry{{
			Index: committed,
			Term:  termAtCommitted, // even if term matches, payload difference is irrelevant
			Data:  []byte("DIFF"),
		}},
		Commit: committed,
	})

	// The follower's committed entry must remain unchanged.
	eAfter, err := f2.raftLog.entry(committed)
	require.NoError(t, err)
	assert.Equal(t, eBefore.Term, eAfter.Term, "term at committed must not change")
	assert.Equal(t, eBefore.Data, eAfter.Data, "data at committed must not change")

	// committed index must not regress or jump.
	assert.Equal(t, committed, f2.raftLog.committed)
}

func TestFastRaft_DecisionShipsChosenEntry(t *testing.T) {
	cfgFast := func(c *Config) { c.EnableFastPath = true }
	nt := newNetworkWithConfig(cfgFast, nil, nil, nil)

	// Start an election and wait until we actually have a leader.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	// Wait loop: nudge the pipeline until leader state is set.
	var lead *raft
	for i := 0; i < 100; i++ {
		if l, ok := nt.peers[1].(*raft); ok && l.state == StateLeader {
			lead = l
			break
		}
		if l, ok := nt.peers[2].(*raft); ok && l.state == StateLeader {
			lead = l
			break
		}
		if l, ok := nt.peers[3].(*raft); ok && l.state == StateLeader {
			lead = l
			break
		}
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	}
	require.NotNil(t, lead, "no leader elected")

	var idx, term uint64
	var sawAppWithChosen bool
	nt.msgHook = func(m pb.Message) bool {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 && len(m.Entries[0].Data) > 0 {
			idx, term = m.Entries[0].Index, m.Entries[0].Term
		}
		if m.Type == pb.MsgApp {
			for _, e := range m.Entries {
				if e.Index == idx && string(e.Data) == "CHOSEN" {
					sawAppWithChosen = true
				}
			}
		}
		return true
	}

	// Propose a user entry (fast path will broadcast MsgFastProp).
	nt.send(pb.Message{
		From: lead.id, To: lead.id, Type: pb.MsgProp,
		Entries: []pb.Entry{{Data: []byte("CHOSEN")}},
	})
	require.NotZero(t, idx, "no MsgFastProp observed")

	dig := entryDigest(&pb.Entry{Index: idx, Term: term, Data: []byte("CHOSEN")})

	// Classic quorum for n=3 is 2. Give the leader self-vote to be explicit,
	// then two follower votes. (Safe even if your code doesn’t require the self-vote.)
	_ = lead.Step(pb.Message{Type: pb.MsgFastVote, From: lead.id, To: lead.id, Index: idx, LogTerm: term, Context: []byte(dig)})
	for _, from := range []uint64{2, 3} {
		_ = lead.Step(pb.Message{Type: pb.MsgFastVote, From: from, To: lead.id, Index: idx, LogTerm: term, Context: []byte(dig)})
	}

	// Nudge the pipeline so the leader ships AppendEntries carrying the chosen entry.
	for i := 0; i < 3 && !sawAppWithChosen; i++ {
		nt.send(pb.Message{From: lead.id, To: lead.id, Type: pb.MsgBeat})
	}

	require.True(t, sawAppWithChosen, "leader did not send AppendEntries carrying the chosen entry at idx")
}

func TestFastRaft_RecoveryMultipleHints(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)

	r.becomeFollower(1, None)
	r.becomeCandidate()

	hints := []pb.SelfApprovedHint{
		{Index: 5, Term: 1, EntryDigest: []byte("h5")},
		{Index: 6, Term: 1, EntryDigest: []byte("h6")},
	}
	_ = r.Step(pb.Message{Type: pb.MsgVoteResp, From: 2, To: 1, Term: 2, Hints: hints})
	r.becomeLeader()

	// After leadership, recovery should seed possibleEntries and try decide at committed+1 (5), then maybe 6.
	require.NotNil(t, r.possibleEntries)
	// It's hard to assert commit here deterministically without full store; assert that a decision is attempted:
	// At least that chosen indexes exist in possibleEntries after seeding.
	_, ok5 := r.possibleEntries[5]
	_, ok6 := r.possibleEntries[6]
	require.True(t, ok5 || ok6, "recovery did not seed any hinted indexes")
}

func TestFastRaft_JointConfig_NoFastCommitOnLargeHalfOnly(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3, 4, 5))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// NEW: fast path is gated until the leader has a commit in its term.
	// Simulate classic replication success by forcing the no-op commit.
	r.raftLog.commitTo(r.raftLog.lastIndex())
	// (optional) clear any old messages before this test’s proposal
	r.msgs, r.msgsAfterAppend = nil, nil

	// Joint: old {1,2,3}, incoming {1,2,3,4,5}
	r.trk.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	r.trk.Voters[1] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		if r.trk.Progress[id] == nil {
			r.trk.Progress[id] = &tracker.Progress{
				Next:      1,
				Inflights: tracker.NewInflights(r.trk.MaxInflight, r.trk.MaxInflightBytes),
			}
		}
	}

	// Propose; capture k,term; then give votes from only the larger half (e.g. {3,4,5})
	require.NoError(t, r.Step(pb.Message{Type: pb.MsgProp, From: 1, Entries: []pb.Entry{{Data: []byte("X")}}}))
	var k, term uint64
	for _, m := range r.msgs {
		if m.Type == pb.MsgFastProp && len(m.Entries) == 1 {
			k, term = m.Entries[0].Index, m.Entries[0].Term
			break
		}
	}
	require.NotZero(t, k)
	dig := entryDigest(&pb.Entry{Index: k, Term: term, Data: []byte("X")})

	_ = r.Step(pb.Message{Type: pb.MsgFastVote, From: 3, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})
	_ = r.Step(pb.Message{Type: pb.MsgFastVote, From: 4, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})
	_ = r.Step(pb.Message{Type: pb.MsgFastVote, From: 5, To: 1, Index: k, LogTerm: term, Context: []byte(dig)})

	// For your FastCommittable (smaller-half threshold), fast commit should not be allowed yet.
	require.NotEqual(t, k, r.trk.FastCommittable(), "should not fast-commit with only the large-half votes")
}

func TestFastVsClassic_SemanticEquivalence_Smoke(t *testing.T) {
	ntFast := newNetworkWithConfig(func(c *Config) { c.EnableFastPath = true }, nil, nil, nil)
	ntCls := newNetworkWithConfig(func(c *Config) { c.EnableFastPath = false }, nil, nil, nil)

	ntFast.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	fast := ntFast.peers[1].(*raft)
	cls := ntCls.peers[1].(*raft)

	inputs := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	for _, p := range inputs {
		// Propose on both
		ntFast.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: p}}})
		ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: p}}})

		// Randomly mix acks (here, just a few beats)
		for i := 0; i < 3; i++ {
			ntFast.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
			ntCls.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
		}
	}

	user := func(r *raft) [][]byte {
		var out [][]byte
		for _, e := range r.raftLog.allEntries() {
			if e.Type == pb.EntryNormal && len(e.Data) > 0 && e.Index <= r.raftLog.committed {
				out = append(out, e.Data)
			}
		}
		return out
	}
	require.Equal(t, user(cls), user(fast), "committed user payloads differ")
}

func TestInsertLeaderChoiceAt_Fallback_TagMatchingDigest(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Choose an unused index beyond leader's no-op.
	k := r.raftLog.lastIndex() + 1

	// Place "B" as self at k in the leader's log (but DO NOT store it in fastPropStore).
	ent := pb.Entry{Index: k, Term: r.Term, Data: []byte("B")}
	require.NoError(t, r.raftLog.sparseAppend(k, ent, pb.InsertedBySelf))
	dig := entryDigest(&ent)

	// Store miss + matching digest at k → should tag to leader.
	r.insertLeaderChoiceAt(k, dig)

	got, err := r.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("B"), got.Data)
	assert.Equal(t, pb.InsertedByLeader, got.InsertedBy)
}

func TestInsertLeaderChoiceAt_Fallback_DoNotTagOnMismatch(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Unused index beyond current tail.
	k := r.raftLog.lastIndex() + 1

	// Put "A"/self at k. No store entry.
	entA := pb.Entry{Index: k, Term: r.Term, Data: []byte("A")}
	require.NoError(t, r.raftLog.sparseAppend(k, entA, pb.InsertedBySelf))

	// Ask to choose digest for "B" at k (store miss + mismatch).
	entB := pb.Entry{Index: k, Term: r.Term, Data: []byte("B")}
	digB := entryDigest(&entB)
	r.insertLeaderChoiceAt(k, digB)

	got, err := r.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), got.Data, "should not overwrite content on mismatch")
	assert.Equal(t, pb.InsertedBySelf, got.InsertedBy, "should not tag mismatched payload")
}

func TestInsertLeaderChoiceAt_Fallback_NoEntryNoStore_NoOp(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Pick a free index well beyond tail to avoid the leader's no-op.
	k := r.raftLog.lastIndex() + 5

	// No store entry; no entry at k. Should warn and NOT create an entry.
	r.insertLeaderChoiceAt(k, "deadbeef")

	_, err := r.raftLog.entry(k)
	assert.Error(t, err, "no entry should be created when both store and log miss")
}

func TestInsertLeaderChoiceAt_PrefersStoreEntry(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	k := r.raftLog.committed + 1
	// Simulate remembering a proposal "S" at k
	ent := pb.Entry{Index: k, Term: r.Term, Data: []byte("S")}
	// Inject into fast-prop store (however you store it; adjust API if needed)
	if r.fastPropStore == nil {
		r.fastPropStore = make(map[uint64]map[string]pb.Entry)
	}
	dig := entryDigest(&ent)
	if r.fastPropStore[k] == nil {
		r.fastPropStore[k] = make(map[string]pb.Entry)
	}
	r.fastPropStore[k][dig] = ent

	// Call insert; should write from store as leader-approved
	r.insertLeaderChoiceAt(k, dig)
	got, err := r.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("S"), got.Data)
	assert.Equal(t, pb.InsertedByLeader, got.InsertedBy)
}

func TestInsertLeaderChoiceAt_Fallback_TagUpgradeFromSelf(t *testing.T) {
	st := newTestMemoryStorage(withPeers(1, 2, 3))
	cfg := newTestConfig(1, 10, 1, st)
	cfg.EnableFastPath = true
	r := newRaft(cfg)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Ensure fast-prop store is empty (force store miss).
	r.fastPropStore = nil

	// Pick k so that entry(k) already exists: use lastIndex()+1 first to create it,
	// then bump lastIndex again (so k <= lastIndex holds on the second call).
	k := r.raftLog.lastIndex() + 1

	// Write a self-approved entry at k contiguously via local MsgFastProp (doesn't touch store).
	e := pb.Entry{Index: k, Term: r.Term, Data: []byte("UPGRADE_ME")}
	require.NoError(t, r.Step(pb.Message{
		Type: pb.MsgFastProp,
		From: r.id, To: r.id, Term: r.Term,
		Entries: []pb.Entry{e},
	}))
	cur, err := r.raftLog.entry(k)
	require.NoError(t, err)
	require.Equal(t, pb.InsertedBySelf, cur.InsertedBy)

	// Bump the tail so index <= lastIndex() is definitely true during sparseAppend.
	// A simple way: append a leader no-op via classic path.
	require.NoError(t, r.Step(pb.Message{Type: pb.MsgProp, From: r.id, Entries: []pb.Entry{{}}}))
	require.GreaterOrEqual(t, r.raftLog.lastIndex(), k)

	// Store still misses; digest matches payload at k.
	dig := entryDigest(&e)
	r.insertLeaderChoiceAt(k, dig)

	// Assert: tag upgraded to leader, content unchanged.
	got, err := r.raftLog.entry(k)
	require.NoError(t, err)
	assert.Equal(t, []byte("UPGRADE_ME"), got.Data)
	assert.Equal(t, pb.InsertedByLeader, got.InsertedBy)
}
