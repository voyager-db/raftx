// fast_raft_e2e_test.go
package raft

import (
	"bytes"
	"testing"
	"time"

	pb "github.com/voyager-db/raftx/raftpb"
)

// build an N-node network with the given configFunc applied to each node's Config
func newFastNetworkN(n int, configFunc func(*Config)) *network {
	peers := make([]stateMachine, n) // nil => newNetworkWithConfig will instantiate raft nodes
	return newNetworkWithConfig(configFunc, peers...)
}

func leaderOf(nw *network) uint64 {
	for id, p := range nw.peers {
		if r, ok := p.(*raft); ok && r.state == StateLeader {
			return id
		}
	}
	return 0
}

func enableFastCommitOnAll(nw *network, v bool) {
	for _, p := range nw.peers {
		if r, ok := p.(*raft); ok {
			r.enableFastCommit = v
		}
	}
}

func broadcastFastProp(nw *network, idx uint64, cid string, payload []byte, only ...uint64) {
	e := pb.Entry{
		Type:      pb.EntryNormal,
		ContentId: []byte(cid),
		Data:      payload,
	}
	targets := only
	if len(targets) == 0 {
		targets = idsBySize(len(nw.peers))
	}
	for _, to := range targets {
		nw.send(pb.Message{
			From:    0, // client (ignored)
			To:      to,
			Type:    pb.MsgFastProp,
			Index:   idx,
			Entries: []pb.Entry{e},
		})
	}
}

// Wait for a leader; once found, force a couple of heartbeats from the leader
// so all followers learn r.lead before we send fast proposals.
func waitLeader(t *testing.T, nw *network) uint64 {
	t.Helper()
	var lid uint64
	for i := 0; i < 200; i++ {
		if lid = leaderOf(nw); lid != 0 {
			break
		}
		nw.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	}
	if lid == 0 {
		t.Fatalf("no leader elected")
	}
	// Publish leadership: two beats to be safe.
	nw.send(pb.Message{From: lid, To: lid, Type: pb.MsgBeat})
	nw.send(pb.Message{From: lid, To: lid, Type: pb.MsgBeat})
	return lid
}

func waitCommittedOnAll(t *testing.T, nw *network, idx uint64) {
	t.Helper()
	for i := 0; i < 2000; i++ { // ~2s worst-case
		ok := true
		for _, id := range idsBySize(len(nw.peers)) {
			r := nw.peers[id].(*raft)
			if r.raftLog.committed < idx {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		// nudge the network forward
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("commit did not reach %d on all peers", idx)
}

func waitCommittedOnSet(t *testing.T, nw *network, ids []uint64, idx uint64) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		ok := true
		for _, id := range ids {
			r := nw.peers[id].(*raft)
			if r.raftLog.committed < idx {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("commit did not reach %d on peers %v", idx, ids)
}

func entryCIDAt(t *testing.T, r *raft, idx uint64) []byte {
	t.Helper()
	ents, err := r.raftLog.slice(idx, idx+1, noLimit)
	if err != nil || len(ents) != 1 {
		t.Fatalf("slice(%d): err=%v len=%d", idx, err, len(ents))
	}
	return ents[0].ContentId
}

// Step the isolated leader down quickly (no timing needed).
func forceStepDown(nw *network, lid uint64) {
	// First call marks all followers inactive; second call sees no quorum and steps down.
	nw.send(pb.Message{From: lid, To: lid, Type: pb.MsgCheckQuorum})
	nw.send(pb.Message{From: lid, To: lid, Type: pb.MsgCheckQuorum})
}

// Expire follower leases so they won't reject {Pre,}Vote under CheckQuorum lease.
func expireLeasesExcept(nw *network, except uint64) {
	for _, id := range idsBySize(len(nw.peers)) {
		if id == except {
			continue
		}
		if r, ok := nw.peers[id].(*raft); ok {
			r.electionElapsed = r.electionTimeout
		}
	}
}

// Wait for a leader that is NOT `exclude`.
func waitLeaderExcept(t *testing.T, nw *network, exclude uint64) uint64 {
	t.Helper()
	for i := 0; i < 300; i++ {
		for id, p := range nw.peers {
			if id == exclude {
				continue
			}
			if r, ok := p.(*raft); ok && r.state == StateLeader {
				// Publish leadership to others
				nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
				nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
				return id
			}
		}
		// nudge: try to start an election on someone other than exclude
		for _, id := range idsBySize(len(nw.peers)) {
			if id != exclude {
				nw.send(pb.Message{From: id, To: id, Type: pb.MsgHup})
				break
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("no new leader elected (excluding %d)", exclude)
	return 0
}

// --- E2E tests -------------------------------------------------------------

// 1) Bootstrap → 1-RTT fast commit via MsgFastProp, then replicate everywhere.
func TestE2E_FastPath_BootstrapAndFastCommit(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	k := ldr.raftLog.committed + 1
	broadcastFastProp(nw, k, "cid-fast-1", []byte("v1"))

	waitCommittedOnAll(t, nw, k)

	for _, id := range idsBySize(len(nw.peers)) {
		got := entryCIDAt(t, nw.peers[id].(*raft), k)
		if !bytes.Equal(got, []byte("cid-fast-1")) {
			t.Fatalf("peer %d: cid at %d=%q, want %q", id, k, string(got), "cid-fast-1")
		}
	}

	k2 := k + 1
	broadcastFastProp(nw, k2, "cid-fast-2", []byte("v2"))
	waitCommittedOnAll(t, nw, k2)
	for _, id := range idsBySize(len(nw.peers)) {
		got := entryCIDAt(t, nw.peers[id].(*raft), k2)
		if !bytes.Equal(got, []byte("cid-fast-2")) {
			t.Fatalf("peer %d: cid at %d=%q, want %q", id, k2, string(got), "cid-fast-2")
		}
	}
}

// 2) Leader crashes after proposals → new leader seeds from SelfApproved and re-commits same value.
func TestE2E_FastPath_LeaderCrash_RecommitSameValue(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	lid := waitLeader(t, nw)
	k := nw.peers[lid].(*raft).raftLog.committed + 1

	// Majority of nodes self-approve the same content.
	const cid = "cid-crash-recover"
	broadcastFastProp(nw, k, cid, []byte("recover"))

	// Crash/isolate the current leader.
	nw.isolate(lid)

	forceStepDown(nw, lid)
	expireLeasesExcept(nw, lid)

	// Force a new election on a different node.
	var nid uint64
	for _, id := range idsBySize(len(nw.peers)) {
		if id != lid {
			nid = id
			break
		}
	}
	nw.send(pb.Message{From: nid, To: nid, Type: pb.MsgHup})
	newLid := waitLeader(t, nw)
	if newLid == lid {
		t.Fatalf("expected a different leader; still %d", lid)
	}

	// Surviving majority should commit k.
	var survivors []uint64
	for _, id := range idsBySize(len(nw.peers)) {
		if id != lid {
			survivors = append(survivors, id)
		}
	}
	waitCommittedOnSet(t, nw, survivors, k)

	for _, id := range survivors {
		got := entryCIDAt(t, nw.peers[id].(*raft), k)
		if !bytes.Equal(got, []byte(cid)) {
			t.Fatalf("peer %d: cid at %d=%q, want %q", id, k, string(got), cid)
		}
	}

	// Heal and ensure old leader catches up.
	nw.recover()
	waitCommittedOnAll(t, nw, k)
	for _, id := range idsBySize(len(nw.peers)) {
		got := entryCIDAt(t, nw.peers[id].(*raft), k)
		if !bytes.Equal(got, []byte(cid)) {
			t.Fatalf("after heal, peer %d: cid at %d=%q, want %q", id, k, string(got), cid)
		}
	}
}

// 3) Fast path on but fast commit disabled → decision chosen and replicated classically.
func TestE2E_FastPath_DisabledFastCommit_FallsBackToClassic(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, false) // classic-only commits

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	k := ldr.raftLog.committed + 1
	broadcastFastProp(nw, k, "cid-classic", []byte("classic"))

	waitCommittedOnAll(t, nw, k)

	for _, id := range idsBySize(len(nw.peers)) {
		got := entryCIDAt(t, nw.peers[id].(*raft), k)
		if !bytes.Equal(got, []byte("cid-classic")) {
			t.Fatalf("peer %d: cid at %d=%q, want %q", id, k, string(got), "cid-classic")
		}
	}
}

// --- more E2E tests --------------------------------------------------------

// 4) Contention across a window of indices: ensure one decision per k in order.
func TestE2E_FastPath_Contention_Window4(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	start := ldr.raftLog.committed + 1 // k
	// For window [k..k+3], blast multiple competing contentIds for each index.
	for i := uint64(0); i < 4; i++ {
		k := start + i
		// two contenders per index; send to all so majority self-approve
		broadcastFastProp(nw, k, "cid-X-"+string('A'+byte(i)), []byte("vx"))
		broadcastFastProp(nw, k, "cid-Y-"+string('A'+byte(i)), []byte("vy"))
	}

	// Wait until the window fully commits.
	waitCommittedOnAll(t, nw, start+3)

	// Verify: exactly one commit per index and commits are in order.
	lastCID := make(map[string]struct{})
	for k := start; k <= start+3; k++ {
		var cid []byte
		for _, id := range idsBySize(len(nw.peers)) {
			got := entryCIDAt(t, nw.peers[id].(*raft), k)
			if cid == nil {
				cid = got
			} else if !bytes.Equal(cid, got) {
				t.Fatalf("index %d: peer %d has cid=%q, mismatch with %q", k, id, string(got), string(cid))
			}
		}
		key := string(cid)
		if _, dup := lastCID[key]; dup {
			t.Fatalf("duplicate contentId %q chosen at multiple indices in window", key)
		}
		lastCID[key] = struct{}{}
	}
}

// 5) Mixed-binary cluster: two nodes run classic-only; fast path still commits safely.
func TestE2E_FastPath_MixedBinary_TwoClassicOnly(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true // default for most
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	// Disable fast path/commit on two peers to simulate mixed cluster.
	for _, id := range []uint64{4, 5} {
		if r, ok := nw.peers[id].(*raft); ok {
			r.enableFastPath = false
			r.enableFastCommit = false
		}
	}

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	k := ldr.raftLog.committed + 1
	broadcastFastProp(nw, k, "cid-mixed", []byte("vm"))

	// Even if fast-commit is impossible (depends on quorum math),
	// the leader should still decide and replicate classically.
	waitCommittedOnAll(t, nw, k)

	for _, id := range idsBySize(len(nw.peers)) {
		got := entryCIDAt(t, nw.peers[id].(*raft), k)
		if !bytes.Equal(got, []byte("cid-mixed")) {
			t.Fatalf("peer %d: cid at %d=%q, want %q", id, k, string(got), "cid-mixed")
		}
	}
}

// 6) ReadIndex linearizability: after a fast commit, reads fence at >= committed index.
func TestE2E_FastPath_ReadIndexAfterFastCommit(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	// Commit one index in the current term (fast-commit).
	k := ldr.raftLog.committed + 1
	broadcastFastProp(nw, k, "cid-read", []byte("vr"))
	waitCommittedOnAll(t, nw, k)

	// Choose a follower to issue ReadIndex from.
	var fid uint64
	for _, id := range idsBySize(len(nw.peers)) {
		if id != lid {
			fid = id
			break
		}
	}

	// Correlate via context token.
	ctx := []byte("readctx-1")
	seen := make(chan uint64, 1)
	origHook := nw.msgHook
	nw.msgHook = func(m pb.Message) bool {
		if m.Type == pb.MsgReadIndexResp && len(m.Entries) == 1 && bytes.Equal(m.Entries[0].Data, ctx) {
			seen <- m.Index
		}
		return true
	}
	defer func() { nw.msgHook = origHook }()

	// Issue ReadIndex from follower; it forwards to leader, which responds once safe.
	nw.send(pb.Message{
		From: fid, To: fid, Type: pb.MsgReadIndex,
		Entries: []pb.Entry{{Data: ctx}},
	})

	// Nudge the net a bit and wait for response.
	var idx uint64
	select {
	case idx = <-seen:
	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive ReadIndexResp")
	}
	if idx < k {
		t.Fatalf("ReadIndexResp=%d, want >= %d", idx, k)
	}
}

// 7) Slow follower: isolate a node, commit 3 indices, heal, and ensure it catches up.
func TestE2E_FastPath_SlowFollower_CatchUp(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	// Pick the last id as the slow follower.
	slow := uint64(5)
	if slow == lid {
		slow = 4
	}
	// Isolate the slow node.
	nw.isolate(slow)

	start := ldr.raftLog.committed + 1
	for i := 0; i < 3; i++ {
		k := start + uint64(i)
		broadcastFastProp(nw, k, "cid-slow-"+string('A'+byte(i)), []byte("vs"))
		waitCommittedOnSet(t, nw, []uint64{1, 2, 3, 4}, k) // survivors
	}

	// Slow follower should not have advanced.
	if r := nw.peers[slow].(*raft); r.raftLog.committed >= start {
		t.Fatalf("slow follower %d unexpectedly advanced commit to %d (start=%d)", slow, r.raftLog.committed, start)
	}

	// Heal and ensure it catches up fully.
	nw.recover()
	waitCommittedOnAll(t, nw, start+2)
}

// 8) Light chaos: drop/reorder traffic while committing a sequence; no conflicts.
func TestE2E_FastPath_Chaos_NoConflictingCommits(t *testing.T) {
	nw := newFastNetworkN(5, func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	})
	enableFastCommitOnAll(nw, true)

	// Add symmetric 20% drop on all pairs (light chaos).
	for _, a := range idsBySize(len(nw.peers)) {
		for _, b := range idsBySize(len(nw.peers)) {
			if a == b {
				continue
			}
			nw.drop(a, b, 0.2)
		}
	}

	lid := waitLeader(t, nw)
	ldr := nw.peers[lid].(*raft)

	start := ldr.raftLog.committed + 1
	N := 8
	for i := 0; i < N; i++ {
		k := start + uint64(i)
		broadcastFastProp(nw, k, "cid-chaos-"+string('A'+byte(i)), []byte("vc"))
		// give the network some time to make progress
		for j := 0; j < 200; j++ {
			for _, id := range idsBySize(len(nw.peers)) {
				nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
			}
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Remove drops and converge.
	nw.recover()
	waitCommittedOnAll(t, nw, start+uint64(N-1))

	// Verify: for every committed index, all peers agree on the same contentId.
	for k := start; k < start+uint64(N); k++ {
		var cid []byte
		for _, id := range idsBySize(len(nw.peers)) {
			got := entryCIDAt(t, nw.peers[id].(*raft), k)
			if cid == nil {
				cid = got
			} else if !bytes.Equal(cid, got) {
				t.Fatalf("conflicting commits at index %d: %q vs %q", k, string(cid), string(got))
			}
		}
	}
}

// applyCommittedConfs scans committed entries on each *raft* peer,
// applies ConfChangeV2 entries on peers that already have a non-empty
// voter set (i.e., actual members), and advances the raft applied index.
// It keeps per-peer cursors in 'applied' to avoid re-applying.
func applyCommittedConfs(t *testing.T, nw *network, applied map[uint64]uint64) {
	t.Helper()
	for id, p := range nw.peers {
		r, ok := p.(*raft)
		if !ok { // skip blackHole or other non-raft steppers
			continue
		}
		last := applied[id]
		hi := r.raftLog.committed
		if hi <= last {
			continue
		}
		for idx := last + 1; idx <= hi; idx++ {
			ents, err := r.raftLog.slice(idx, idx+1, noLimit)
			if err != nil || len(ents) != 1 {
				break
			}
			e := ents[0]

			if e.Type == pb.EntryConfChangeV2 {
				var cc pb.ConfChangeV2
				if err := cc.Unmarshal(e.Data); err != nil {
					t.Fatalf("peer %d: unmarshal ConfChangeV2 at %d: %v", id, idx, err)
				}
				// ✅ Apply only if this peer already has any voters (is a real member).
				hasVoters := len(r.trk.Config.Voters[0]) > 0 || len(r.trk.Config.Voters[1]) > 0
				if hasVoters {
					_ = r.applyConfChange(cc)
				}
				// Else: skip applying on this node (e.g., brand-new peer 4); it will learn
				// the config via replication/snapshot; we still advance 'applied' below.
			}

			// Always advance applied so pendingConfIndex clears and state stays in sync.
			r.appliedTo(idx, entsSize(ents))
		}
		applied[id] = hi
	}
}

// raftIDs returns IDs of peers that are actual *raft* nodes.
func raftIDs(nw *network) []uint64 {
	var ids []uint64
	for id, p := range nw.peers {
		if _, ok := p.(*raft); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// Wait until all *raft* peers report committed >= idx (skip blackHole peers).
func waitCommittedOnAllRaft(t *testing.T, nw *network, idx uint64) {
	t.Helper()
	ids := raftIDs(nw)
	for i := 0; i < 2000; i++ {
		ok := true
		for _, id := range ids {
			r := nw.peers[id].(*raft)
			if r.raftLog.committed < idx {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("commit did not reach %d on raft peers %v", idx, ids)
}

func proposeAddVoter(nw *network, lid, node uint64) {
	cc := pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddNode,
			NodeID: node,
		}},
	}
	data, _ := cc.Marshal()
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}},
	})
}

func proposeData(nw *network, lid uint64, payload []byte) {
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryNormal, Data: payload}},
	})
}

// --- the test ---------------------------------------------------------------

func TestE2E_Reconfig_OldConfigQuorumUntilApply_ThenNewConfig(t *testing.T) {
	// Start with a 3-voter cluster (1..3) — NO peer 4 yet.
	nw := newNetworkWithConfig(func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true   // harmless for this test
		c.FastOpenIndexWindow = 4 // harmless
	}, nil, nil, nil) // exactly three raft nodes

	// Now add a 4th peer as a black hole (not in the initial ConfState).
	nw.peers[4] = &blackHole{}

	// Elect leader among 1..3 and publish leadership.
	lid := waitLeader(t, nw)

	// Isolate node 3. Reachable voters: {1,2}. Peer 4 remains black-holed.
	nw.isolate(3)

	// Propose AddNode(4) as a voter via ConfChangeV2.
	startCommit := nw.peers[lid].(*raft).raftLog.committed
	proposeAddVoter(nw, lid, 4)

	// Drive the network and apply committed ConfChangeV2 using the shim.
	applied := map[uint64]uint64{}
	var appliedOnLeader bool
	for i := 0; i < 2000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		applyCommittedConfs(t, nw, applied)
		// Has the leader applied the change?
		if r := nw.peers[lid].(*raft); !appliedOnLeader {
			if _, ok := r.trk.Config.Voters.IDs()[4]; ok {
				appliedOnLeader = true
				break
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !appliedOnLeader {
		t.Fatalf("leader did not apply ConfChangeV2(AddNode 4); quorum likely not using old config")
	}

	// Sanity: commit index advanced for the config entry.
	if nw.peers[lid].(*raft).raftLog.committed <= startCommit {
		t.Fatalf("commit index did not advance for conf change (commit=%d start=%d)",
			nw.peers[lid].(*raft).raftLog.committed, startCommit)
	}

	// New config active: voters {1,2,3,4}. With 3 still isolated & 4 black-holed,
	// only {1,2} reachable => 2/4 (not a majority). A normal entry MUST NOT commit.
	preCommit := nw.peers[lid].(*raft).raftLog.committed
	proposeData(nw, lid, []byte("under-new-config"))
	for i := 0; i < 500; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	if nw.peers[lid].(*raft).raftLog.committed > preCommit {
		t.Fatalf("entry committed under new config with only 2/4 reachable; expected to stall")
	}

	// Heal node 3 → {1,2,3} reachable => 3/4 majority; entry should commit now.
	nw.recover()
	waitCommittedOnAllRaft(t, nw, preCommit+1)
}

// --- reconfig: learner -> voter (joint explicit) ----------------------------------

// addRaftPeer creates a new raft node and seeds its storage with the current
// cluster ConfState so conf changes can be applied locally without panics.
func addRaftPeer(nw *network, id uint64) {
	// Extract current committed voter set from any existing raft node.
	var voters []uint64
	for _, p := range nw.peers {
		if r, ok := p.(*raft); ok {
			for vid := range r.trk.Config.Voters.IDs() {
				voters = append(voters, vid)
			}
			break
		}
	}
	ms := newTestMemoryStorage()
	// Seed a snapshot so InitialState() returns a non-empty ConfState.
	ms.snapshot.Metadata.ConfState.Voters = voters
	ms.snapshot.Metadata.Index = 1
	ms.snapshot.Metadata.Term = 1

	r := newTestRaft(id, 10, 1, ms)
	r.checkQuorum = true
	r.preVote = true
	nw.peers[id] = r
	nw.storage[id] = ms
}

func proposeAddLearner(nw *network, lid, node uint64) {
	cc := pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddLearnerNode,
			NodeID: node,
		}},
	}
	data, _ := cc.Marshal()
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}},
	})
}

func proposePromoteLearnerJointExplicit(nw *network, lid, node uint64) {
	cc := pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointExplicit,
		Changes: []pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddNode,
			NodeID: node,
		}},
	}
	data, _ := cc.Marshal()
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}},
	})
}

func proposeLeaveJoint(nw *network, lid uint64) {
	// Explicit leave: empty ConfChangeV2.
	cc := pb.ConfChangeV2{}
	data, _ := cc.Marshal()
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}},
	})
}

// --- config inspection helpers ---
func isJoint(r *raft) bool { return len(r.trk.Config.Voters[1]) > 0 }

func voterInIncoming(r *raft, id uint64) bool {
	_, ok := r.trk.Config.Voters[0][id]
	return ok
}
func voterInOutgoing(r *raft, id uint64) bool {
	_, ok := r.trk.Config.Voters[1][id]
	return ok
}
func voterAnywhere(r *raft, id uint64) bool {
	return voterInIncoming(r, id) || voterInOutgoing(r, id)
}
func learnerHas(r *raft, id uint64) bool {
	_, ok := r.trk.Learners[id]
	return ok
}

// Learner->Voter promotion via JointExplicit. Propose while joint; verify joint quorums.
func TestE2E_Reconfig_LearnerPromotion_JointExplicit(t *testing.T) {
	// Start with 3 voters (1..3).
	nw := newNetworkWithConfig(func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	}, nil, nil, nil)

	// Add a 4th raft node (not in the config yet).
	addRaftPeer(nw, 4)

	lid := waitLeader(t, nw)

	{
		r := nw.peers[lid].(*raft)
		if voterAnywhere(r, 4) || learnerHas(r, 4) {
			t.Fatalf("peer 4 unexpectedly present before AddLearner; voters=%v,%v learners=%v",
				r.trk.Config.Voters[0], r.trk.Config.Voters[1], r.trk.Learners)
		}
	}

	// 1) Add 4 as learner; should commit using old 3-node majority.
	proposeAddLearner(nw, lid, 4)
	applied := map[uint64]uint64{}
	var learnerOnLeader bool
	for i := 0; i < 2000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		applyCommittedConfs(t, nw, applied)
		if r := nw.peers[lid].(*raft); !learnerOnLeader {
			if _, ok := r.trk.Learners[4]; ok {
				learnerOnLeader = true
				break
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !learnerOnLeader {
		t.Fatalf("leader did not apply AddLearner(4)")
	}

	{
		r := nw.peers[lid].(*raft)
		if !learnerHas(r, 4) {
			t.Fatalf("expected 4 to be a learner after AddLearner; learners=%v", r.trk.Learners)
		}
		if voterAnywhere(r, 4) {
			t.Fatalf("did not expect 4 to be a voter after AddLearner; voters(in,out)=%v,%v",
				r.trk.Config.Voters[0], r.trk.Config.Voters[1])
		}
	}

	// 2) Promote to voter via JointExplicit (stay joint).
	proposePromoteLearnerJointExplicit(nw, lid, 4)
	var jointActive bool
	for i := 0; i < 2000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		applyCommittedConfs(t, nw, applied)
		if r := nw.peers[lid].(*raft); !jointActive {
			// Joint iff outgoing voters set is non-empty.
			if len(r.trk.Config.Voters[1]) > 0 {
				jointActive = true
				break
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !jointActive {
		t.Fatalf("JointExplicit promotion did not activate joint config")
	}

	// While joint, joint quorums require maj(old={1,2,3}) AND maj(new={1,2,3,4}).
	// Cut 3 and 4 so only {1,2} reachable: old maj satisfied (2/3), new maj NOT (2/4).
	nw.isolate(3)
	nw.isolate(4)
	preCommit := nw.peers[lid].(*raft).raftLog.committed
	proposeData(nw, lid, []byte("during-joint"))
	for i := 0; i < 500; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	if nw.peers[lid].(*raft).raftLog.committed > preCommit {
		t.Fatalf("data committed during joint with only 2/4 (should stall)")
	}

	{
		r := nw.peers[lid].(*raft)
		if !isJoint(r) {
			t.Fatalf("expected joint config active; voters(in,out)=%v,%v",
				r.trk.Config.Voters[0], r.trk.Config.Voters[1])
		}
		if !voterInIncoming(r, 4) {
			t.Fatalf("expected 4 in incoming voters during joint; incoming=%v", r.trk.Config.Voters[0])
		}
		if voterInOutgoing(r, 4) {
			t.Fatalf("did not expect 4 in outgoing voters during joint; outgoing=%v", r.trk.Config.Voters[1])
		}
		if learnerHas(r, 4) {
			t.Fatalf("did not expect 4 to remain a learner during joint; learners=%v", r.trk.Learners)
		}
	}

	// Heal 3 (now {1,2,3} reachable): new maj 3/4 satisfied, old maj 2/3 satisfied ⇒ commit proceeds.
	nw.recover()  // clear all drops
	nw.isolate(4) // keep 4 down to prove it isn't needed
	waitCommittedOnSet(t, nw, []uint64{1, 2, 3}, preCommit+1)

	nw.recover()
	waitCommittedOnAllRaft(t, nw, preCommit+1)

	// 3) Leave joint explicitly; final voters {1,2,3,4}.
	proposeLeaveJoint(nw, lid)
	for i := 0; i < 2000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		applyCommittedConfs(t, nw, applied)
		if r := nw.peers[lid].(*raft); len(r.trk.Config.Voters[1]) == 0 {
			break // left joint
		}
		time.Sleep(1 * time.Millisecond)
	}

	{
		r := nw.peers[lid].(*raft)
		if isJoint(r) {
			t.Fatalf("expected to have left joint; voters(in,out)=%v,%v",
				r.trk.Config.Voters[0], r.trk.Config.Voters[1])
		}
		if !voterInIncoming(r, 4) {
			t.Fatalf("expected 4 to be a voter in final config; incoming=%v", r.trk.Config.Voters[0])
		}
		if learnerHas(r, 4) {
			t.Fatalf("did not expect 4 to remain a learner after leave; learners=%v", r.trk.Learners)
		}
	}

	// Under final config (1..4), with 4 isolated: 1,2,3 is 3/4 ⇒ commits; with only 1,2 it would stall.
	pre2 := nw.peers[lid].(*raft).raftLog.committed
	proposeData(nw, lid, []byte("after-leave"))
	// ✅ majority of final config with 4 isolated is {1,2,3}
	waitCommittedOnSet(t, nw, []uint64{1, 2, 3}, pre2+1)

	// (optional) then heal and verify everyone catches up
	nw.recover()
	waitCommittedOnAllRaft(t, nw, pre2+1)
}

// --- reconfig: remove node under contention ---------------------------------

func proposeRemoveVoter(nw *network, lid, node uint64) {
	cc := pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{{
			Type:   pb.ConfChangeRemoveNode,
			NodeID: node,
		}},
	}
	data, _ := cc.Marshal()
	nw.send(pb.Message{
		From:    lid,
		To:      lid,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}},
	})
}

func TestE2E_Reconfig_RemoveNode_Contention_QuorumsSwitchAfterApply(t *testing.T) {
	// Start with 4 voters (1..4).
	nw := newNetworkWithConfig(func(c *Config) {
		c.PreVote = true
		c.CheckQuorum = true
		c.EnableFastPath = true
		c.FastOpenIndexWindow = 4
	}, nil, nil, nil, nil)

	lid := waitLeader(t, nw)

	// Cut 3 and 4 so only {1,2} reachable: 2/4 (not a majority).
	nw.isolate(3)
	nw.isolate(4)

	// Propose RemoveNode(4) — should **stall** under old config (needs 3/4).
	startCommit := nw.peers[lid].(*raft).raftLog.committed
	proposeRemoveVoter(nw, lid, 4)
	for i := 0; i < 500; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		time.Sleep(1 * time.Millisecond)
	}
	if nw.peers[lid].(*raft).raftLog.committed > startCommit {
		t.Fatalf("remove(4) committed with only 2/4 reachable; should stall")
	}

	// Heal 3: now {1,2,3} reachable ⇒ 3/4 majority; remove should commit+apply.
	nw.recover()
	applied := map[uint64]uint64{}
	var removedOnLeader bool
	for i := 0; i < 2000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		applyCommittedConfs(t, nw, applied)
		if r := nw.peers[lid].(*raft); !removedOnLeader {
			if _, ok := r.trk.Config.Voters.IDs()[4]; !ok {
				removedOnLeader = true
				break
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !removedOnLeader {
		t.Fatalf("remove(4) did not apply after healing 3")
	}

	// Now final config is {1,2,3}. Cut 3 again; only {1,2} reachable ⇒ 2/3 (majority) — data should commit.
	nw.isolate(3)
	preCommit := nw.peers[lid].(*raft).raftLog.committed
	proposeData(nw, lid, []byte("after-remove"))
	for i := 0; i < 1000; i++ {
		for _, id := range idsBySize(len(nw.peers)) {
			nw.send(pb.Message{From: id, To: id, Type: pb.MsgBeat})
		}
		if nw.peers[lid].(*raft).raftLog.committed > preCommit {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if nw.peers[lid].(*raft).raftLog.committed == preCommit {
		t.Fatalf("data did not commit with 2/3 after remove(4) applied")
	}
}

func TestLeaderFallbackRunsWhenKHasUncommittedLeaderNoop(t *testing.T) {
	// Build a single-node leader with fast path on.
	cfg := baseConfigFast(1) // or baseConfigFast if you have it
	cfg.EnableFastPath = true
	rl := newRaft(cfg)
	primeSingleVoter(rl, rl.id)

	rl.becomeCandidate()
	rl.becomeLeader()

	// Build a committed prefix: committed = 5
	mustAppendCommitted(rl, 5)

	// Now append a leader-approved noop at k = committed+1 (index 6).
	// This simulates the upstream leader's noop at term start.
	if !rl.appendEntry(pb.Entry{Data: nil}) {
		t.Fatalf("append noop failed")
	}
	noopIdx := rl.raftLog.lastIndex() // should be 6
	if noopIdx != rl.raftLog.committed+1 {
		t.Fatalf("noop not at k; noopIdx=%d committed=%d", noopIdx, rl.raftLog.committed)
	}
	// Sanity: hasLeaderApprovedAt(k) should be true for the noop.
	if !rl.hasLeaderApprovedAt(noopIdx) {
		t.Fatalf("expected leader-approved noop at k=%d", noopIdx)
	}

	// Pre-cache the leader payload at leader's current k so the decision
	// will use *our* bytes (important for etcd waiter).
	rl.CacheLeaderFastPayload([]byte("leader-payload"), []byte("cid-x"))

	// Record state before attempting fallback.
	lastIdxBefore := rl.raftLog.lastIndex()

	// Send the leader's own fast-prop (From=None → normalized to leader).
	msg := pb.Message{
		Type:  pb.MsgFastProp,
		From:  None, // local
		Index: 0,    // ignored by leader
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("leader-payload"),
			ContentId: []byte("cid-x"),
		}},
	}
	if err := rl.Step(msg); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	// After the fix:
	// - fallback should *run*, i.e. append the decision (either at k, replacing noop,
	//   or at k+1 if your fallback still uses appendEntry), so lastIndex must increase by 1.
	// - proposalCache[k] should be cleared.
	// - fastMatchIndex[leader] should count k.
	lastIdxAfter := rl.raftLog.lastIndex()
	if lastIdxAfter != lastIdxBefore+1 {
		t.Fatalf("fallback did not append decision; lastIdx before=%d after=%d", lastIdxBefore, lastIdxAfter)
	}

	// proposalCache for k should be cleared after install.
	if rl.proposalCache[noopIdx] != nil {
		t.Fatalf("proposalCache[%d] not cleared", noopIdx)
	}

	// Leader should count itself at k toward fast quorum.
	if rl.fastMatchIndex[rl.id] < noopIdx {
		t.Fatalf("fastMatchIndex[self]=%d want >= %d", rl.fastMatchIndex[rl.id], noopIdx)
	}

	// Optional: if your fallback installs *exactly* at k (ideal), validate it replaced the noop.
	ents, _ := rl.raftLog.slice(noopIdx, noopIdx+1, noLimit)
	if len(ents) == 1 && len(ents[0].Data) != 0 {
		if string(ents[0].Data) != "leader-payload" || getOrigin(&ents[0]) != pb.EntryOriginLeader {
			t.Fatalf("entry at k not leader-approved payload: %+v", ents[0])
		}
	} else {
		// If you still append at k+1, ensure the leader payload is at lastIdxAfter.
		ents2, _ := rl.raftLog.slice(lastIdxAfter, lastIdxAfter+1, noLimit)
		if len(ents2) != 1 || string(ents2[0].Data) != "leader-payload" || getOrigin(&ents2[0]) != pb.EntryOriginLeader {
			t.Fatalf("leader decision not appended properly: %+v", ents2)
		}
	}
}
