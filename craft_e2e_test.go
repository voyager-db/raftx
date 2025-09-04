// e2e_craft_test.go
package raft

import (
	"fmt"
	"testing"

	"github.com/voyager-db/raftx/quorum"
	pb "github.com/voyager-db/raftx/raftpb"
	"github.com/voyager-db/raftx/tracker"
)

// ---------------------------
// E2E network & helpers (isolated)
// ---------------------------
type e2eDropFn func(m pb.Message) bool

type e2eNet struct {
	nodes  map[uint64]*raft
	dropFn e2eDropFn // optional
}

func newE2ENet(nodes map[uint64]*raft) *e2eNet  { return &e2eNet{nodes: nodes} }
func (n *e2eNet) withDrop(fn e2eDropFn) *e2eNet { n.dropFn = fn; return n }

func currentGlobalLeader(leaders map[uint64]*raft) *raft {
	for _, r := range leaders {
		if r.gState == gStateLeader {
			return r
		}
	}
	return nil
}

// pump delivers all queued messages between nodes until quiescence.
// It drops messages to unknown recipients (useful if membership grows).
func (n *e2eNet) pump(t *testing.T) {
	t.Helper()
	progress := true
	for progress {
		progress = false
		for _, rn := range n.nodes {
			all := append([]pb.Message{}, rn.msgs...)
			all = append(all, rn.msgsAfterAppend...)
			if len(all) == 0 {
				continue
			}
			rn.msgs, rn.msgsAfterAppend = nil, nil
			progress = true
			for _, m := range all {
				if m.To == 0 || m.To == rn.id {
					continue
				}
				if n.dropFn != nil && n.dropFn(m) {
					continue
				}
				dst := n.nodes[m.To]
				if dst == nil {
					continue
				}
				if err := dst.Step(m); err != nil {
					t.Fatalf("step to %x failed: %v", m.To, err)
				}
			}
		}
	}
}

// create a 5-node local cluster with unique IDs starting at baseID+1..+5.
// Uses MsgHup to elect a leader (network-based).
func e2eNewLocalCluster(t *testing.T, baseID uint64) (map[uint64]*raft, uint64) {
	t.Helper()
	ids := []uint64{baseID + 1, baseID + 2, baseID + 3, baseID + 4, baseID + 5}
	nodes := make(map[uint64]*raft, 5)
	for _, id := range ids {
		mem := NewMemoryStorage()
		// Important: non-zero Index/Term so ConfState restores and nodes are promotable.
		cs := pb.ConfState{Voters: ids}
		mem.ApplySnapshot(pb.Snapshot{
			Metadata: pb.SnapshotMetadata{
				ConfState: cs,
				Index:     1,
				Term:      1,
			},
		})
		cfg := &Config{
			ID:                  id,
			ElectionTick:        10,
			HeartbeatTick:       1,
			Storage:             mem,
			MaxSizePerMsg:       1 << 20,
			MaxInflightMsgs:     256,
			EnableFastPath:      true,
			EnableCRaft:         true, // CRaft on; only local leaders participate globally
			GlobalElectionTick:  10,
			GlobalHeartbeatTick: 1,
			Logger:              getLogger(),
		}
		nodes[id] = newRaft(cfg)
	}

	net := newE2ENet(nodes)

	// Elect leader via MsgHup from first node (pure network)
	first := ids[0]
	if err := nodes[first].Step(pb.Message{From: first, To: first, Type: pb.MsgHup}); err != nil {
		t.Fatalf("Hup failed: %v", err)
	}
	net.pump(t)

	// Find leader
	var leaderID uint64
	for _, id := range ids {
		if nodes[id].state == StateLeader {
			leaderID = id
			break
		}
	}
	if leaderID == 0 {
		t.Fatalf("no local leader elected in cluster base=%d", baseID)
	}
	return nodes, leaderID
}

// Seed global membership among the provided 5 cluster leaders (ids).
// Applies the same gtrk membership on each leader so they can reach global quorum.
func e2eInitGlobalMembershipOnLeaders(leaders map[uint64]*raft, leaderIDs []uint64) {
	for _, r := range leaders {
		r.gtrk.Config.Voters = quorum.JointConfig{make(quorum.MajorityConfig), nil}
		for _, id := range leaderIDs {
			// ✅ add the id to the GLOBAL voters set
			r.gtrk.Config.Voters[0][id] = struct{}{} // <-- restore this

			if r.gtrk.Progress[id] == nil {
				r.gtrk.Progress[id] = &tracker.Progress{
					Next:      r.glog.lastIndex() + 1,
					Inflights: tracker.NewInflights(r.gtrk.MaxInflight, r.gtrk.MaxInflightBytes),
				}
			}
			r.gtrk.Progress[id].RecentActive = true
		}
		if pr := r.gtrk.Progress[r.id]; pr != nil {
			pr.BecomeReplicate()
			pr.RecentActive = true
		}
	}
}

// Propose a specific set of entries as a single GlobalBatch and ensure the
// leader's local wrapper commits before broadcasting the fast-prop.
func e2eProposeGlobalBatchWithEntries(t *testing.T, net *e2eNet, glCluster map[uint64]*raft, ldr *raft, ents []pb.Entry) {
	t.Helper()
	// Append to local log + commit/apply so the batch has a real (First/LastIndex).
	for _, e := range ents {
		if !ldr.appendEntry(e) {
			t.Fatalf("append failed")
		}
	}
	ldr.raftLog.commitTo(ldr.raftLog.lastIndex())
	ldr.appliedTo(ldr.raftLog.committed, 0)

	batch := pb.GlobalBatch{
		Entries:    ents,
		FirstIndex: ldr.raftLog.committed - uint64(len(ents)) + 1,
		LastIndex:  ldr.raftLog.committed,
	}
	ldr.proposeGlobalStateEntry(batch)

	// Ensure the wrapper commits on the whole 5-node local cluster first.
	for _, n := range glCluster {
		forceLocalCommitAll(n)
	}
	// Deliver the broadcast of MsgGlobalFastProp to other cluster leaders.
	net.pump(t)
}

// propose k normal entries on the local leader and commit locally using the network.
func e2eProposeLocalAndCommit(t *testing.T, net *e2eNet, cluster map[uint64]*raft, leaderID uint64, k int) {
	t.Helper()
	ldr := cluster[leaderID]
	for i := 0; i < k; i++ {
		if err := ldr.Step(pb.Message{
			From: leaderID, To: leaderID, Type: pb.MsgProp,
			Entries: []pb.Entry{{Type: pb.EntryNormal, Data: []byte(fmt.Sprintf("m-%d", i))}},
		}); err != nil {
			t.Fatalf("local propose failed: %v", err)
		}
		net.pump(t)
	}
}

// create a global batch on the local leader (from the locally committed range) and broadcast via CRaft.
// Ensures the local wrapper actually commits on the 5-node local cluster first.
func e2eProposeGlobalBatch(t *testing.T, net *e2eNet, glCluster map[uint64]*raft, ldr *raft) {
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("batch")}
	if !ldr.appendEntry(ent) {
		t.Fatalf("append failed")
	}
	ldr.raftLog.commitTo(ldr.raftLog.lastIndex())
	ldr.appliedTo(ldr.raftLog.committed, 0)

	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent},
		FirstIndex: ldr.raftLog.committed,
		LastIndex:  ldr.raftLog.committed,
	}
	ldr.proposeGlobalStateEntry(batch)

	// Ensure the wrapper commits on the 5-node local cluster (gates the broadcast).
	for _, n := range glCluster {
		forceLocalCommitAll(n)
	}
	// Deliver the broadcast of MsgGlobalFastProp
	net.pump(t)
}

// wait until each non-global leader has queued a MsgGlobalFastVote (their local wrapper committed)
func waitGlobalVotesQueued(t *testing.T, leaders map[uint64]*raft, leaderIDs []uint64, glID uint64) {
	t.Helper()
	for _, id := range leaderIDs {
		if id == glID {
			continue
		}
		r := leaders[id]
		queued := false
		for _, m := range r.msgs {
			if m.Type == pb.MsgGlobalFastVote {
				queued = true
				break
			}
		}
		if !queued {
			t.Fatalf("leader %x did not queue MsgGlobalFastVote; wrapper likely not committed locally", id)
		}
	}
}

// wait until the global leader has materialized at least one global entry and advanced gCommit
func waitGlobalMaterialized(t *testing.T, gl *raft) {
	t.Helper()
	if gl.gLastIndex == 0 {
		t.Fatalf("global leader %x did not materialize global batch (gLastIndex=0)", gl.id)
	}
	if gl.gCommit == 0 {
		t.Fatalf("global leader %x has not advanced gCommit", gl.id)
	}
}

func currentGlobalIndexFromLeader(gl *raft) uint64 {
	var idx uint64
	for k := range gl.gPossibleBatches {
		if k > idx {
			idx = k
		}
	}
	return idx
}

// wait until each non-global leader has *received* a MsgGlobalFastProp,
// i.e. it has a pending wrapper ack (or already has a committed global-state entry).
func waitFastPropReceived(t *testing.T, leaders map[uint64]*raft, leaderIDs []uint64, glID uint64) {
	t.Helper()
	gi := currentGlobalIndexFromLeader(leaders[glID])
	for _, id := range leaderIDs {
		if id == glID {
			continue
		}
		r := leaders[id]
		if len(r.pendingGlobalAcks) == 0 &&
			!r.hasCommittedGlobalStateFor(gi) &&
			!hasQueuedGlobalFastVoteFor(r, gi) {
			t.Fatalf("leader %x did not receive global fast-prop (no pendingGlobalAcks, no committed wrapper, no queued vote)", id)
		}
	}
}

// Evidence that follower processed the global fast-prop:
// 1) pending wrapper ack exists, OR
// 2) committed wrapper for gi exists, OR
// 3) queued MsgGlobalFastVote for gi exists.
func hasQueuedGlobalFastVoteFor(r *raft, gi uint64) bool {
	all := append([]pb.Message{}, r.msgs...)
	all = append(all, r.msgsAfterAppend...)
	for _, m := range all {
		if m.Type == pb.MsgGlobalFastVote && m.GlobalIndex == gi {
			return true
		}
	}
	return false
}

func fastPropReceivedOn(r *raft, gi uint64) bool {
	if len(r.pendingGlobalAcks) > 0 { // any pending ack is sufficient signal
		return true
	}
	if r.hasCommittedGlobalStateFor(gi) {
		return true
	}
	if hasQueuedGlobalFastVoteFor(r, gi) {
		return true
	}
	return false
}

func waitFastPropReceivedSpin(t *testing.T, net *e2eNet, leaders map[uint64]*raft, leaderIDs []uint64, glID uint64, rounds int) {
	t.Helper()
	gi := currentGlobalIndexFromLeader(leaders[glID])
	for r := 0; r < rounds; r++ {
		ok := true
		for _, id := range leaderIDs {
			if id == glID {
				continue
			}
			if !fastPropReceivedOn(leaders[id], gi) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		net.pump(t) // allow delivery/processing
	}
	// final check to produce a useful error
	for _, id := range leaderIDs {
		if id == glID {
			continue
		}
		if !fastPropReceivedOn(leaders[id], gi) {
			t.Fatalf("leader %x did not receive global fast-prop (no pendingGlobalAcks, no committed wrapper, no queued vote) for gIdx=%d", id, gi)
		}
	}
}

func waitGlobalVotesQueuedSpin(t *testing.T, net *e2eNet, leaders map[uint64]*raft, leaderIDs []uint64, glID uint64, rounds int) {
	t.Helper()
	for r := 0; r < rounds; r++ {
		ok := true
		for _, id := range leaderIDs {
			if id == glID {
				continue
			}
			queued := false
			all := append([]pb.Message{}, leaders[id].msgs...)
			all = append(all, leaders[id].msgsAfterAppend...)
			for _, m := range all {
				if m.Type == pb.MsgGlobalFastVote {
					queued = true
					break
				}
			}
			if !queued {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		net.pump(t)
	}
	// final check
	for _, id := range leaderIDs {
		if id == glID {
			continue
		}
		found := false
		all := append([]pb.Message{}, leaders[id].msgs...)
		all = append(all, leaders[id].msgsAfterAppend...)
		for _, m := range all {
			if m.Type == pb.MsgGlobalFastVote {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("leader %x did not queue MsgGlobalFastVote; wrapper likely not committed locally", id)
		}
	}
}

func dumpAllGlobal(t *testing.T, leaders map[uint64]*raft) {
	for id, r := range leaders {
		t.Logf("====== GLOBAL DUMP id=%x ======\n%s", id, r.dbgGlobalProgress())
	}
}

// ---------------------------
// The E2E test
// ---------------------------

func TestCRaft_E2E_FiveClusters_FiveNodesEach(t *testing.T) {
	// Build 5 local clusters; elect local leaders
	var (
		allNodes  = map[uint64]*raft{}
		leaders   = map[uint64]*raft{}
		leaderIDs []uint64
		clusters  = make([]map[uint64]*raft, 0, 5)
	)
	var base uint64 = 100
	for c := 0; c < 5; c++ {
		cluster, lid := e2eNewLocalCluster(t, base)
		base += 100
		for id, n := range cluster {
			allNodes[id] = n
		}
		clusters = append(clusters, cluster)
		leaders[lid] = cluster[lid]
		leaderIDs = append(leaderIDs, lid)
	}

	net := newE2ENet(allNodes)

	// Seed global membership across the 5 local leaders
	e2eInitGlobalMembershipOnLeaders(leaders, leaderIDs)

	// Pick global leader and make it global leader (bootstrap)
	glID := leaderIDs[0]
	gl := leaders[glID]
	gl.gTerm++
	gl.becomeGlobalLeader()
	net.pump(t)

	// Find the global leader's local cluster
	var glCluster map[uint64]*raft
	for _, cl := range clusters {
		if _, ok := cl[glID]; ok {
			glCluster = cl
			break
		}
	}
	if glCluster == nil {
		t.Fatal("failed to locate the global leader's local cluster")
	}

	// Drive some local commits in the global leader's local cluster
	e2eProposeLocalAndCommit(t, net, glCluster, glID, 3)

	// Propose and broadcast a global batch (ensures local wrapper commits, then fast-prop broadcast)
	e2eProposeGlobalBatch(t, net, glCluster, gl)

	// Give the network a chance to deliver MsgGlobalFastProp to other leaders
	net.pump(t)

	// Ensure each non-global leader has actually received the fast-prop (has wrapper recorded)
	waitFastPropReceived(t, leaders, leaderIDs, glID)

	gi := currentGlobalIndexFromLeader(currentGlobalLeader(leaders))
	for id, r := range leaders {
		if r.gState == gStateLeader {
			t.Logf("[T] leader=%x", id)
		}
		r.dbgHasWrapper(gi)
	}

	// Ensure the other 4 leaders commit their wrappers (so they queue votes)
	for _, id := range leaderIDs {
		if id == glID {
			continue
		}
		forceLocalCommitAll(leaders[id])
	}

	// IMPORTANT: assert votes are *queued* before you flush the queues
	waitGlobalVotesQueued(t, leaders, leaderIDs, glID)

	// Deliver votes and drive decision (fast/fallback)
	net.pump(t)

	dumpAllGlobal(t, leaders)

	// Let leader materialize + broadcast appends/heartbeats
	net.pump(t)

	act := currentGlobalLeader(leaders)
	if act == nil {
		t.Fatalf("no global leader at assertion time")
	}
	// Assert global leader materialized and committed
	waitGlobalMaterialized(t, act)

	// Finally assert all five leaders converge on the same global commit
	var want uint64
	for i, id := range leaderIDs {
		r := leaders[id]
		if r.gLastIndex == 0 {
			t.Fatalf("leader %x did not materialize batch", id)
		}
		if r.gCommit == 0 {
			t.Fatalf("leader %x has gCommit=0", id)
		}
		if i == 0 {
			want = r.gCommit
		} else if r.gCommit != want {
			t.Fatalf("global commit mismatch: leader %x gCommit=%d want=%d", id, r.gCommit, want)
		}
	}
}

func TestCRaft_E2E_FallbackClassic_NoFastQuorum(t *testing.T) {
	// Build clusters/leaders (reuse your existing setup)
	var (
		allNodes  = map[uint64]*raft{}
		leaders   = map[uint64]*raft{}
		leaderIDs []uint64
		clusters  []map[uint64]*raft
	)
	var base uint64 = 2000
	for c := 0; c < 5; c++ {
		cl, lid := e2eNewLocalCluster(t, base)
		base += 100
		for id, n := range cl {
			allNodes[id] = n
		}
		clusters = append(clusters, cl)
		leaders[lid] = cl[lid]
		leaderIDs = append(leaderIDs, lid)
	}

	// Drop one fast vote to keep us at classic quorum=3 but < fast quorum=4
	net := newE2ENet(allNodes).withDrop(func(m pb.Message) bool {
		// drop one follower's fast-vote (say the last leaderID)
		dropFrom := leaderIDs[len(leaderIDs)-1]
		return m.Type == pb.MsgGlobalFastVote && m.From == dropFrom
	})

	e2eInitGlobalMembershipOnLeaders(leaders, leaderIDs)

	glID := leaderIDs[0]
	gl := leaders[glID]
	gl.gTerm++
	gl.becomeGlobalLeader()
	net.pump(t)

	// Find GL's local cluster
	var glCluster map[uint64]*raft
	for _, cl := range clusters {
		if _, ok := cl[glID]; ok {
			glCluster = cl
			break
		}
	}
	if glCluster == nil {
		t.Fatal("no GL local cluster")
	}

	// Local commits + propose global batch
	e2eProposeLocalAndCommit(t, net, glCluster, glID, 3)
	e2eProposeGlobalBatch(t, net, glCluster, gl)
	net.pump(t) // deliver fast-prop
	// Force wrappers committed so votes will be sent (except our one dropped follower)
	for _, id := range leaderIDs {
		if id != glID {
			forceLocalCommitAll(leaders[id])
		}
	}

	// deliver votes (one is dropped by filter) -> leader should materialize (classic quorum)
	net.pump(t)
	// replicate & commit classically
	net.pump(t)

	act := currentGlobalLeader(leaders)
	if act == nil {
		t.Fatalf("no global leader")
	}
	if act.gLastIndex == 0 {
		t.Fatalf("leader did not materialize under classic fallback")
	}
	if act.gCommit == 0 {
		t.Fatalf("leader did not commit under classic fallback")
	}
}

func TestCRaft_GlobalElection_HeartbeatIsNotVote(t *testing.T) {
	// minimal 3 global voters, singleton locals
	nodes := make(map[uint64]*raft)
	gVoters := []uint64{1, 2, 3}
	for _, id := range gVoters {
		mem := NewMemoryStorage()
		mem.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{ConfState: pb.ConfState{Voters: []uint64{id}}, Index: 1, Term: 1}})
		cfg := &Config{ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: mem, EnableCRaft: true, GlobalVoters: gVoters, Logger: getLogger(), MaxInflightMsgs: 256, MaxSizePerMsg: 1 << 20}
		n := newRaft(cfg)
		nodes[id] = n
		// Make local leader
		if n.trk.Progress[n.id] == nil {
			n.trk.Progress[n.id] = &tracker.Progress{Match: n.raftLog.lastIndex(), Next: n.raftLog.lastIndex() + 1, Inflights: tracker.NewInflights(n.trk.MaxInflight, n.trk.MaxInflightBytes), RecentActive: true}
		}
		v := quorum.MajorityConfig{n.id: struct{}{}}
		n.trk.Config.Voters = quorum.JointConfig{v, nil}
	}
	net := newE2ENet(nodes).withDrop(func(m pb.Message) bool {
		// drop all global votes so only heartbeats flow
		return m.Type == pb.MsgGlobalVote || m.Type == pb.MsgGlobalVoteResp
	})
	// start an election from node 1
	n1 := nodes[1]
	n1.startGlobalElection()
	net.pump(t)
	if n1.gState == gStateLeader {
		t.Fatalf("candidate became leader using heartbeat responses; should require votes")
	}
}

func TestCRaft_E2E_RecoveryFromHints_Rebroadcast(t *testing.T) {
	// Build 3 clusters; elect local leaders
	all := map[uint64]*raft{}
	leaders := map[uint64]*raft{}
	var leaderIDs []uint64
	var clusters []map[uint64]*raft

	for base := uint64(3000); base < 3300; base += 100 {
		cl, lid := e2eNewLocalCluster(t, base) // 5-node locals is fine
		for id, n := range cl {
			all[id] = n
		}
		clusters = append(clusters, cl)
		leaders[lid] = cl[lid]
		leaderIDs = append(leaderIDs, lid)
	}

	// Optional: drop global appends so we keep the decision "in flight" until after the switch.
	net := newE2ENet(all).withDrop(func(m pb.Message) bool {
		// Block global App/Resp to prevent classic commit before the switch.
		return m.Type == pb.MsgGlobalApp || m.Type == pb.MsgGlobalAppResp
	})

	e2eInitGlobalMembershipOnLeaders(leaders, leaderIDs)

	glID := leaderIDs[0]
	gl := leaders[glID]
	gl.gTerm++
	gl.becomeGlobalLeader()
	net.pump(t)

	// Find the global leader's local cluster (this was missing!)
	var glCluster map[uint64]*raft
	for _, cl := range clusters {
		if _, ok := cl[glID]; ok {
			glCluster = cl
			break
		}
	}
	if glCluster == nil {
		t.Fatal("failed to locate the global leader's local cluster")
	}

	// Drive some local commits and broadcast a global batch (fast-prop)
	e2eProposeLocalAndCommit(t, net, glCluster, glID, 3)
	e2eProposeGlobalBatch(t, net, glCluster, gl)
	net.pump(t) // deliver fast-prop to other cluster leaders

	// Make all followers commit their local wrappers so they send fast votes
	for _, id := range leaderIDs {
		if id != glID {
			forceLocalCommitAll(leaders[id])
		}
	}
	net.pump(t) // deliver votes (global App is still dropped by filter)

	// --- Switch global leader BEFORE commit completes ---
	newID := leaderIDs[1]
	nl := leaders[newID]
	nl.gTerm = gl.gTerm + 1
	nl.becomeGlobalLeader() // triggers findLatestGlobalStateHint + rebroadcastGlobalAt
	net.pump(t)

	// Allow replication now so the new leader can finish classic or fast commit
	net.withDrop(nil)
	net.pump(t)
	net.pump(t)

	if nl.gLastIndex == 0 || nl.gCommit == 0 {
		t.Fatalf("new global leader did not rebroadcast/complete decision from hints; gLast=%d gCommit=%d",
			nl.gLastIndex, nl.gCommit)
	}
}

func TestCRaft_E2E_GlobalConfChange_JointConfig(t *testing.T) {
	// set up 3 clusters
	var all = map[uint64]*raft{}
	var leaders = map[uint64]*raft{}
	var leaderIDs []uint64
	var clusters []map[uint64]*raft
	var base uint64 = 4000
	for i := 0; i < 3; i++ {
		cl, lid := e2eNewLocalCluster(t, base)
		base += 100
		for id, n := range cl {
			all[id] = n
		}
		clusters = append(clusters, cl)
		leaders[lid] = cl[lid]
		leaderIDs = append(leaderIDs, lid)
	}

	net := newE2ENet(all)
	e2eInitGlobalMembershipOnLeaders(leaders, leaderIDs)

	// Pick global leader and bootstrap
	glID := leaderIDs[0]
	gl := leaders[glID]
	gl.gTerm++
	gl.becomeGlobalLeader()
	net.pump(t)

	// Find the global leader's local cluster
	var glCluster map[uint64]*raft
	for _, cl := range clusters {
		if _, ok := cl[glID]; ok {
			glCluster = cl
			break
		}
	}
	if glCluster == nil {
		t.Fatal("failed to locate the global leader's local cluster")
	}

	// Build a global batch that contains a ConfChangeV2 (REMOVE an existing voter)
	removed := leaderIDs[2] // ensure this is != glID
	cc := pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode, NodeID: removed}},
	}
	b, _ := cc.Marshal()
	ent := pb.Entry{Type: pb.EntryConfChangeV2, Data: b}

	// Propose and force local wrapper commit before broadcast
	e2eProposeGlobalBatchWithEntries(t, net, glCluster, gl, []pb.Entry{ent})

	// Give the broadcast a hop to reach followers
	net.pump(t)

	// Make sure non-global leaders actually *received* the fast-prop (wrappers recorded / vote queued)
	waitFastPropReceivedSpin(t, net, leaders, leaderIDs, glID, 20)

	// Ensure followers commit their wrappers and queue votes
	for _, id := range leaderIDs {
		if id != glID {
			forceLocalCommitAll(leaders[id])
		}
	}
	waitGlobalVotesQueuedSpin(t, net, leaders, leaderIDs, glID, 20)

	// Deliver votes and drive decision (conf change => classic fallback)
	net.pump(t) // deliver votes
	net.pump(t) // replicate chosen batch
	net.pump(t) // commit & apply

	// Should commit and apply config — gCommit should be > 0
	if gl.gCommit == 0 {
		t.Fatalf("global conf change not committed")
	}

	// Removed node's progress should be gone after apply
	if gl.gtrk.Progress[removed] != nil {
		t.Fatalf("removed node %x still present in global tracker after conf change", removed)
	}

	// Remaining leaders should still have progress
	for _, id := range leaderIDs {
		if id == removed {
			continue
		}
		if gl.gtrk.Progress[id] == nil {
			t.Fatalf("progress missing for %x after conf change", id)
		}
	}
}

func TestCRaft_GlobalFastCounter_FallbackWhenNoVotersSeeded(t *testing.T) {
	// 3 singletons
	nodes := make(map[uint64]*raft)
	ids := []uint64{11, 22, 33}
	for _, id := range ids {
		mem := NewMemoryStorage()
		mem.ApplySnapshot(pb.Snapshot{
			Metadata: pb.SnapshotMetadata{
				ConfState: pb.ConfState{Voters: []uint64{id}},
				Index:     1, Term: 1,
			},
		})
		cfg := &Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1,
			Storage: mem, EnableCRaft: true, Logger: getLogger(),
			MaxInflightMsgs: 256, MaxSizePerMsg: 1 << 20,
		}
		n := newRaft(cfg)
		// local singleton
		if n.trk.Progress[n.id] == nil {
			n.trk.Progress[n.id] = &tracker.Progress{
				Match:        n.raftLog.lastIndex(),
				Next:         n.raftLog.lastIndex() + 1,
				Inflights:    tracker.NewInflights(n.trk.MaxInflight, n.trk.MaxInflightBytes),
				RecentActive: true,
			}
		}
		n.trk.Config.Voters = quorum.JointConfig{quorum.MajorityConfig{n.id: {}}, nil}
		nodes[id] = n
	}

	// seed GLOBAL Progress on every node so global fallback electorate = {11,22,33}
	for _, n := range nodes {
		for _, id := range ids {
			pr := n.gtrk.Progress[id]
			if pr == nil {
				pr = &tracker.Progress{
					Match: 0, Next: 1,
					Inflights:    tracker.NewInflights(n.gtrk.MaxInflight, n.gtrk.MaxInflightBytes),
					RecentActive: true,
				}
				n.gtrk.Progress[id] = pr
			} else {
				pr.RecentActive = true
				if pr.Inflights == nil {
					pr.Inflights = tracker.NewInflights(n.gtrk.MaxInflight, n.gtrk.MaxInflightBytes)
				}
				if pr.Next == 0 {
					pr.Next = 1
				}
			}
			pr.IsLearner = false
		}
	}

	net := newE2ENet(nodes)

	// leader 11
	n11 := nodes[11]
	n11.gTerm++
	n11.becomeGlobalLeader()

	// build a local entry and a single-entry global batch
	ent := pb.Entry{Type: pb.EntryNormal, Data: []byte("x")}
	n11.appendEntry(ent)
	n11.raftLog.commitTo(n11.raftLog.lastIndex())
	n11.appliedTo(n11.raftLog.committed, 0)

	batch := pb.GlobalBatch{
		Entries:    []pb.Entry{ent},
		FirstIndex: n11.raftLog.committed,
		LastIndex:  n11.raftLog.committed,
	}
	n11.proposeGlobalStateEntry(batch)
	forceLocalCommitAll(n11) // leader wrapper commit → onGSECommitted → fast-prop enqueued

	// ✅ deliver fast-prop so followers append the wrapper first
	net.pump(t)

	// ✅ now commit wrapper on followers so appliedTo() sends FastVote
	for id, n := range nodes {
		if id != 11 {
			forceLocalCommitAll(n)
		}
	}

	// deliver votes → leader tallies/materializes
	net.pump(t)

	// optional: replication phase
	net.pump(t)

	if n11.gLastIndex == 0 {
		t.Fatalf("fallback fast vote counter failed to materialize without seeded voters")
	}
}
