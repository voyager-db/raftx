// global_election_test.go
package raft

import (
	"testing"

	pb "github.com/voyager-db/raftx/raftpb"
)

func newGlobalNodeForVoteTest(t *testing.T, id uint64) *raft {
	t.Helper()
	mem := NewMemoryStorage()
	// seed local singleton so node is promotable (not strictly required for global vote path)
	mem.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: []uint64{id}},
			Index:     1, Term: 1,
		},
	})
	r := newRaft(&Config{
		ID:               id,
		ElectionTick:     10,
		HeartbeatTick:    1,
		Storage:          mem,
		EnableCRaft:      true,
		MaxInflightMsgs:  256,
		MaxSizePerMsg:    1 << 20,
		Logger:           getLogger(),
	})
	// empty global log; callers can seed r.glog as needed
	return r
}

func sendGlobalVote(t *testing.T, voter *raft, candID, term, candLastIdx, candLastTerm uint64) (resp *pb.Message) {
	t.Helper()
	_ = voter.Step(pb.Message{
		To:            voter.id,
		From:          candID,
		Type:          pb.MsgGlobalVote,
		Term:          term,
		GlobalIndex:   candLastIdx,
		GlobalLogTerm: candLastTerm,
	})
	// MsgGlobalVoteResp is a "response" class; it will be queued in msgsAfterAppend
	for i := range voter.msgsAfterAppend {
		m := &voter.msgsAfterAppend[i]
		if m.Type == pb.MsgGlobalVoteResp && m.To == candID {
			resp = m
			break
		}
	}
	// clear queues for next call
	voter.msgs = nil
	voter.msgsAfterAppend = nil
	return resp
}

func TestGlobalVote_UpToDate_EqualTermIndex_Grant(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	// follower global log at (idx=1, term=1)
	v.glog.ents[1] = gEntry{term: 1}
	v.glog.last = 1

	resp := sendGlobalVote(t, v, 2, /*vote term*/ 1, /*cand idx*/ 1, /*cand term*/ 1)
	if resp == nil || resp.Reject {
		t.Fatalf("expected grant, got %#v", resp)
	}
	if v.gVote != 2 {
		t.Fatalf("expected gVote=2, got %d", v.gVote)
	}
}

func TestGlobalVote_UpToDate_HigherTerm_Grant(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	v.glog.ents[5] = gEntry{term: 3}
	v.glog.last = 5
	// candidate has term 4, index 2 -> term wins
	resp := sendGlobalVote(t, v, 2, 4, 2, 4)
	if resp == nil || resp.Reject {
		t.Fatalf("expected grant on higher term, got %#v", resp)
	}
}

func TestGlobalVote_UpToDate_LowerIndex_Reject(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	v.glog.ents[7] = gEntry{term: 2}
	v.glog.last = 7
	// candidate term 2, but index 6 (<7) -> reject
	resp := sendGlobalVote(t, v, 2, 2, 6, 2)
	if resp == nil || !resp.Reject {
		t.Fatalf("expected reject on lower index, got %#v", resp)
	}
}

func TestGlobalVote_OnlyOnePerTerm(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	v.glog.ents[1] = gEntry{term: 1}
	v.glog.last = 1

	r1 := sendGlobalVote(t, v, 2, 3, 1, 1) // term=3, up-to-date -> grant
	if r1 == nil || r1.Reject || v.gVote != 2 {
		t.Fatalf("first vote should be granted, resp=%#v, gVote=%d", r1, v.gVote)
	}
	r2 := sendGlobalVote(t, v, 99, 3, 1, 1) // same term, different candidate -> reject
	if r2 == nil || !r2.Reject {
		t.Fatalf("second vote in same term should be rejected, got %#v", r2)
	}
}

func TestGlobalVote_BumpsTermOnHigherCandidate(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	v.gTerm = 5
	v.glog.ents[2] = gEntry{term: 5}
	v.glog.last = 2

	// candidate presents higher message term; follower should become global follower at term 6 and evaluate
	resp := sendGlobalVote(t, v, 2, 6, 2, 5)
	if v.gTerm != 6 {
		t.Fatalf("expected gTerm=6, got %d", v.gTerm)
	}
	if resp == nil || resp.Reject {
		t.Fatalf("expected grant after bumping term, got %#v", resp)
	}
}

func TestGlobalVote_IgnoresStaleTerm(t *testing.T) {
	v := newGlobalNodeForVoteTest(t, 1)
	v.gTerm = 4
	v.glog.ents[3] = gEntry{term: 4}
	v.glog.last = 3

	// stale vote: message Term < follower's gTerm; stepGlobal should drop it (no resp)
	resp := sendGlobalVote(t, v, 2, 3, 3, 4)
	if resp != nil {
		t.Fatalf("expected no response to stale-term vote, got %#v", resp)
	}
	// vote state unchanged
	if v.gVote != 0 {
		t.Fatalf("expected no vote recorded, got %d", v.gVote)
	}
}
