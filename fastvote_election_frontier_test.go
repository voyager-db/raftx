package raft

import (
	"testing"

	pb "github.com/voyager-db/raftx/raftpb"
)

// --- helpers ----------------------------------------------------------------

func newVoterWithLog(t *testing.T, id uint64, voters []uint64, lastIdx, lastTerm uint64, lastLeaderIdx uint64) *raft {
	t.Helper()

	ms := NewMemoryStorage()
	// Seed snapshot so term lookups work.
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
	if lastIdx >= 2 {
		var ents []pb.Entry
		for i := uint64(2); i <= lastIdx; i++ {
			ents = append(ents, pb.Entry{Index: i, Term: lastTerm, Type: pb.EntryNormal})
		}
		if err := ms.Append(ents); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	cfg := &Config{
		ID:              id,
		Storage:         ms,
		ElectionTick:    10,
		HeartbeatTick:   1,
		MaxInflightMsgs: 256,
		MaxSizePerMsg:   1 << 20,
		CheckQuorum:     false,
		PreVote:         true,
		EnableFastPath:  true,
	}
	r := newRaft(cfg)

	// IMPORTANT: start with non-zero term to avoid sending PreVoteResp with term 0.
	r.becomeFollower(1, None)
	r.lead = None
	r.lastLeaderIndex = lastLeaderIdx

	r.msgs = nil
	r.msgsAfterAppend = nil
	return r
}

func preVote(from uint64, term, idx, lterm uint64, candLLI, candLLT *uint64) pb.Message {
	msg := pb.Message{
		From:    from,
		Type:    pb.MsgPreVote,
		Term:    term,
		Index:   idx,
		LogTerm: lterm,
	}
	if candLLI != nil {
		msg.CandLastLeaderIndex = candLLI
	}
	if candLLT != nil {
		msg.CandLastLeaderTerm = candLLT
	}
	return msg
}

func singleResp(t *testing.T, r *raft) pb.Message {
	t.Helper()
	if len(r.msgsAfterAppend) != 1 {
		t.Fatalf("expected 1 response, got %d (msgsAfterAppend=%v)", len(r.msgsAfterAppend), r.msgsAfterAppend)
	}
	m := r.msgsAfterAppend[0]
	r.msgsAfterAppend = nil
	return m
}

// --- tests ------------------------------------------------------------------

// Candidate with stronger (LLI,LLT) wins even if classic lastIndex/lastTerm is worse.
func TestVotePrefersLeaderApprovedFrontier(t *testing.T) {
	// Voter frontier: (LLI=5, LLT=2), log up to index=8 @ term=2.
	v := newVoterWithLog(t, 2, []uint64{1, 2, 3}, 8, 2, 5)

	// Candidate A: (LLI=6, LLT=2) >= (5,2) -> grant.
	lliA, lltA := uint64(6), uint64(2)
	reqA := preVote(1 /*term*/, 2 /*classic idx*/, 1 /*classic term*/, 1, &lliA, &lltA)
	_ = v.Step(reqA)
	resp := singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || resp.Reject {
		t.Fatalf("expected grant for candidate A; got %+v", resp)
	}
	// classic semantics: grant carries m.Term
	if resp.Term != reqA.Term {
		t.Fatalf("grant PreVoteResp term mismatch: got %d want %d", resp.Term, reqA.Term)
	}

	// Candidate B: (LLI=4, LLT=2) < (5,2) -> reject (even if classic fields look better).
	lliB, lltB := uint64(4), uint64(2)
	reqB := preVote(3 /*term*/, 2 /*classic idx*/, 100 /*classic term*/, 99, &lliB, &lltB)
	_ = v.Step(reqB)
	resp = singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || !resp.Reject {
		t.Fatalf("expected reject for candidate B; got %+v", resp)
	}
	// classic semantics: reject carries local r.Term
	if resp.Term != v.Term {
		t.Fatalf("reject PreVoteResp term mismatch: got %d want local %d", resp.Term, v.Term)
	}
}

// If the new frontier fields are absent, fall back to classic isUpToDate.
func TestVoteBackCompatClassicFields(t *testing.T) {
	v := newVoterWithLog(t, 2, []uint64{1, 2, 3}, 7, 3, 5)
	myLast := v.raftLog.lastEntryID()

	// Classic-more-up-to-date -> grant.
	reqA := preVote(1 /*term*/, 2, myLast.index+1, myLast.term+1 /*candLLI*/, nil /*candLLT*/, nil)
	_ = v.Step(reqA)
	resp := singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || resp.Reject {
		t.Fatalf("expected classic grant; got %+v", resp)
	}
	if resp.Term != reqA.Term {
		t.Fatalf("grant PreVoteResp term mismatch: got %d want %d", resp.Term, reqA.Term)
	}

	// Classic-less-up-to-date -> reject.
	reqB := preVote(3 /*term*/, 2, myLast.index-1, myLast.term /*candLLI*/, nil /*candLLT*/, nil)
	_ = v.Step(reqB)
	resp = singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || !resp.Reject {
		t.Fatalf("expected classic reject; got %+v", resp)
	}
	if resp.Term != v.Term {
		t.Fatalf("reject PreVoteResp term mismatch: got %d want local %d", resp.Term, v.Term)
	}
}

// Long self-approved tail with a LOWER (LLI,LLT) must lose to a candidate with
// higher frontier, regardless of classic length.
func TestCandidateWithLongSelfTailLosesToShorterLeaderPrefix(t *testing.T) {
	// Voter frontier: (LLI=8, LLT=2)
	v := newVoterWithLog(t, 2, []uint64{1, 2, 3}, 20, 2, 8)

	// Candidate B: advertises lower frontier -> reject.
	lliB, lltB := uint64(6), uint64(2)
	reqB := preVote(3 /*term*/, 2 /*classic idx*/, 100 /*classic term*/, 2, &lliB, &lltB)
	_ = v.Step(reqB)
	resp := singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || !resp.Reject {
		t.Fatalf("expected reject for low frontier; got %+v", resp)
	}
	if resp.Term != v.Term {
		t.Fatalf("reject PreVoteResp term mismatch: got %d want local %d", resp.Term, v.Term)
	}

	// Candidate A: frontier >= voter's -> grant.
	lliA, lltA := uint64(9), uint64(2)
	reqA := preVote(1 /*term*/, 2 /*classic idx*/, 9 /*classic term*/, 2, &lliA, &lltA)
	_ = v.Step(reqA)
	resp = singleResp(t, v)
	if resp.Type != pb.MsgPreVoteResp || resp.Reject {
		t.Fatalf("expected grant for higher frontier; got %+v", resp)
	}
	if resp.Term != reqA.Term {
		t.Fatalf("grant PreVoteResp term mismatch: got %d want %d", resp.Term, reqA.Term)
	}
}
