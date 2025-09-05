package raft

import (
    "math"
    "testing"

    pb "github.com/voyager-db/raftx/raftpb"
    "github.com/voyager-db/raftx/quorum"
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
    cfg.Voters[0] = quorum.MajorityConfig{ r.id: struct{}{} }
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

func TestFollowerIgnoresOutOfWindowProposals(t *testing.T) {
    r := newRaft(baseConfigFast(2))
    r.becomeFollower(1, None)
    mustAppendCommitted(r, 5)
    idx := r.raftLog.committed + uint64(r.fastOpenIndexWindow) + 1
    m := pb.Message{
        Type:  pb.MsgFastProp,
        Index: idx,
        Entries: []pb.Entry{{
            Type:      pb.EntryNormal,
            ContentId: []byte("cid-out"),
        }},
    }
    if err := r.Step(m); err != nil {
        t.Fatalf("step: %v", err)
    }
    if r.raftLog.lastIndex() != r.raftLog.committed {
        t.Fatalf("out-of-window fast-prop was appended")
    }
}

func TestFollowerDoesNotOverwriteOnFastProp(t *testing.T) {
    r := newRaft(baseConfigFast(2))
    r.becomeFollower(1, None)
    mustAppendCommitted(r, 5)
    mustAppendUncommitted(r, 1) // lastIndex=6

    m := pb.Message{
        Type:  pb.MsgFastProp, Index: 6,
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

func TestFollowerSendsFastVoteToLeaderAndLeaderTallies(t *testing.T) {
	// Leader (1)
	rl := newRaft(baseConfigFast(1))
	primeSingleVoter(rl, rl.id)
	rl.becomeCandidate()
	rl.becomeLeader()
	mustAppendCommitted(rl, 5)

	// Follower (2) that knows its leader
	rf := newRaft(baseConfigFast(2))
	rf.becomeFollower(rl.Term, rl.id)
	rf.lead = rl.id // make sure leader is set
	mustAppendCommitted(rf, 5)

	idx := rl.raftLog.committed + 1

	// Follower accepts fast prop and emits a vote to the leader
	prop := pb.Message{
		Type:  pb.MsgFastProp,
		Index: idx,
		Entries: []pb.Entry{{
			Type:      pb.EntryNormal,
			Data:      []byte("v1"),
			ContentId: []byte("cid-v1"),
		}},
	}
	if err := rf.Step(prop); err != nil {
		t.Fatalf("follower step: %v", err)
	}

	// Deliver the vote to the leader (search both queues)
	var vote pb.Message
	found := false
	for _, msg := range rf.msgs {
		if msg.Type == pb.MsgFastVote {
			vote = msg; found = true; break
		}
	}
	if !found {
		for _, msg := range rf.msgsAfterAppend {
			if msg.Type == pb.MsgFastVote {
				vote = msg; found = true; break
			}
		}
	}
	if !found {
		t.Fatalf("follower did not send MsgFastVote")
	}
	if vote.To != rl.id {
		t.Fatalf("vote sent to %d want %d", vote.To, rl.id)
	}
	if len(vote.FastVotes) != 1 || vote.FastVotes[0] == nil || vote.FastVotes[0].Index == nil || *vote.FastVotes[0].Index != idx {
		t.Fatalf("vote content bad: %#v", vote.FastVotes)
	}
	if err := rl.Step(vote); err != nil {
		t.Fatalf("leader step: %v", err)
	}

	// Verify leader tallied the vote at idx for cid-v1
	if rl.possibleEntries == nil || rl.possibleEntries[idx] == nil {
		t.Fatalf("no tally bucket for index %d", idx)
	}
	set, ok := rl.possibleEntries[idx]["cid-v1"]
	if !ok || len(set) != 1 {
		t.Fatalf("unexpected tally for cid-v1: %#v", rl.possibleEntries[idx])
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
    rl := newRaft(baseConfigFast(1))
    primeSingleVoter(rl, rl.id)
    rl.becomeCandidate()
    rl.becomeLeader()
    mustAppendCommitted(rl, 3)

    idx := rl.raftLog.committed + 1
    msg := pb.Message{
        Type:  pb.MsgFastProp,
        Index: idx,
        Entries: []pb.Entry{{
            Type:      pb.EntryNormal,
            Data:      []byte("payload"),
            ContentId: []byte("cid-x"),
        }},
    }
    if err := rl.Step(msg); err != nil {
        t.Fatalf("leader step: %v", err)
    }
    if rl.proposalCache[idx] == nil {
        t.Fatalf("cache bucket missing")
    }
    if e, ok := rl.proposalCache[idx]["cid-x"]; !ok || string(e.Data) != "payload" {
        t.Fatalf("cache miss or wrong payload")
    }
}
