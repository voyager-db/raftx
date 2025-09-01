package raft

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/voyager-db/raftx/confchange"
	"github.com/voyager-db/raftx/quorum"
	"github.com/voyager-db/raftx/tracker"

	pb "github.com/voyager-db/raftx/raftpb"
)

type pendingGlobalAck struct {
	to     uint64
	gIndex uint64
	gTerm  uint64
	digest string
}

// ====== C-RAFT: global state machine ======
type gStateType uint8

const (
	gStateFollower gStateType = iota
	gStateCandidate
	gStateLeader
)

func (s gStateType) String() string {
	switch s {
	case gStateFollower:
		return "GStateFollower"
	case gStateCandidate:
		return "GStateCandidate"
	case gStateLeader:
		return "GStateLeader"
	default:
		return "GStateUnknown"
	}
}

// ====== C-RAFT ======
func (r *raft) tickGlobal() {
	if !r.enableCRaft {
		return
	}
	if r.state != StateLeader {
		// Only local leaders drive global consensus
		return
	}
	r.gElectionElapsed++
	r.gHeartbeatElapsed++

	// Global election (PreVote is optional; simple election shown)
	if r.gElectionElapsed >= r.gElectionTimeout && r.gState != gStateLeader {
		r.startGlobalElection()
	}

	if r.gState == gStateLeader && r.gHeartbeatElapsed >= r.gHeartbeatTimeout {
		r.gHeartbeatElapsed = 0
		r.bcastGlobalHeartbeat()
		// Optional: trigger batching policy periodically
		r.maybeProposeGlobalBatch()
	}
}

func (r *raft) becomeGlobalFollower(term uint64, lead uint64) {
	r.gState = gStateFollower
	r.gTerm = term
	r.gVote = None
	r.gLead = lead
	r.gElectionElapsed = 0
	r.gHeartbeatElapsed = 0
	// Reset global fast matches
	r.gtrk.ResetFastMatches()
}

func (r *raft) becomeGlobalCandidate() {
	r.gState = gStateCandidate
	r.gTerm++
	r.gVote = r.id // local leader self-votes at global tier
	r.gLead = None
	r.gElectionElapsed = 0
	r.gHeartbeatElapsed = 0
	r.gtrk.ResetVotes()
}

// findLatestGlobalStateHint finds the most recent local global-state entry.
func (r *raft) findLatestGlobalStateHint() (uint64, *pb.GlobalBatch) {
	lo := r.raftLog.firstIndex()
	hi := r.raftLog.lastIndex()
	if hi < lo {
		return 0, nil
	}
	for i := hi; ; i-- {
		e, err := r.raftLog.entry(i)
		if err == nil && len(e.Data) > 0 {
			var gse pb.GlobalStateEntry
			if gse.Unmarshal(e.Data) == nil && gse.GlobalIndex != 0 && gse.Batch != nil {
				return gse.GlobalIndex, gse.Batch
			}
		}
		if i == lo {
			break
		}
	}
	return 0, nil
}

func (r *raft) rebroadcastGlobalAt(gIndex uint64, batch *pb.GlobalBatch) {
	if r.gState != gStateLeader || batch == nil || gIndex == 0 {
		return
	}

	r.rememberGlobalBatch(gIndex, batch)

	prev := gIndex - 1
	prevTerm, _ := r.glog.termAt(prev)
	if lastnew, ok := r.glog.maybeAppend(prev, prevTerm, r.gTerm, batch); ok {
		if lastnew > r.gLastIndex {
			r.gLastIndex = lastnew
		}
		// NEW: reflect our local append in the leader's global Progress
		if pr := r.gtrk.Progress[r.id]; pr != nil {
			if pr.Match < r.gLastIndex {
				pr.Match = r.gLastIndex
				pr.Next = pr.Match + 1
			}
		}
	} else {
		r.logger.Errorf("%x: rebroadcast append failed at global idx %d (prev=%d, prevTerm=%d)", r.id, gIndex, prev, prevTerm)
		return
	}

	// Seed fast-vote counter and leader's fast match (you already have this)
	dig := globalBatchDigest(batch)
	ctr := r.gPossibleBatches[gIndex]
	if ctr == nil {
		ctr = quorum.NewFastVoteCounter(r.globalMajorityConfig())
		r.gPossibleBatches[gIndex] = ctr
	}
	ctr.RecordVote(r.id, dig)
	if pr := r.gtrk.Progress[r.id]; pr != nil && !pr.IsLearner && pr.FastMatchIndex < gIndex {
		pr.FastMatchIndex = gIndex
	}

	// Re-broadcast fast-prop so followers that already committed wrappers re-vote
	base := pb.Message{
		Type:          pb.MsgGlobalFastProp,
		GlobalBatch:   batch,
		GlobalIndex:   gIndex,
		GlobalLogTerm: r.gTerm,
		Context:       []byte(dig),
	}
	r.gtrk.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		m := base
		m.To = id
		r.send(m)
	})
}

func (r *raft) becomeGlobalLeader() {
	if r.gState == gStateLeader {
		return
	}
	r.gState = gStateLeader
	r.gLead = r.id
	r.gElectionElapsed, r.gHeartbeatElapsed = 0, 0
	if pr := r.gtrk.Progress[r.id]; pr != nil {
		pr.BecomeReplicate()
		pr.RecentActive = true
	}
	// Recover in-flight decision if any
	if gi, batch := r.findLatestGlobalStateHint(); gi != 0 && batch != nil {
		r.rebroadcastGlobalAt(gi, batch)
	} else {
		r.bcastGlobalHeartbeat()
	}
}

// in raft.tickGlobal() leave the election trigger as-is; it will call:
func (r *raft) startGlobalElection() {
	r.becomeGlobalCandidate()

	// candidate's last leader-approved GLOBAL log coords
	lastIdx := r.glog.lastIndex()
	lastTerm, _ := r.glog.termAt(lastIdx)

	// send vote requests to all global voters
	var ids []uint64
	for id := range r.gtrk.Voters.IDs() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if id == r.id {
			// count our durable self-vote after persistence
			r.handleGlobalVoteResp(pb.Message{From: r.id, Term: r.gTerm, Type: pb.MsgGlobalVoteResp})
			continue
		}
		r.send(pb.Message{
			To:            id,
			Type:          pb.MsgGlobalVote,
			Term:          r.gTerm,
			GlobalLogTerm: lastTerm, // candidate’s last leader-approved global term
			GlobalIndex:   lastIdx,  // candidate’s last leader-approved global index
		})
	}
}

// helper: "up-to-date" check at GLOBAL tier
func (r *raft) globalUpToDate(candLastIdx, candLastTerm uint64) bool {
	myIdx := r.glog.lastIndex()
	myTerm, _ := r.glog.termAt(myIdx)
	if candLastTerm != myTerm {
		return candLastTerm > myTerm
	}
	return candLastIdx >= myIdx
}

func (r *raft) handleGlobalVote(m pb.Message) {
	// term handling
	if m.Term > r.gTerm {
		r.becomeGlobalFollower(m.Term, None)
	}
	// only one vote per global term
	canVote := (r.gVote == None || r.gVote == m.From)
	upToDate := r.globalUpToDate(m.GlobalIndex, m.GlobalLogTerm)

	grant := canVote && upToDate
	if grant {
		r.gVote = m.From // persist with other global state in your Ready/HardState if you have one
	}
	r.send(pb.Message{
		To:   m.From,
		Type: pb.MsgGlobalVoteResp,
		Term: r.gTerm,
		// Reject=false means vote granted
		Reject: !grant,
	})
}

func (r *raft) handleGlobalVoteResp(m pb.Message) {
	if r.gState != gStateCandidate {
		return
	}
	r.gtrk.RecordVote(m.From, !m.Reject)
	gr, rj, res := r.gtrk.TallyVotes()
	r.logger.Infof("%x (global) vote tally: grants=%d rejects=%d", r.id, gr, rj)
	switch res {
	case quorum.VoteWon:
		r.becomeGlobalLeader()
	case quorum.VoteLost:
		r.becomeGlobalFollower(r.gTerm, None)
	}
}

func (r *raft) stepGlobal(m pb.Message) error {
	// Basic term handling for the global tier
	switch {
	case m.Term == 0:
		// local/global plumbing message
	case m.Term > r.gTerm:
		if m.Type == pb.MsgGlobalHeartbeat || m.Type == pb.MsgGlobalApp {
			r.becomeGlobalFollower(m.Term, m.From)
		} else {
			r.becomeGlobalFollower(m.Term, None)
		}
	case m.Term < r.gTerm:
		switch m.Type {
		case pb.MsgGlobalHeartbeat:
			r.send(pb.Message{To: m.From, Type: pb.MsgGlobalHeartbeatResp})
		case pb.MsgGlobalApp:
			r.send(pb.Message{To: m.From, Type: pb.MsgGlobalAppResp})
		}
		r.logger.Infof("%x (global) rejected msg from %x due to stale term %d < %d", r.id, m.From, m.Term, r.gTerm)
		return nil
	}

	switch m.Type {
	case pb.MsgGlobalVote:
		r.handleGlobalVote(m)
		return nil
	case pb.MsgGlobalVoteResp:
		r.handleGlobalVoteResp(m)
		return nil

	case pb.MsgGlobalHeartbeat:
		// accept leader and bump commit, but do NOT treat as a vote
		r.gLead = m.From
		if m.GlobalCommit > r.gCommit {
			old := r.gCommit
			r.gCommit = m.GlobalCommit
			for i := old + 1; i <= r.gCommit; i++ {
				r.applyGlobalCommittedAt(i)
			}
		}
		r.send(pb.Message{To: m.From, Type: pb.MsgGlobalHeartbeatResp})
		return nil
	case pb.MsgGlobalHeartbeatResp:
		return nil

	case pb.MsgGlobalFastProp:
		// RECEIVER (cluster leader) must first replicate a global-state entry locally
		return r.handleGlobalFastProp(m)

	case pb.MsgGlobalFastVote:
		if r.gState == gStateLeader {
			return r.handleGlobalFastVote(m)
		}
		return nil

	case pb.MsgGlobalApp:
		// Classic fallback append at global tier
		return r.handleGlobalAppend(m)

	case pb.MsgGlobalAppResp:
		// Similar to local MsgAppResp but on gtrk
		return r.handleGlobalAppendResp(m)

	case pb.MsgGlobalTimeoutNow:
		// Force global election
		r.startGlobalElection()
		return nil
	}
	return nil
}

func (r *raft) bcastGlobalHeartbeat() {
	if r.gState != gStateLeader {
		return
	}
	r.gtrk.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.send(pb.Message{
			To:           id,
			Type:         pb.MsgGlobalHeartbeat,
			GlobalCommit: r.gCommit,
		})
	})
}

// maybeProposeGlobalBatch is a simple policy: if we (local leader) have new locally
// committed entries after the last included index, package them into one batch.
func (r *raft) maybeProposeGlobalBatch() {
	if !r.enableCRaft || r.state != StateLeader || r.gState != gStateLeader {
		return
	}
	lo := r.globLastLocalIncluded + 1
	hi := r.raftLog.committed
	if hi < lo {
		return
	}
	ents, err := r.raftLog.entries(lo, noLimit)
	if err != nil || len(ents) == 0 {
		return
	}

	// Filter out leader no-ops (EntryNormal with empty Data).
	filtered := make([]pb.Entry, 0, len(ents))
	var firstIdx, lastIdx uint64
	for i := range ents {
		e := ents[i]
		if e.Type == pb.EntryNormal && len(e.Data) == 0 {
			continue
		}
		filtered = append(filtered, e)
		if firstIdx == 0 {
			firstIdx = e.Index
		}
		lastIdx = e.Index
	}
	if len(filtered) == 0 {
		return // nothing meaningful to batch
	}

	batch := pb.GlobalBatch{
		Entries:    filtered,
		FirstIndex: firstIdx,
		LastIndex:  lastIdx,
	}
	// ✅ fill digest
	r.setBatchDigest(&batch)

	r.proposeGlobalStateEntry(batch)
}

// setBatchDigest populates b.Digest if empty using globalBatchDigest.
// If already set, it verifies consistency and logs a warning on mismatch.
func (r *raft) setBatchDigest(b *pb.GlobalBatch) {
	if b == nil {
		return
	}
	got := globalBatchDigest(b) // string holding raw bytes
	if len(b.Digest) == 0 {
		b.Digest = []byte(got)
		return
	}
	// verify
	if string(b.Digest) != got {
		r.logger.Warningf("[G] batch digest mismatch: provided=%x recomputed=%x (first=%d last=%d ents=%d)",
			b.Digest, got, b.FirstIndex, b.LastIndex, len(b.Entries))
		// normalize to recomputed to avoid split-brain on tally keys
		b.Digest = []byte(got)
	}
}

func (r *raft) proposeGlobalStateEntry(batch pb.GlobalBatch) {
	// Predict local index assignment (fast path uses committed+1 like your broadcastFastProp).
	k := r.raftLog.committed + 1
	gse := pb.GlobalStateEntry{
		Batch:       &batch,
		GlobalIndex: r.gLastIndex + 1,
		GlobalTerm:  r.gTerm,
	}
	data, _ := gse.Marshal() // gogoproto generated
	e := pb.Entry{Type: pb.EntryNormal, Data: data}
	// Optional hint for tooling:
	e.IsGlobalState = true

	if r.enableFastPath {
		r.broadcastFastProp([]pb.Entry{e})
	} else {
		if !r.appendEntry(e) {
			r.logger.Warningf("%x: dropped global-state entry", r.id)
			return
		}
		r.bcastAppend()
	}
	// Remember to continue once local index k is applied/committed.
	r.pendingGlobalState[k] = batch
}

func (r *raft) onGlobalStateEntryCommitted(batch pb.GlobalBatch) {
	if r.gState != gStateLeader {
		return
	}
	gIndex := r.gLastIndex + 1
	r.logger.Infof("[G] onGSECommitted idx=%d first=%d last=%d batchEnts=%d",
		gIndex, batch.FirstIndex, batch.LastIndex, len(batch.Entries))
	r.rememberGlobalBatch(gIndex, &batch)
	dig := globalBatchDigest(&batch)

	// Ensure a counter exists for this index
	ctr := r.gPossibleBatches[gIndex]
	if ctr == nil {
		ctr = quorum.NewFastVoteCounter(r.globalMajorityConfig())
		r.gPossibleBatches[gIndex] = ctr
	}

	// **Count the leader's own fast vote** for this digest
	ctr.RecordVote(r.id, dig)

	// **Bump the leader's FastMatchIndex** at this global index
	if pr := r.gtrk.Progress[r.id]; pr != nil && !pr.IsLearner && pr.FastMatchIndex < gIndex {
		pr.FastMatchIndex = gIndex
	}

	// Broadcast fast proposal (include digest so followers vote on the same thing)
	base := pb.Message{
		Type:          pb.MsgGlobalFastProp,
		GlobalBatch:   &batch,
		GlobalIndex:   gIndex,
		GlobalLogTerm: r.gTerm,
		Context:       []byte(dig),
	}
	r.gtrk.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		m := base
		m.To = id
		r.send(m)
	})

	// Advance local bookkeeping for next batch window
	r.globLastLocalIncluded = batch.LastIndex
}

// hasCommittedGlobalStateFor reports whether the local log contains a committed
// global state entry for the given global index.
func (r *raft) hasCommittedGlobalStateFor(gIndex uint64) bool {
	if gIndex == 0 {
		return false
	}
	lo := r.raftLog.firstIndex()
	hi := r.raftLog.committed
	if hi < lo {
		return false
	}
	for i := hi; ; i-- {
		e, err := r.raftLog.entry(i)
		if err == nil && len(e.Data) > 0 {
			var gse pb.GlobalStateEntry
			if gse.Unmarshal(e.Data) == nil && gse.GlobalIndex == gIndex {
				return true
			}
		}
		if i == lo {
			break
		}
	}
	return false
}

func (r *raft) findCommittedWrapperBatch(gIndex uint64) *pb.GlobalBatch {
	lo, hi := r.raftLog.firstIndex(), r.raftLog.committed
	for i := hi; i >= lo; i-- {
		e, err := r.raftLog.entry(i)
		if err == nil && len(e.Data) > 0 {
			var gse pb.GlobalStateEntry
			if gse.Unmarshal(e.Data) == nil && gse.GlobalIndex == gIndex && gse.Batch != nil {
				return gse.Batch
			}
		}
		if i == lo {
			break
		}
	}
	return nil
}

func (r *raft) handleGlobalFastProp(m pb.Message) error {
	// If we've already committed a wrapper for this global index, vote immediately.
	if r.hasCommittedGlobalStateFor(m.GlobalIndex) {
		// vote with the digest of our locally committed wrapper (not the leader’s incoming one)
		var dig string
		if b := r.findCommittedWrapperBatch(m.GlobalIndex); b != nil {
			dig = globalBatchDigest(b)
		} else if m.GlobalBatch != nil {
			// fallback (shouldn’t normally happen)
			dig = globalBatchDigest(m.GlobalBatch)
		}
		vote := pb.Message{
			Type:          pb.MsgGlobalFastVote,
			To:            m.From,
			From:          r.id,
			GlobalIndex:   m.GlobalIndex,
			GlobalLogTerm: m.GlobalLogTerm,
			Context:       []byte(dig),
			Term:          m.GlobalLogTerm,
		}
		r.send(vote)
		return nil
	}

	if m.GlobalBatch != nil {
		gse := pb.GlobalStateEntry{
			Batch:       m.GlobalBatch,
			GlobalIndex: m.GlobalIndex,
			GlobalTerm:  m.GlobalLogTerm,
		}
		data, _ := gse.Marshal()

		e := pb.Entry{
			Type:          pb.EntryNormal,
			Data:          data,
			IsGlobalState: true,
		}

		if !r.appendEntry(e) {
			r.logger.Warningf("%x: dropped global-state wrapper on fast-prop", r.id)
			return nil
		}

		// ✅ CAPTURE ACTUAL LOCAL INDEX
		newLast := r.raftLog.lastIndex()
		r.logger.Infof("[G] got FastProp -> appended wrapper localIdx=%d gIndex=%d from=%x",
			newLast, m.GlobalIndex, m.From)

		if r.pendingGlobalAcks == nil {
			r.pendingGlobalAcks = make(map[uint64]pendingGlobalAck)
		}
		r.pendingGlobalAcks[newLast] = pendingGlobalAck{
			to:     m.From,
			gIndex: m.GlobalIndex,
			gTerm:  m.GlobalLogTerm,
			digest: globalBatchDigest(m.GlobalBatch),
		}
	}
	return nil
}

// send the chosen batch at idx to all followers now
func (r *raft) broadcastGlobalChosen(idx uint64) {
	r.gtrk.Visit(func(id uint64, pr *tracker.Progress) {
		if id == r.id || pr == nil || pr.IsLearner {
			return
		}
		// ensure we probe from the chosen index
		if pr.State != tracker.StateProbe {
			pr.BecomeProbe()
		}
		if pr.Next > idx {
			pr.Next = idx
		}
		r.maybeSendGlobalAppend(id, true /* sendIfEmpty */)
	})
}

func (r *raft) handleGlobalFastVote(m pb.Message) error {
	idx := m.GlobalIndex
	ctr := r.gPossibleBatches[idx]
	if ctr == nil {
		ctr = quorum.NewFastVoteCounter(r.globalMajorityConfig())
		r.gPossibleBatches[idx] = ctr
	}
	ctr.RecordVote(m.From, string(m.Context))

	if pr := r.gtrk.Progress[m.From]; pr != nil {
		if pr.State != tracker.StateProbe {
			pr.BecomeProbe()
		}
		if pr.Next > idx {
			pr.Next = idx
		}
	}

	tally := ctr.Tally()
	if !tally.HasClassicQuorum {
		return nil
	}

	r.logger.Infof("[G] tally idx=%d hasClassic=%v hasFast=%v grants=%d leadingVoters=%v",
		idx, tally.HasClassicQuorum, r.gtrk.HasFastQuorum(idx), len(tally.LeadingVoters), tally.LeadingVoters)

	// bump FastMatchIndex for voters of leading digest BEFORE checking fast quorum
	for _, id := range tally.LeadingVoters {
		if pr := r.gtrk.Progress[id]; pr != nil && !pr.IsLearner && pr.FastMatchIndex < idx {
			pr.FastMatchIndex = idx
		}
	}

	chosen := r.lookupGlobalBatchByDigest(idx, tally.LeadingDigest)
	if chosen == nil {
		r.logger.Errorf("%x: missing global batch for idx=%d digest=%x", r.id, idx, tally.LeadingDigest)
		return nil
	}

	// ALWAYS materialize into our global log
	r.logger.Infof("[G] materialize idx=%d prev=%d", idx, idx-1)

	// ✅ If already materialized, skip appending again
	if ge, ok := r.glog.ents[idx]; ok && ge.batch != nil {
		// keep r.gLastIndex in sync just in case
		if r.gLastIndex < idx {
			r.gLastIndex = idx
			if pr := r.gtrk.Progress[r.id]; pr != nil {
				if pr.Match < r.gLastIndex {
					pr.Match = r.gLastIndex
				}
				pr.Next = pr.Match + 1
			}
		}
	} else {
		// First materialization
		prev := idx - 1
		prevTerm, _ := r.glog.termAt(prev)
		if lastnew, ok := r.glog.maybeAppend(prev, prevTerm, r.gTerm, chosen); ok && lastnew > r.gLastIndex {
			r.gLastIndex = lastnew
			if pr := r.gtrk.Progress[r.id]; pr != nil {
				if pr.Match < r.gLastIndex {
					pr.Match = r.gLastIndex
				}
				pr.Next = pr.Match + 1
			}
			r.logger.Infof("[G] materialized idx=%d -> gLast=%d", idx, lastnew)
		} else {
			// Already materialized in a racy path or prev mismatch; don't treat as fatal
			r.logger.Infof("[G] materialize skip idx=%d (already present or prev mismatch)", idx)
		}
	}

	r.broadcastGlobalChosen(idx)

	// FAST commit if enough votes and same term
	if r.gtrk.HasFastQuorum(idx) {
		if t, ok := r.glog.termAt(idx); ok && t == r.gTerm {
			r.logger.Infof("[G] FAST COMMIT idx=%d", idx)
			r.glog.commitTo(idx)
			for i := r.gCommit + 1; i <= r.glog.committed; i++ {
				r.applyGlobalCommittedAt(i)
			}
			r.gCommit = r.glog.committed
			r.broadcastGlobalChosen(idx)
			r.bcastGlobalHeartbeat()
			return nil
		}
	}

	r.logger.Infof("[G] CLASSIC replicate idx=%d", idx)

	// Otherwise replicate classically
	r.gtrk.Visit(func(id uint64, _ *tracker.Progress) {
		if id != r.id {
			r.maybeSendGlobalAppend(id, true)
		}
	})
	return nil
}

func (r *raft) handleGlobalAppend(m pb.Message) error {
	// Heartbeat-commit-only messages (no batch) can still advance commit,
	// but we also reflect into r.gCommit and apply — unchanged
	if m.GlobalBatch == nil {
		if m.GlobalCommit > r.glog.committed {
			r.glog.commitTo(m.GlobalCommit)
		}
		if r.glog.committed > r.gCommit {
			old := r.gCommit
			r.gCommit = r.glog.committed
			for i := old + 1; i <= r.gCommit; i++ {
				r.applyGlobalCommittedAt(i)
			}
		}
		if li := r.glog.lastIndex(); li > r.gLastIndex {
			r.gLastIndex = li
		}
		r.send(pb.Message{To: m.From, Type: pb.MsgGlobalAppResp, GlobalIndex: r.glog.lastIndex()})
		return nil
	}

	// we are being asked to accept batch at k := prev+1
	prev := m.GlobalIndex
	k := prev + 1

	// Ensure we first commit a local wrapper for k (global state entry)
	if !r.hasCommittedGlobalStateFor(k) {
		gse := pb.GlobalStateEntry{
			Batch:       m.GlobalBatch,
			GlobalIndex: k,
			GlobalTerm:  m.Term, // global leader term
		}
		data, _ := gse.Marshal()
		e := pb.Entry{Type: pb.EntryNormal, Data: data, IsGlobalState: true}

		if !r.appendEntry(e) {
			r.logger.Warningf("%x: dropped global-state wrapper on fast-prop", r.id)
			return nil
		}

		newLast := r.raftLog.lastIndex()

		if r.pendingGlobalApp == nil {
			r.pendingGlobalApp = make(map[uint64]pb.Message)
		}
		r.pendingGlobalApp[newLast] = m // <-- use actual local index

		// replicate wrapper via normal local path if needed (optional)
		if r.state == StateLeader {
			r.bcastAppend()
		}
		return nil
	}

	// Wrapper is durable locally: proceed to append into the GLOBAL log
	lastnew, ok := r.glog.maybeAppend(prev, m.GlobalLogTerm, m.Term, m.GlobalBatch)
	if !ok {
		hintIdx := min(prev, r.glog.lastIndex())
		hintIdx, hintTerm := r.glog.findConflictByTerm(hintIdx, m.GlobalLogTerm)
		r.send(pb.Message{
			To:            m.From,
			Type:          pb.MsgGlobalAppResp,
			GlobalIndex:   prev,
			Reject:        true,
			RejectHint:    hintIdx,
			GlobalLogTerm: hintTerm,
		})
		return nil
	}

	if lastnew > r.gLastIndex {
		r.gLastIndex = lastnew
	}

	// advance commit if leader's commit moved
	if m.GlobalCommit > r.glog.committed {
		r.glog.commitTo(m.GlobalCommit)
	}
	if r.glog.committed > r.gCommit {
		old := r.gCommit
		r.gCommit = r.glog.committed
		for i := old + 1; i <= r.gCommit; i++ {
			r.applyGlobalCommittedAt(i)
		}
	}
	if lastnew <= r.gCommit {
		r.applyGlobalCommittedAt(lastnew)
	}

	r.send(pb.Message{To: m.From, Type: pb.MsgGlobalAppResp, GlobalIndex: lastnew})
	return nil
}

func (r *raft) handleGlobalAppendResp(m pb.Message) error {
	pr := r.gtrk.Progress[m.From]
	if pr == nil {
		return nil
	}
	pr.RecentActive = true

	if m.Reject {
		// Use tracker logic to step back sensibly based on the hint.
		// This mirrors the local 'MaybeDecrTo' handling in stepLeader.
		if pr.MaybeDecrTo(m.GlobalIndex, m.RejectHint) {
			// If we were replicating, a rejection implies we should probe first.
			if pr.State == tracker.StateReplicate {
				pr.BecomeProbe()
			}
			// Try again (send even if empty to unblock progress).
			r.maybeSendGlobalAppend(m.From, true /* sendIfEmpty */)
		}
		return nil
	}

	// Successful ack: advance and continue replication.
	if pr.MaybeUpdate(m.GlobalIndex) {
		// Try to advance global commit (fast or classic).
		r.maybeGlobalCommit()

		// Send more if possible; drain as much as we can without empty commits.
		for r.maybeSendGlobalAppend(m.From, false /* sendIfEmpty */) {
		}
	}
	return nil
}

func (r *raft) maybeGlobalCommit() bool {
	// FAST commit with term guard
	if fc := r.gtrk.FastCommittable(); fc > r.glog.committed {
		if t, ok := r.glog.termAt(fc); ok && t == r.gTerm {
			r.glog.commitTo(fc)
			for i := r.gCommit + 1; i <= r.glog.committed; i++ {
				r.applyGlobalCommittedAt(i)
			}
			r.gCommit = r.glog.committed
			r.broadcastGlobalChosen(r.gCommit)
			r.bcastGlobalHeartbeat()
			return true
		}
	}
	// classic majority commit (unchanged)
	committed := uint64(r.gtrk.Voters.CommittedIndex(globalMatchAckIndexer{pm: r.gtrk.Progress}))
	if committed > r.glog.committed {
		r.glog.commitTo(committed)
		for i := r.gCommit + 1; i <= r.glog.committed; i++ {
			r.applyGlobalCommittedAt(i)
		}
		r.gCommit = r.glog.committed
		r.broadcastGlobalChosen(r.gCommit)
		r.bcastGlobalHeartbeat()
		return true
	}
	return false
}

// Send one global append (if needed)
func (r *raft) maybeSendGlobalAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.gtrk.Progress[to]
	if pr == nil || pr.IsPaused() {
		return false
	}

	prev := pr.Next - 1
	prevTerm, ok := r.glog.termAt(prev)
	if !ok && prev != 0 {
		// follower too far ahead/behind; send empty to trigger hint
		if !sendIfEmpty {
			return false
		}
		r.send(pb.Message{
			To:            to,
			Type:          pb.MsgGlobalApp,
			GlobalIndex:   prev,
			GlobalLogTerm: 0,
			GlobalCommit:  r.glog.committed,
		})
		return true
	}

	// At global tier we send at most one batch per index
	// If follower needs next batch and we have it, send it.
	next := pr.Next
	ge, ok := r.glog.ents[next]
	var batch *pb.GlobalBatch
	if ok {
		batch = ge.batch
	}

	if batch == nil && !sendIfEmpty {
		return false
	}

	r.send(pb.Message{
		To:            to,
		Type:          pb.MsgGlobalApp,
		GlobalIndex:   prev,
		GlobalLogTerm: prevTerm,
		GlobalBatch:   batch,
		GlobalCommit:  r.glog.committed,
	})
	if batch != nil {
		pr.SentEntries(1, 0) // bytes not tracked here
	}
	pr.SentCommit(r.glog.committed)
	return true
}

// near entryDigest; reuse sha256
func globalBatchDigest(b *pb.GlobalBatch) string {
	if b == nil {
		return ""
	}
	h := sha256.New()
	// stable structural hash
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], b.FirstIndex)
	binary.LittleEndian.PutUint64(buf[8:], b.LastIndex)
	h.Write(buf)
	for i := range b.Entries {
		e := &b.Entries[i]
		h.Write([]byte{byte(e.Type)})
		h.Write(e.Data)
	}
	return string(h.Sum(nil))
}

func (r *raft) rememberGlobalBatch(idx uint64, b *pb.GlobalBatch) {
	if r.globalPropStore == nil {
		r.globalPropStore = make(map[uint64]map[string]*pb.GlobalBatch)
	}
	dig := globalBatchDigest(b)
	m := r.globalPropStore[idx]
	if m == nil {
		m = make(map[string]*pb.GlobalBatch)
		r.globalPropStore[idx] = m
	}
	// store a copy if you plan to mutate
	m[dig] = b
}
func (r *raft) lookupGlobalBatchByDigest(idx uint64, dig string) *pb.GlobalBatch {
	if m := r.globalPropStore[idx]; m != nil {
		return m[dig]
	}
	return nil
}

type gEntry struct {
	term  uint64
	batch *pb.GlobalBatch
	// You can include the digest if you want (not required for correctness)
}

type globalLog struct {
	committed uint64
	last      uint64
	ents      map[uint64]gEntry // sparse, one batch per index
}

func newGlobalLog() *globalLog {
	return &globalLog{ents: make(map[uint64]gEntry)}
}

func (g *globalLog) termAt(i uint64) (uint64, bool) {
	if i == 0 {
		return 0, true
	} // term(0)=0 sentinel
	e, ok := g.ents[i]
	if !ok {
		return 0, false
	}
	return e.term, true
}

func (g *globalLog) match(i, t uint64) bool {
	tt, ok := g.termAt(i)
	if !ok {
		return false
	}
	return tt == t
}

func (g *globalLog) lastIndex() uint64 { return g.last }

func (g *globalLog) commitTo(i uint64) {
	if i > g.last {
		i = g.last
	}
	if i > g.committed {
		g.committed = i
	}
}

// Append at prev+1. Returns lastnew and ok.
func (g *globalLog) maybeAppend(prevIdx, prevTerm uint64, term uint64, b *pb.GlobalBatch) (uint64, bool) {
	if !g.match(prevIdx, prevTerm) {
		return 0, false
	}
	idx := prevIdx + 1
	g.ents[idx] = gEntry{term: term, batch: b}
	if idx > g.last {
		g.last = idx
	}
	return idx, true
}

// follower-side reject hint: max j<=i with term(j)<=t, or best effort
func (g *globalLog) findConflictByTerm(i, t uint64) (uint64, uint64) {
	for j := i; j > 0; j-- {
		tt, ok := g.termAt(j)
		if !ok || tt <= t {
			return j, tt
		}
	}
	return 0, 0
}

type globalMatchAckIndexer struct {
	pm tracker.ProgressMap
}

func (ix globalMatchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	pr, ok := ix.pm[id]
	if !ok || pr.IsLearner {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// applyGlobalConfChange applies a ConfChangeV2 to the GLOBAL tracker.
func (r *raft) applyGlobalConfChange(cc pb.ConfChangeV2) pb.ConfState {
	changer := confchange.Changer{Tracker: r.gtrk, LastIndex: r.glog.lastIndex()}
	cfg, trk, err := func() (tracker.Config, tracker.ProgressMap, error) {
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		}
		if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()
	if err != nil {
		panic(err)
	}

	r.gtrk.Config = cfg
	r.gtrk.Progress = trk

	last := r.glog.lastIndex()

	// RETAIN voters from BOTH sets + learners
	retain := make(map[uint64]struct{})
	for id := range cfg.Voters[0] {
		retain[id] = struct{}{}
	}
	for id := range cfg.Voters[1] {
		retain[id] = struct{}{}
	} // <-- add this
	for id := range cfg.Learners {
		retain[id] = struct{}{}
	}

	for id := range retain {
		pr := r.gtrk.Progress[id]
		if pr == nil {
			pr = &tracker.Progress{
				Next:         last + 1,
				Inflights:    tracker.NewInflights(r.gtrk.MaxInflight, r.gtrk.MaxInflightBytes),
				RecentActive: false,
			}
			r.gtrk.Progress[id] = pr
		} else {
			if pr.Inflights == nil {
				pr.Inflights = tracker.NewInflights(r.gtrk.MaxInflight, r.gtrk.MaxInflightBytes)
			}
			if pr.Next == 0 {
				pr.Next = last + 1
			}
			if pr.Next <= pr.Match {
				pr.Next = pr.Match + 1
			}
		}
		pr.IsLearner = false
		if _, ok := cfg.Learners[id]; ok {
			pr.IsLearner = true
		}
	}
	// prune those not retained
	for id := range r.gtrk.Progress {
		if _, ok := retain[id]; !ok {
			delete(r.gtrk.Progress, id)
		}
	}
	return r.gtrk.ConfState()
}

// applyGlobalBatch scans a committed GlobalBatch and applies any global ConfChange entries.
func (r *raft) applyGlobalBatch(batch *pb.GlobalBatch) {
	if batch == nil {
		return
	}
	for i := range batch.Entries {
		e := batch.Entries[i]
		switch e.Type {
		case pb.EntryConfChangeV2:
			var cc pb.ConfChangeV2
			if err := cc.Unmarshal(e.Data); err != nil {
				r.logger.Warningf("%x: failed to unmarshal global ConfChangeV2: %v", r.id, err)
				continue
			}
			_ = r.applyGlobalConfChange(cc)
		case pb.EntryConfChange:
			var cc pb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				r.logger.Warningf("%x: failed to unmarshal global ConfChange: %v", r.id, err)
				continue
			}
			_ = r.applyGlobalConfChange(cc.AsV2())
		default:
			// EntryNormal: nothing to do here
		}
	}
}

// applyGlobalCommittedAt applies the committed batch at idx (leader and followers).
func (r *raft) applyGlobalCommittedAt(idx uint64) {
	ge, ok := r.glog.ents[idx]
	if !ok || ge.batch == nil {
		return
	}
	r.applyGlobalBatch(ge.batch)
}

// ---------- DEBUG HELPERS ----------
func (r *raft) dbgGlobalProgress() string {
	type row struct {
		st       string
		m, n, fm uint64
		act      bool
		lr       bool
	}
	out := make([]string, 0, len(r.gtrk.Progress)+2)
	out = append(out, fmt.Sprintf("[GPROG] node=%x term=%d gState=%s lead=%x gLast=%d gCommit=%d",
		r.id, r.gTerm, r.gState, r.gLead, r.gLastIndex, r.gCommit))
	// voters
	var ids []uint64
	for id := range r.gtrk.Progress {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		pr := r.gtrk.Progress[id]
		st := "?"
		switch pr.State {
		case tracker.StateProbe:
			st = "Probe"
		case tracker.StateReplicate:
			st = "Repl"
		case tracker.StateSnapshot:
			st = "Snap"
		}
		out = append(out, fmt.Sprintf("  pr id=%x st=%s Match=%d Next=%d Fast=%d act=%v learner=%v",
			id, st, pr.Match, pr.Next, pr.FastMatchIndex, pr.RecentActive, pr.IsLearner))
	}
	// global log keys
	keys := make([]uint64, 0, len(r.glog.ents))
	for k := range r.glog.ents {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out = append(out, fmt.Sprintf("  glog keys=%v", keys))
	return strings.Join(out, "\n")
}

func (r *raft) dbgHasWrapper(gIdx uint64) {
	lo, hi := r.raftLog.firstIndex(), r.raftLog.committed
	found := false
	var pos uint64
	for i := hi; i >= lo; i-- {
		e, err := r.raftLog.entry(i)
		if err == nil && len(e.Data) > 0 {
			var gse pb.GlobalStateEntry
			if gse.Unmarshal(e.Data) == nil && gse.GlobalIndex == gIdx {
				found, pos = true, i
				break
			}
		}
		if i == lo {
			break
		}
	}
	r.logger.Infof("[GDBG] hasWrapper node=%x gIdx=%d => %v at localIdx=%d (applied=%d committed=%d)",
		r.id, gIdx, found, pos, r.raftLog.applied, r.raftLog.committed)
}

// build a majority config for the GLOBAL tier, with a safe fallback
func (r *raft) globalMajorityConfig() quorum.MajorityConfig {
	cfg := quorum.MajorityConfig{}
	ids := r.gtrk.Voters.IDs()
	if len(ids) == 0 {
		// defensive fallback: use non-learner progress entries
		for id, pr := range r.gtrk.Progress {
			if pr != nil && !pr.IsLearner {
				cfg[id] = struct{}{}
			}
		}
	} else {
		for id := range ids {
			cfg[id] = struct{}{}
		}
	}
	return cfg
}
