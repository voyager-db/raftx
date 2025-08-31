// Copyright 2015 The etcd Authors
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

import pb "go.etcd.io/raft/v3/raftpb"

// unstable contains "unstable" log entries and snapshot state that has
// not yet been written to Storage. The type serves two roles. First, it
// holds on to new log entries and an optional snapshot until they are
// handed to a Ready struct for persistence. Second, it continues to
// hold on to this state after it has been handed off to provide raftLog
// with a view of the in-progress log entries and snapshot until their
// writes have been stabilized and are guaranteed to be reflected in
// queries of Storage. After this point, the corresponding log entries
// and/or snapshot can be cleared from unstable.
//
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	// entries[i] has raft log position i+offset.
	offset uint64

	// if true, snapshot is being written to storage.
	snapshotInProgress bool
	// entries[:offsetInProgress-offset] are being written to storage.
	// Like offset, offsetInProgress is exclusive, meaning that it
	// contains the index following the largest in-progress entry.
	// Invariant: offset <= offsetInProgress
	offsetInProgress uint64

	logger Logger

	// === FAST RAFT ADDITIONS ===
	// sparseEntries holds entries that don't fit contiguously in entries slice
	// This allows gaps in the log for Fast Raft's sparse insertion
	sparseEntries map[uint64]*pb.Entry
}

// === NEW METHOD FOR FAST RAFT ===
// sparseInsert adds an entry at a specific index, growing the slice and
// filling holes as needed. It never panics on gaps.
func (u *unstable) sparseInsert(e pb.Entry) {
	// Ignore entries that are older than our current window (compacted away).
	if e.Index < u.offset {
		return
	}

	pos := int(e.Index - u.offset)
	switch {
	case pos == len(u.entries):
		// append contiguously
		u.entries = append(u.entries, e)
	case pos < len(u.entries):
		// overwrite within existing range
		u.entries[pos] = e
	default:
		// grow and fill holes with zero Entries
		gap := pos - len(u.entries)
		u.entries = append(u.entries, make([]pb.Entry, gap)...)
		u.entries = append(u.entries, e)
	}
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// === MODIFIED maybeLastIndex - consider sparse entries ===
func (u *unstable) maybeLastIndex() (uint64, bool) {
	// Find max from sparse entries
	var maxSparse uint64
	haveSparse := false
	if u.sparseEntries != nil {
		for idx := range u.sparseEntries {
			if !haveSparse || idx > maxSparse {
				maxSparse = idx
				haveSparse = true
			}
		}
	}

	// Check regular entries
	if l := len(u.entries); l != 0 {
		regularLast := u.offset + uint64(l) - 1
		if haveSparse && maxSparse > regularLast {
			return maxSparse, true
		}
		return regularLast, true
	}

	// Check snapshot
	if u.snapshot != nil {
		if haveSparse && maxSparse > u.snapshot.Metadata.Index {
			return maxSparse, true
		}
		return u.snapshot.Metadata.Index, true
	}

	// Only sparse entries
	if haveSparse {
		return maxSparse, true
	}

	return 0, false
}

// === MODIFIED maybeTerm - add sparse entry check ===
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	// Check regular entries first
	if i < u.offset+uint64(len(u.entries)) {
		return u.entries[i-u.offset].Term, true
	}

	// FAST RAFT: Check sparse entries
	if u.sparseEntries != nil {
		if e, ok := u.sparseEntries[i]; ok {
			return e.Term, true
		}
	}

	return 0, false
}

// nextEntries returns the unstable entries that are not already in the process
// of being written to storage.
func (u *unstable) nextEntries() []pb.Entry {
	inProgress := int(u.offsetInProgress - u.offset)
	if len(u.entries) == inProgress {
		return nil
	}
	return u.entries[inProgress:]
}

// nextSnapshot returns the unstable snapshot, if one exists that is not already
// in the process of being written to storage.
func (u *unstable) nextSnapshot() *pb.Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// acceptInProgress marks all entries and the snapshot, if any, in the unstable
// as having begun the process of being written to storage. The entries/snapshot
// will no longer be returned from nextEntries/nextSnapshot. However, new
// entries/snapshots added after a call to acceptInProgress will be returned
// from those methods, until the next call to acceptInProgress.
func (u *unstable) acceptInProgress() {
	if len(u.entries) > 0 {
		// NOTE: +1 because offsetInProgress is exclusive, like offset.
		u.offsetInProgress = u.entries[len(u.entries)-1].Index + 1
	}
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

// === MODIFIED stableTo - clean up sparse entries ===
func (u *unstable) stableTo(id entryID) {
	gt, ok := u.maybeTerm(id.index)
	if !ok {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", id.index)
		return
	}
	if id.index < u.offset {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", id.index)
		return
	}
	if gt != id.term {
		// Term mismatch between unstable entry and specified entry. Ignore.
		u.logger.Infof("entry at (index,term)=(%d,%d) mismatched with "+
			"entry at (%d,%d) in unstable log; ignoring", id.index, id.term, id.index, gt)
		return
	}

	// Original logic for regular entries
	if id.index < u.offset+uint64(len(u.entries)) {
		num := int(id.index + 1 - u.offset)
		u.entries = u.entries[num:]
		u.offset = id.index + 1
		u.offsetInProgress = max(u.offsetInProgress, u.offset)
		u.shrinkEntriesArray()
	}

	// FAST RAFT: Clean up stable sparse entries
	if u.sparseEntries != nil {
		for idx := range u.sparseEntries {
			if idx <= id.index {
				delete(u.sparseEntries, idx)
			}
		}
		if len(u.sparseEntries) == 0 {
			u.sparseEntries = nil
		}
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.offsetInProgress = u.offset
	u.entries = nil
	u.snapshot = &s
	u.snapshotInProgress = false
}

func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	fromIndex := ents[0].Index
	switch {
	case fromIndex == u.offset+uint64(len(u.entries)):
		// fromIndex is the next index in the u.entries, so append directly.
		u.entries = append(u.entries, ents...)
	case fromIndex <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", fromIndex)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries.
		u.entries = ents
		u.offset = fromIndex
		u.offsetInProgress = u.offset
	default:
		// Truncate to fromIndex (exclusive), and append the new entries.
		u.logger.Infof("truncate the unstable entries before index %d", fromIndex)
		keep := u.slice(u.offset, fromIndex) // NB: appending to this slice is safe,
		u.entries = append(keep, ents...)    // and will reallocate/copy it
		// Only in-progress entries before fromIndex are still considered to be
		// in-progress.
		u.offsetInProgress = min(u.offsetInProgress, fromIndex)
	}
}

// === MODIFIED slice - include sparse entries ===
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)

	// Start with regular entries
	result := u.entries[lo-u.offset : hi-u.offset : hi-u.offset]

	// FAST RAFT: Check if we need to overlay sparse entries
	if u.sparseEntries != nil {
		// We need to create a new slice if there are sparse entries in range
		var hasSparseInRange bool
		for idx := range u.sparseEntries {
			if idx >= lo && idx < hi {
				hasSparseInRange = true
				break
			}
		}

		if hasSparseInRange {
			// Create new slice with sparse entries overlaid
			newResult := make([]pb.Entry, len(result))
			copy(newResult, result)

			for idx, entry := range u.sparseEntries {
				if idx >= lo && idx < hi {
					newResult[idx-lo] = *entry
				}
			}
			return newResult
		}
	}

	return result
}

// === MODIFIED mustCheckOutOfBounds - allow sparse entries beyond regular range ===
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}

	// FAST RAFT: Check both regular and sparse bounds
	upper := u.offset + uint64(len(u.entries))

	// Also check sparse entries for upper bound
	if u.sparseEntries != nil {
		for idx := range u.sparseEntries {
			if idx >= upper {
				upper = idx + 1
			}
		}
	}

	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}

// === NEW METHOD - attempt to compact sparse entries into regular entries ===
func (u *unstable) compactSparse() {
	if u.sparseEntries == nil || len(u.sparseEntries) == 0 {
		return
	}

	// Try to extend entries with any contiguous sparse entries
	nextIdx := u.offset + uint64(len(u.entries))
	for {
		if e, ok := u.sparseEntries[nextIdx]; ok {
			u.entries = append(u.entries, *e)
			delete(u.sparseEntries, nextIdx)
			nextIdx++
		} else {
			break
		}
	}

	if len(u.sparseEntries) == 0 {
		u.sparseEntries = nil
	}
}
