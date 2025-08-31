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

package quorum

import (
	"fmt"
	"slices"
	"strings"
)

// FastQuorumSize returns the size of a fast quorum (⌈3n/4⌉) for Fast Raft.
// A fast quorum requires agreement from three-quarters of the nodes (rounded up).
// This ensures that any fast quorum intersects with any classic majority quorum.
func FastQuorumSize(n int) int {
	// ceil(3n/4) = (3n + 3) / 4
	return (3*n + 3) / 4
}

// ClassicQuorumSize returns the size of a classic majority quorum (⌈(n+1)/2⌉).
// This is equivalent to n/2 + 1 for standard majority.
func ClassicQuorumSize(n int) int {
	return n/2 + 1
}

// FastVoteCounter tracks votes for different entry digests at a given index.
// It's used by the leader to tally MsgFastVote messages and determine when
// a fast or classic quorum has been reached for a particular entry.
type FastVoteCounter struct {
	// config is the MajorityConfig of voters
	config MajorityConfig
	// votes maps entry digest -> set of voters who voted for that entry
	votes map[string]map[uint64]struct{}
	// voterChoice maps voter -> digest they voted for (for vote changes)
	voterChoice map[uint64]string
}

// NewFastVoteCounter creates a new FastVoteCounter for the given configuration.
func NewFastVoteCounter(cfg MajorityConfig) *FastVoteCounter {
	return &FastVoteCounter{
		config:      cfg,
		votes:       make(map[string]map[uint64]struct{}),
		voterChoice: make(map[uint64]string),
	}
}

// RecordVote records a vote from a voter for an entry digest.
// If the voter previously voted for a different digest, their vote is moved.
// Returns true if this is a new vote (not a duplicate).
func (fvc *FastVoteCounter) RecordVote(voterID uint64, digest string) bool {
	// Check if voter is in the configuration
	if _, ok := fvc.config[voterID]; !ok {
		return false
	}

	// Check if voter already voted for this exact digest
	if prevDigest, voted := fvc.voterChoice[voterID]; voted {
		if prevDigest == digest {
			return false // duplicate vote
		}
		// Remove previous vote
		delete(fvc.votes[prevDigest], voterID)
		if len(fvc.votes[prevDigest]) == 0 {
			delete(fvc.votes, prevDigest)
		}
	}

	// Record new vote
	if fvc.votes[digest] == nil {
		fvc.votes[digest] = make(map[uint64]struct{})
	}
	fvc.votes[digest][voterID] = struct{}{}
	fvc.voterChoice[voterID] = digest

	return true
}

// TallyResult represents the outcome of a vote tally at a specific index.
type TallyResult struct {
	// LeadingDigest is the digest with the most votes
	LeadingDigest string
	// LeadingCount is the number of votes for the leading digest
	LeadingCount int
	// HasClassicQuorum indicates if the leading digest has a classic majority
	HasClassicQuorum bool
	// HasFastQuorum indicates if the leading digest has a fast quorum
	HasFastQuorum bool
	// Voters who voted for the leading digest
	LeadingVoters []uint64
}

// Tally computes the current vote tally and determines if any entry has reached
// a quorum (fast or classic).
func (fvc *FastVoteCounter) Tally() TallyResult {
	n := len(fvc.config)
	if n == 0 {
		return TallyResult{}
	}

	classicQuorum := ClassicQuorumSize(n)
	fastQuorum := FastQuorumSize(n)

	var result TallyResult

	// Find the digest with the most votes
	for digest, voters := range fvc.votes {
		count := len(voters)
		if count > result.LeadingCount {
			result.LeadingDigest = digest
			result.LeadingCount = count

			// Convert voters to slice
			result.LeadingVoters = make([]uint64, 0, count)
			for v := range voters {
				result.LeadingVoters = append(result.LeadingVoters, v)
			}
			slices.Sort(result.LeadingVoters)
		}
	}

	// Check quorum conditions
	if result.LeadingCount >= classicQuorum {
		result.HasClassicQuorum = true
	}
	if result.LeadingCount >= fastQuorum {
		result.HasFastQuorum = true
	}

	return result
}

// VoteCount returns the number of votes for a specific digest.
func (fvc *FastVoteCounter) VoteCount(digest string) int {
	return len(fvc.votes[digest])
}

// Reset clears all recorded votes.
func (fvc *FastVoteCounter) Reset() {
	fvc.votes = make(map[string]map[uint64]struct{})
	fvc.voterChoice = make(map[uint64]string)
}

// String returns a string representation of the current vote state.
func (fvc *FastVoteCounter) String() string {
	var buf strings.Builder

	n := len(fvc.config)
	fmt.Fprintf(&buf, "FastVoteCounter(n=%d, classic=%d, fast=%d):\n",
		n, ClassicQuorumSize(n), FastQuorumSize(n))

	// Sort digests for consistent output
	type digestVotes struct {
		digest string
		count  int
		voters []uint64
	}

	var sorted []digestVotes
	for digest, voters := range fvc.votes {
		var voterList []uint64
		for v := range voters {
			voterList = append(voterList, v)
		}
		slices.Sort(voterList)
		sorted = append(sorted, digestVotes{
			digest: digest,
			count:  len(voters),
			voters: voterList,
		})
	}

	// Sort by count (descending), then by digest
	slices.SortFunc(sorted, func(a, b digestVotes) int {
		if a.count != b.count {
			return b.count - a.count // descending
		}
		return strings.Compare(a.digest, b.digest)
	})

	for _, dv := range sorted {
		fmt.Fprintf(&buf, "  %s: %d votes %v\n",
			dv.digest[:min(8, len(dv.digest))], dv.count, dv.voters)
	}

	return buf.String()
}

// min returns the minimum of two integers (until we have min built-in in older Go versions).
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
