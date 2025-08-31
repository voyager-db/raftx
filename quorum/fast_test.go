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
	"testing"
)

func TestFastQuorumSize(t *testing.T) {
	tests := []struct {
		n            int
		wantClassic  int
		wantFast     int
	}{
		{1, 1, 1},   // 1 node: classic=1, fast=ceil(3/4)=1
		{2, 2, 2},   // 2 nodes: classic=2, fast=ceil(6/4)=2
		{3, 2, 3},   // 3 nodes: classic=2, fast=ceil(9/4)=3
		{4, 3, 3},   // 4 nodes: classic=3, fast=ceil(12/4)=3
		{5, 3, 4},   // 5 nodes: classic=3, fast=ceil(15/4)=4
		{6, 4, 5},   // 6 nodes: classic=4, fast=ceil(18/4)=5
		{7, 4, 6},   // 7 nodes: classic=4, fast=ceil(21/4)=6
		{8, 5, 6},   // 8 nodes: classic=5, fast=ceil(24/4)=6
		{9, 5, 7},   // 9 nodes: classic=5, fast=ceil(27/4)=7
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d", tt.n), func(t *testing.T) {
			gotClassic := ClassicQuorumSize(tt.n)
			if gotClassic != tt.wantClassic {
				t.Errorf("ClassicQuorumSize(%d) = %d, want %d", tt.n, gotClassic, tt.wantClassic)
			}

			gotFast := FastQuorumSize(tt.n)
			if gotFast != tt.wantFast {
				t.Errorf("FastQuorumSize(%d) = %d, want %d", tt.n, gotFast, tt.wantFast)
			}

			// Verify that fast quorum is always >= classic quorum
			if gotFast < gotClassic {
				t.Errorf("FastQuorum(%d) < ClassicQuorum(%d) for n=%d", gotFast, gotClassic, tt.n)
			}

			// Verify intersection property: fast quorum + (n - classic quorum + 1) > n
			// This ensures any fast quorum intersects with any classic quorum
			if gotFast+(tt.n-gotClassic+1) <= tt.n {
				t.Errorf("Intersection property violated for n=%d", tt.n)
			}
		})
	}
}

func TestFastVoteCounter(t *testing.T) {
	// Create a 5-node configuration
	cfg := MajorityConfig{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {},
	}

	t.Run("BasicVoting", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		// Record votes for digest "A"
		if !fvc.RecordVote(1, "A") {
			t.Error("Expected new vote to return true")
		}
		if !fvc.RecordVote(2, "A") {
			t.Error("Expected new vote to return true")
		}
		if !fvc.RecordVote(3, "A") {
			t.Error("Expected new vote to return true")
		}

		// Duplicate vote should return false
		if fvc.RecordVote(1, "A") {
			t.Error("Expected duplicate vote to return false")
		}

		tally := fvc.Tally()
		if tally.LeadingDigest != "A" {
			t.Errorf("Expected leading digest 'A', got %q", tally.LeadingDigest)
		}
		if tally.LeadingCount != 3 {
			t.Errorf("Expected 3 votes for 'A', got %d", tally.LeadingCount)
		}
		if !tally.HasClassicQuorum {
			t.Error("Expected classic quorum with 3/5 votes")
		}
		if tally.HasFastQuorum {
			t.Error("Should not have fast quorum with only 3/5 votes")
		}
	})

	t.Run("FastQuorum", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		// Record 4 votes for digest "B" (fast quorum for n=5)
		fvc.RecordVote(1, "B")
		fvc.RecordVote(2, "B")
		fvc.RecordVote(3, "B")
		fvc.RecordVote(4, "B")

		tally := fvc.Tally()
		if !tally.HasClassicQuorum {
			t.Error("Expected classic quorum with 4/5 votes")
		}
		if !tally.HasFastQuorum {
			t.Error("Expected fast quorum with 4/5 votes")
		}
	})

	t.Run("VoteChange", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		// Node 1 votes for "A"
		fvc.RecordVote(1, "A")
		if fvc.VoteCount("A") != 1 {
			t.Errorf("Expected 1 vote for 'A', got %d", fvc.VoteCount("A"))
		}

		// Node 1 changes vote to "B"
		fvc.RecordVote(1, "B")
		if fvc.VoteCount("A") != 0 {
			t.Errorf("Expected 0 votes for 'A' after vote change, got %d", fvc.VoteCount("A"))
		}
		if fvc.VoteCount("B") != 1 {
			t.Errorf("Expected 1 vote for 'B', got %d", fvc.VoteCount("B"))
		}
	})

	t.Run("Contention", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		// Split votes between two digests
		fvc.RecordVote(1, "A")
		fvc.RecordVote(2, "A")
		fvc.RecordVote(3, "B")
		fvc.RecordVote(4, "B")
		fvc.RecordVote(5, "B")

		tally := fvc.Tally()
		if tally.LeadingDigest != "B" {
			t.Errorf("Expected 'B' to lead with 3 votes, got %q", tally.LeadingDigest)
		}
		if tally.LeadingCount != 3 {
			t.Errorf("Expected 3 votes for leading digest, got %d", tally.LeadingCount)
		}
		if !tally.HasClassicQuorum {
			t.Error("Expected classic quorum for 'B' with 3/5 votes")
		}
		if tally.HasFastQuorum {
			t.Error("Should not have fast quorum with only 3/5 votes")
		}
	})

	t.Run("InvalidVoter", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		// Vote from non-existent node should be ignored
		if fvc.RecordVote(99, "A") {
			t.Error("Expected vote from non-member to return false")
		}

		tally := fvc.Tally()
		if tally.LeadingCount != 0 {
			t.Errorf("Expected no votes, got %d", tally.LeadingCount)
		}
	})

	t.Run("Reset", func(t *testing.T) {
		fvc := NewFastVoteCounter(cfg)

		fvc.RecordVote(1, "A")
		fvc.RecordVote(2, "A")
		
		fvc.Reset()
		
		tally := fvc.Tally()
		if tally.LeadingCount != 0 {
			t.Errorf("Expected no votes after reset, got %d", tally.LeadingCount)
		}
	})
}

func TestFastVoteCounterString(t *testing.T) {
	cfg := MajorityConfig{1: {}, 2: {}, 3: {}}
	fvc := NewFastVoteCounter(cfg)

	fvc.RecordVote(1, "digest-AAA")
	fvc.RecordVote(2, "digest-AAA")
	fvc.RecordVote(3, "digest-BBB")

	str := fvc.String()
	
	// Check that important information is present
	if !contains(str, "n=3") {
		t.Error("Expected string to contain cluster size")
	}
	if !contains(str, "classic=2") {
		t.Error("Expected string to contain classic quorum size")
	}
	if !contains(str, "fast=3") {
		t.Error("Expected string to contain fast quorum size")
	}
	if !contains(str, "digest-A") {
		t.Error("Expected string to contain digest prefix")
	}
	if !contains(str, "2 votes") {
		t.Error("Expected string to contain vote count")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && stringContains(s, substr)
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// BenchmarkFastVoteCounter benchmarks vote recording and tallying
func BenchmarkFastVoteCounter(b *testing.B) {
	sizes := []int{3, 5, 7, 9}
	
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			cfg := make(MajorityConfig)
			for i := uint64(1); i <= uint64(n); i++ {
				cfg[i] = struct{}{}
			}
			
			fvc := NewFastVoteCounter(cfg)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Simulate voting pattern
				voterID := uint64((i % n) + 1)
				digest := fmt.Sprintf("digest-%d", i%3)
				fvc.RecordVote(voterID, digest)
				
				if i%n == 0 {
					fvc.Tally()
				}
			}
		})
	}
}