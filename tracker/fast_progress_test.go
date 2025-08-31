// Additional tests for Fast Raft quorum and tracker functionality

// In quorum/fast_raft_test.go (already created earlier)
// These tests are already in the quorum package

// In tracker/progress_test.go - add these tests
package tracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestProgressFastMatch tests FastMatch tracking
func TestProgressFastMatch(t *testing.T) {
	pr := &Progress{
		Match:          5,
		Next:           6,
		FastMatchIndex: 0,
		Inflights:      NewInflights(10, 0),
	}

	// Update fast match
	pr.UpdateFastMatch(3)
	assert.Equal(t, uint64(3), pr.FastMatchIndex)

	// FastMatch CAN exceed Match in Fast Raft
	// This is the whole point - a node can vote for an entry before replicating it
	pr.UpdateFastMatch(10)
	assert.Equal(t, uint64(10), pr.FastMatchIndex, "FastMatch can exceed Match in Fast Raft")

	// Test that FastMatch only increases
	pr.UpdateFastMatch(8)
	assert.Equal(t, uint64(10), pr.FastMatchIndex, "FastMatch should not decrease")

	// Reset fast match
	pr.ResetFastMatch()
	assert.Equal(t, uint64(0), pr.FastMatchIndex)
}

func TestProgressTrackerFastCommittable(t *testing.T) {
	tests := []struct {
		name           string
		clusterSize    int
		fastMatches    map[uint64]uint64 // node -> fastMatch
		expectedCommit uint64
	}{
		{
			name:        "3-node fast commit",
			clusterSize: 3,
			fastMatches: map[uint64]uint64{
				1: 5,
				2: 5,
				3: 5,
			},
			expectedCommit: 5, // All 3 nodes = fast quorum for n=3
		},
		{
			name:        "5-node fast commit",
			clusterSize: 5,
			fastMatches: map[uint64]uint64{
				1: 10,
				2: 10,
				3: 10,
				4: 10,
				5: 8,
			},
			expectedCommit: 10, // 4 nodes at 10 = fast quorum for n=5
		},
		{
			name:        "5-node partial fast commit",
			clusterSize: 5,
			fastMatches: map[uint64]uint64{
				1: 10,
				2: 10,
				3: 10,
				4: 5,
				5: 5,
			},
			expectedCommit: 5, // All 5 nodes have voted for at least index 5
			// Note: Only 3 nodes reached index 10, but all 5 reached index 5
			// So index 5 can be fast-committed
		},
		{
			name:        "5-node no fast commit",
			clusterSize: 5,
			fastMatches: map[uint64]uint64{
				1: 10,
				2: 10,
				3: 10,
				4: 0, // Changed: Node hasn't voted
				5: 0, // Changed: Node hasn't voted
			},
			expectedCommit: 0, // Only 3 nodes voted, need 4 for fast quorum
		},
		{
			name:        "7-node fast commit",
			clusterSize: 7,
			fastMatches: map[uint64]uint64{
				1: 15,
				2: 15,
				3: 15,
				4: 15,
				5: 15,
				6: 15,
				7: 10,
			},
			expectedCommit: 15, // 6 nodes = fast quorum for n=7
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := MakeProgressTrackerWithFastPath(10, 0, true)

			// Set up cluster
			for i := uint64(1); i <= uint64(tt.clusterSize); i++ {
				tracker.Progress[i] = &Progress{
					Match:          tt.fastMatches[i],
					FastMatchIndex: tt.fastMatches[i],
					Next:           tt.fastMatches[i] + 1,
					Inflights:      NewInflights(10, 0),
				}
				tracker.Config.Voters[0][i] = struct{}{}
			}

			fc := tracker.FastCommittable()
			assert.Equal(t, tt.expectedCommit, fc,
				"FastCommittable should return %d for %s", tt.expectedCommit, tt.name)
		})
	}
}

func TestProgressTrackerHasFastQuorum(t *testing.T) {
	tr := MakeProgressTrackerWithFastPath(10, 0, true)

	// Set up a 5-node single-config cluster.
	for i := uint64(1); i <= 5; i++ {
		tr.Progress[i] = &Progress{
			Match:          0,
			FastMatchIndex: 0,
			Next:           1,
			Inflights:      NewInflights(10, 0),
		}
		tr.Config.Voters[0][i] = struct{}{}
	}

	// No fast quorum initially at index 10.
	assert.False(t, tr.HasFastQuorum(10))

	// Simulate the leader having decided an entry at index 10 and having
	// bumped FastMatchIndex for the winning voters post-decision.
	// For n=5, fast quorum = ceil(3n/4) = 4.
	tr.Progress[1].FastMatchIndex = 10
	assert.False(t, tr.HasFastQuorum(10))

	tr.Progress[2].FastMatchIndex = 10
	assert.False(t, tr.HasFastQuorum(10))

	tr.Progress[3].FastMatchIndex = 10
	assert.False(t, tr.HasFastQuorum(10))

	// Fourth voter reaches the threshold: fast quorum achieved at index 10.
	tr.Progress[4].FastMatchIndex = 10
	assert.True(t, tr.HasFastQuorum(10))

	// Lower indices without enough votes should not have quorum.
	assert.False(t, tr.HasFastQuorum(11))
}


// TestProgressTrackerResetFastMatches tests resetting fast matches
func TestProgressTrackerResetFastMatches(t *testing.T) {
	tracker := MakeProgressTrackerWithFastPath(10, 0, true)

	// Set up cluster with fast matches
	for i := uint64(1); i <= 3; i++ {
		tracker.Progress[i] = &Progress{
			Match:          10,
			FastMatchIndex: 8,
			Next:           11,
			Inflights:      NewInflights(10, 0),
		}
	}

	// Reset all fast matches
	tracker.ResetFastMatches()

	// Verify all reset
	tracker.Visit(func(id uint64, pr *Progress) {
		assert.Equal(t, uint64(0), pr.FastMatchIndex)
		assert.Equal(t, uint64(10), pr.Match, "Match should not be affected")
	})
}

// TestProgressTrackerDisabledFastPath tests that fast path can be disabled
func TestProgressTrackerDisabledFastPath(t *testing.T) {
	tracker := MakeProgressTracker(10, 0) // Fast path disabled by default

	// Set up cluster
	for i := uint64(1); i <= 3; i++ {
		tracker.Progress[i] = &Progress{
			Match:          10,
			FastMatchIndex: 10,
			Next:           11,
			Inflights:      NewInflights(10, 0),
		}
		tracker.Config.Voters[0][i] = struct{}{}
	}

	// Fast committable should return 0 when disabled
	assert.Equal(t, uint64(0), tracker.FastCommittable())
	assert.False(t, tracker.HasFastQuorum(10))
}
