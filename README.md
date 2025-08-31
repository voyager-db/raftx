# Raft + Fast Raft Library

This library implements the Raft consensus protocol and extends it with Fast Raft: an optimized path that reduces the number of message rounds needed to commit an entry under no contention.

Like upstream etcd/raft, this library implements only the core Raft algorithm and leaves transport, storage, and application integration to the user. The codebase is deterministic, minimal, and designed to be embedded into larger systems.

## Background

Raft is a protocol with which a cluster of nodes can maintain a replicated state machine. The state machine is kept in sync through the use of a replicated log.

In classic Raft, every proposal must flow through the leader: proposer → leader → followers → commit.  
In Fast Raft, the proposer broadcasts directly to all peers, which speculatively insert the entry as self-approved and send votes back to the leader. The leader tallies votes for each index:

- If a fast quorum agrees (⌈3n/4⌉ for cluster size n), the leader can fast-commit in 2 rounds: proposer→all, leader→all.
- If only a classic quorum agrees (⌈n/2⌉), the leader falls back to the classic Raft track, appending the chosen entry as leader-approved and replicating it.

This preserves Raft’s safety and liveness guarantees, while reducing latency in the uncontended case.

## Features

This library includes all features from upstream etcd/raft:

- Leader election  
- Log replication  
- Log compaction  
- Membership changes  
- Leadership transfer  
- Linearizable read-only queries (safe and lease-based)  
- Flow control, batching, parallel leader disk writes, conf-change safety  

With the following Fast Raft extensions:

- **Fast path log insertion (MsgFastProp, MsgFastVote)**  
- Followers insert speculative self-approved entries without truncating  
- Leader tallies votes in `possibleEntries` and upgrades the chosen entry  
- **Fast quorum commit rule**: If ⌈3n/4⌉ nodes (including leader) agree on an entry, it can be committed immediately  
- **Classic fallback path**: If only ⌈n/2⌉ nodes agree, leader inserts the chosen entry as leader-approved and continues via normal AppendEntries  
- **Overwrite rule at chosen index**: Followers overwrite self-approved entries with the leader’s chosen entry once decided  
- **Election recovery**: During leader election, nodes include self-approved hints in MsgVoteResp  
- **Side-buffer flush**: Followers can buffer out-of-order fast proposals and flush them into the log once the prefix arrives  

## Usage

Usage is identical to etcd/raft. Create a Node with `raft.StartNode` or `raft.RestartNode`, wire up storage and transport, and drive the state machine loop with `Tick` and `Step`.

To enable Fast Raft in a node:

```go
cfg := &raft.Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         raft.NewMemoryStorage(),
    MaxSizePerMsg:   4096,
    MaxInflightMsgs: 256,
    EnableFastPath:  true, // <-- enable Fast Raft
}
n := raft.StartNode(cfg, peers)
```

## Messages and Log Entries

Messages and log entries are the same as in classic Raft, with two additions:

- **MsgFastProp** – proposer broadcasts entries directly to peers  
- **MsgFastVote** – follower sends vote for a self-approved entry back to leader  

If `EnableFastPath=false`, the library behaves exactly like upstream etcd/raft.

## Implementation Details

### insertedBy flag
Each log entry tracks its origin:

- **InsertedBySelf**: entry speculatively inserted from a fast proposal  
- **InsertedByLeader**: entry inserted/confirmed by the leader  
- **InsertedByUnknown**: default (classic Raft)  

### Log structure
- Follower uses `sparseAppend` to insert entries at arbitrary indexes ≥ committed+1, leaving gaps  
- Leader never truncates speculative entries, but overwrites at the chosen index  

### Decision flow
- `handleFastProp`: follower inserts entry, votes for it  
- `handleFastVote`: leader records vote, tallies at index k  
- `tryDecideAt`: if quorum, upgrades chosen entry and replicates  
- `maybeAppend`: follower overwrites self-approved entry with leader-approved choice  

### Recovery flow
- Voters piggyback self-approved hints on `MsgVoteResp`  
- New leader seeds `possibleEntries` from hints  
- Any entry that could have been chosen is preserved  

## Example

Fast Raft reduces the latency for uncontended proposals. In a 5-node cluster:

- **Classic Raft**: proposer→leader→followers→leader commit → broadcast commit (~2 RTT)  
- **Fast Raft**: proposer→all, followers→leader votes, leader commit → broadcast commit (~1 RTT faster)  

## Status

This library is experimental. While it passes a suite of correctness tests (safety, election recovery, overwrite rules), further benchmarking and production hardening are in progress.

## Go docs

API docs are mostly identical to etcd/raft: [pkg.go.dev/go.etcd.io/raft/v3](https://pkg.go.dev/go.etcd.io/raft/v3)
