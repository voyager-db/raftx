![Project banner](craft.png)

# Raft + Fast Raft Library + C-Raft (Hierarchical)

This library is a drop-in Raft core with two opt-in accelerators:
- **Fast Raft**: a 2-round (under no contention) fast path using direct proposer→all broadcasts and follower votes.
- **C-Raft**: a *two-level* consensus: Fast Raft inside each local cluster *and* across cluster leaders for a global log.

Like upstream etcd/raft, this library implements only the core Raft algorithm and leaves transport, storage, and application integration to the user. The codebase is deterministic, minimal, and designed to be embedded into larger systems.

## Why Fast Raft?

Classic Raft: proposer → leader → followers → leader commits → broadcast (≈2 RTT).  
**Fast Raft**: proposer → **all**, followers speculatively insert & **vote** → leader commits → broadcast (≈1 RTT faster when uncontended).

- Followers **self-approve** speculative entries (no truncation).
- Leader tallies per-index votes:
  - **Fast commit** when ⌈3n/4⌉ (fast quorum) agree in the **same term**.
  - **Classic fallback** when only ⌈n/2⌉ agree: leader materializes the chosen entry and replicates via AppendEntries.
- Safety/liveness equal to classic Raft.

## Why C-Raft?

C-Raft composes Fast Raft **twice**:
- **Local tier:** in-cluster Fast Raft for each shard/region.
- **Global tier:** cluster leaders run Fast Raft among themselves to build a **global log** of **batches** of local commits.

Key invariant: every global proposal is first recorded as a **Global State Entry** (wrapper) in the local cluster and **committed locally** before voting/acceptance globally. This guarantees recovery if a local leader fails mid-decision.

## Features

This library preserves the full etcd/raft feature set and adds opt-in accelerators for **Fast Raft** (local tier) and **C-Raft** (global tier). Your application loop (Ready → persist → send → apply → Advance; Step; Tick) is unchanged.

### Classic Raft (baseline, unchanged API)
- Leader election
- Log replication
- Log compaction / snapshots
- Membership changes (joint config safety)
- Leadership transfer
- Linearizable read-only queries (safe / lease-based)
- Flow control, batching, parallel leader disk writes
- Proven persistence & ordering invariants (Entries → HardState/Snapshot; message send after HS)

### Fast Raft (local tier; `EnableFastPath=true`)
- **Fast path messages:** `MsgFastProp` (proposer→all), `MsgFastVote` (follower→leader).
- **Speculative insertion:** followers **sparse-append** self-approved entries (no truncation of gaps).
- **Per-index tally:** leader tallies votes; on **classic quorum** it **materializes** the chosen entry at that index and replicates; on **fast quorum** (⌈3n/4⌉) and **same term**, it **fast-commits**.
- **Classic fallback path:** if only ⌈n/2⌉ agree, leader inserts the chosen entry as leader-approved and continues via AppendEntries.
- **Overwrite rule at the chosen index:** followers overwrite any self-approved entry at the decided index with the leader’s entry.
- **Out-of-order buffering:** followers may buffer fast proposals and flush them once the prefix arrives.
- **Election recovery:** nodes piggyback self-approved hints during elections so a new leader can seed decisions safely.
- **Safety guards:** one vote per term; fast-commit requires **same-term**; classic invariants retained.

### C-Raft (two-level consensus; `EnableCRaft=true`)
- **Two tiers:** Fast Raft inside each **local** cluster **and** across **cluster leaders** to build a **global log**.
- **Global batches:** leaders package contiguous **locally committed** entries as `GlobalBatch{Entries, FirstIndex, LastIndex, Digest}` (leader no-ops filtered; stable digest).
- **Wrapper-first invariant:** before voting/accepting globally, each cluster leader proposes a **Global State Entry** (wrapper) to its **local** cluster and waits for it to **commit**. Guarantees recovery.
- **Global fast path:** `MsgGlobalFastProp` / `MsgGlobalFastVote`; leader **materializes** on **classic quorum**, **fast-commits** on **fast quorum + same global term**; otherwise **classic** replication via `MsgGlobalApp`.
- **Global election:** `MsgGlobalVote/Resp`; one vote per global term; up-to-date check uses the last **leader-approved** global entry.
- **Recovery:** a new global leader scans its local log for the latest **committed wrapper** and **rebroadcasts** the in-flight decision.
- **Payload replication guaranteed:** leader **sends the chosen batch** (not just a heartbeat) after materialization/commit; followers install the batch and advance `gCommit`.
- **Membership changes at global tier:** joint config safety; both voter sets retained during transition.
- **Defensive fallback:** if global voter config is not seeded, counters fall back to non-learner `Progress` to form a sensible electorate (for materialization); commit still requires a real voter config.

### Operational characteristics
- **Loop & API unchanged:** Ready/Step/Tick/Advance semantics remain the same.
- **Opt-in & backward compatible:** disabling FastPath/CRaft reverts behavior to classic etcd/raft.
- **Deterministic core:** transport, disk, and SM integration are up to you; the library enforces classic persistence and ordering invariants.

## Install

```bash
go get github.com/voyager-db/raftx
```

## Usage

Usage is identical to etcd/raft. Create a Node with `raft.StartNode` or `raft.RestartNode`, wire up storage and transport, and drive the state machine loop with `Tick` and `Step`.

To enable Fast Raft in a node:

```go
storage := raft.NewMemoryStorage()
cfg := &raft.Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         raft.NewMemoryStorage(),
    MaxSizePerMsg:   4096,
    MaxInflightMsgs: 256,

    // Opt-ins:
    EnableFastPath:        true,  // Fast Raft
    EnableCRaft:           true,  // C-Raft global tier
    GlobalElectionTick:    10,
    GlobalHeartbeatTick:   1,
    // Optional: seed initial global voters (cluster leader IDs)
    GlobalVoters:          []uint64{/* 0xA, 0xB, 0xC */},
}
// Start fresh or from storage; API and loop are identical to etcd/raft.
n := raft.StartNode(cfg, []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}})
```

## Your responsibilities

Core loop(same as etcd/raft):
```go
  for {
    select {
    case <-s.Ticker:
      n.Tick()
    case rd := <-s.Node.Ready():
      saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == raftpb.EntryConfChange {
          var cc raftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      s.Node.Advance()
    case <-s.done:
      return
    }
  }
```



## Messages and Log Entries

Fast Raft and C-Raft extend classic Raft with a few message types and one new log wrapper. Your app loop does **not** change.

### Fast Raft (local tier) – message additions
- **MsgFastProp** – proposer broadcasts entries directly to all peers (including the leader).
- **MsgFastVote** – follower votes for the *speculatively inserted* entry at an index back to the leader.

> If `EnableFastPath=false`, these are not used and behavior matches upstream etcd/raft.

### C-Raft (global tier) – message additions
- **MsgGlobalVote / MsgGlobalVoteResp** – *global* leader election among cluster leaders (one vote per global term; up-to-date check uses the last **leader-approved** global entry).
- **MsgGlobalFastProp** – cluster leader proposes a **global batch** (see below) to all other cluster leaders (fast path).
- **MsgGlobalFastVote** – voter cluster leader returns its vote for the proposed batch (by **digest**) to the global leader.
- **MsgGlobalApp / MsgGlobalAppResp** – classic replication of the chosen global batch (fallback or catch-up).
- **MsgGlobalHeartbeat / MsgGlobalHeartbeatResp** – global heartbeats that also carry the global commit index.

> If `EnableCRaft=false`, the global tier is disabled entirely.

### Log entries and wrappers

#### Classic/Fast (local) entries
- **insertedBy** (internal tag):
  - **InsertedBySelf** – follower speculatively inserted the entry from a fast proposal.
  - **InsertedByLeader** – leader materialized/replicated the chosen entry.
  - **InsertedByUnknown** – classic path (default).

- **Structure rules**:
  - Followers **sparse-append** self-approved entries at arbitrary indexes ≥ committed+1 (gaps allowed); they **do not truncate**.
  - Once the leader decides at index *k*, it replicates the **chosen** entry; followers **overwrite** any self-approved entry at *k* with the leader’s entry.

#### C-Raft local wrapper: **Global State Entry**
- Written to the **local** log by a cluster leader before it votes/accepts the global batch:
  - `GlobalStateEntry{ GlobalIndex, GlobalTerm, Batch }`
  - Commits via **local** Fast Raft (wrapper-first invariant).
  - Ensures recovery: a new local leader can discover/continue any in-flight global decision.

#### Global payload: **GlobalBatch**
- A contiguous batch of **locally committed** entries:
  - `GlobalBatch{ Entries, FirstIndex, LastIndex, Digest }`
  - `Digest` is a stable hash over `(FirstIndex, LastIndex, entries’ {Type,Data})`; it is populated at creation and verified on receipt. Voting/tallying keys off this digest.
  - Leader no-ops (empty `EntryNormal`) are **filtered out** of batches.

---

## Decision Flows (what happens under the hood)

### Fast Raft (local tier)
1. **Propose (fast path)**: proposer sends `MsgFastProp` → all.
2. **Follower**: sparse-append as **InsertedBySelf**; queue a `MsgFastVote` (by entry digest) to the leader.
3. **Leader**: for index *k*,
   - Tally votes; on **classic quorum**: **materialize** the **chosen** entry at *k* as **InsertedByLeader** and replicate.
   - If **fast quorum** and term matches: **fast-commit** *k* immediately; else continue classic replication.
4. **Follower**: on leader append, **overwrite** any self-approved entry at *k* with the leader’s entry and advance commit.

### C-Raft (global tier)
1. **Batching**: when a cluster leader has new locally committed entries, it builds a `GlobalBatch` (no leader no-ops).
2. **Wrapper-first**: it proposes a **Global State Entry** to its **local** cluster and **waits for it to commit**.
3. **Fast-prop**: after the wrapper commits locally, it broadcasts `MsgGlobalFastProp` (carrying the batch + digest) to all other cluster leaders.
4. **Voters**: on receipt, they append a matching local wrapper (via their local Fast Raft), **then** send `MsgGlobalFastVote` (by digest).
5. **Global leader (decision)** for global index *g*:
   - On **classic quorum** of votes: **materialize** the chosen batch at *g* into the global log, **broadcast the chosen batch** (not just a heartbeat), and allow classic commit to advance.
   - On **fast quorum** and **same global term**: **fast-commit** *g*, broadcast chosen batch + heartbeat.
6. **Followers**: accept `MsgGlobalApp`, install the batch at `g`, and advance global commit from heartbeats/commits.

> **Recovery**:  
> • *Local tier*: nodes piggyback self-approved hints in vote responses; a new leader seeds its per-index tallies and decides safely.  
> • *Global tier*: a new global leader **scans its local log** for the latest **committed** Global State Entry and **rebroadcasts** at that global index; wrapper-first ensures safety.

---

## Example latency (5-node cluster)

- **Classic Raft**: proposer → leader → followers → leader commit → broadcast commit (~2 RTT).  
- **Fast Raft (local)**: proposer → all; followers → leader votes; leader commit → broadcast (~1 RTT faster when uncontended).  
- **C-Raft (global)**: same pattern across **cluster leaders** with the added wrapper-first step to tie local and global tiers together (recovery-safe).


## Status

This library is experimental. While it passes a suite of correctness tests (safety, election recovery, overwrite rules), further benchmarking and production hardening are in progress.

## Go docs

API docs are mostly identical to etcd/raft: [pkg.go.dev/go.etcd.io/raft/v3](https://pkg.go.dev/go.etcd.io/raft/v3)
