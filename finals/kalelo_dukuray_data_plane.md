# The Data Plane (Block Storage & Integrity)

### Section 1: Architectural Design

The data plane is responsible for durably storing actual file bytes — the raw content that metadata only points to. The central design question is how to map a chunk identifier to its physical location across potentially thousands of storage nodes.

The naive approach is a **centralized placement registry**: a master lookup table that records exactly which node holds which chunk. This works well at small scale, but breaks down quickly. With petabytes of data and billions of chunks, the lookup table itself becomes enormous, requires constant updates on every write, rebalance, and failure event, and becomes a single point of contention. Any downtime or slowness in the registry blocks all reads and writes. The registry also suffers from hot-path bottlenecks when highly popular chunks are fetched repeatedly.

The more scalable alternative is **algorithmic placement**, most prominently the CRUSH algorithm used by Ceph. Rather than looking up a chunk's location, CRUSH _computes_ it. Given a chunk ID and a cluster map describing the topology (racks, nodes, disks), CRUSH deterministically outputs a placement list without any central lookup. This means clients and storage nodes can independently agree on where data lives, and the cluster map — updated only on membership changes — can be distributed lazily via gossip. The result is near-zero lookup overhead, natural load distribution, and resilience to registry outages.

In practice, the architecture separates concerns into three layers. First, a **chunking and content-addressable storage layer**: the client splits files into fixed-size chunks (e.g., 4 MB), computes a hash (SHA-256 or BLAKE3) of each chunk's content, and uses that hash as the chunk ID. This naturally deduplicates identical data — if two users upload the same file, only one copy of each chunk is stored. Second, a **placement layer** (CRUSH or equivalent) determines which three storage nodes a given chunk hash maps to. Third, the **storage nodes** themselves are simple daemons that expose a basic put/get/delete interface over chunk IDs and write data to local disks.

This design scales horizontally: adding nodes updates the cluster map, CRUSH recomputes placements, and background workers migrate only the affected fraction of chunks. No central bottleneck coordinates individual chunk reads or writes.

---

## Section 2: Fault Tolerance & Storage

Once we know where chunks live, we must decide how many copies to keep and in what form. The two main strategies are **replication** and **erasure coding**, each with distinct availability and cost profiles.

**Replication** is conceptually simple: store N identical copies of each chunk on N different nodes (typically N=3, spread across racks). Any single copy can satisfy a read request, and the system can tolerate up to N−1 node failures before data is unavailable. Recovery after a failure is fast: the surviving copies are simply re-replicated to a new node. The cost is space amplification — 3× replication means 1 TB of raw data consumes 3 TB of storage, which is expensive at petabyte scale.

**Erasure coding** is the more storage-efficient alternative. Rather than storing full copies, the chunk is encoded into k+m fragments where any k fragments are sufficient to reconstruct the original. A common configuration is 6+3: the chunk is split into 6 data fragments and 3 parity fragments, and any 6 of the 9 are sufficient for recovery. This yields roughly 1.5× storage overhead (versus 3× for replication), a significant saving at scale. The tradeoff is computational cost and repair complexity. Recovering a lost fragment requires reading k surviving fragments and performing a decode operation — substantially more I/O and CPU than simply copying a replica. Under high failure rates or during degraded reads, this can stress the cluster.

The practical implication for this system is a tiered policy. **Hot data** — recently uploaded or frequently accessed files — uses 3× replication for low-latency reads and fast repair. **Cold or archival data** transitions to erasure coding (e.g., 6+3) after a cooling period, trading repair performance for storage efficiency. Background workers handle this tiering transparently, and the metadata layer records which scheme each chunk uses so readers can issue the correct request.

One nuance: replication is inherently more available during partial outages. With 3× replication, reads succeed even if 2 of 3 replicas are slow or unreachable. Erasure coding requires at least k fragments from k+m nodes, so recovering from scattered failures is more complex and involves more network coordination.

---

## Section 3: Quorums & Repair

Even with redundancy, we need a formal model for how many nodes must acknowledge a read or write before we consider it complete. The **quorum model** parameterizes this with three values: N (total replicas), W (write quorum), and R (read quorum).

A write completes when W out of N replicas acknowledge the data. A read is satisfied when R replicas respond with their version. The critical invariant is **R + W > N**, which guarantees at least one node participates in both the write quorum and the read quorum. Concretely, if N=3, W=2, R=2, then R+W=4>3, so any read is guaranteed to overlap with the most recent write's participants. This means we can always find the up-to-date chunk without consulting all N nodes.

If we relax to W=1, R=1 (prioritizing availability and low latency), we risk **stale reads**: a client might read from a replica that hasn't yet received the latest write. This is acceptable in some workloads — e.g., reading a large video file where minor staleness is invisible — but unacceptable for file sync, where reading an old version of a shared document would cause silent data loss or divergence.

For this system, the sensible default is W=2, R=2 with N=3, giving strong consistency with tolerance for one node being slow or unavailable. Writes can still make progress even if one replica is temporarily down, and reads always see the latest committed version.

**Read repair** is a complementary background mechanism. When a read fetches data from R replicas and notices they hold different versions of a chunk (detected via hash mismatch or version vector comparison), the coordinator identifies the stale replica and writes the latest version back to it asynchronously. This is cheap to implement and gradually heals divergence caused by temporary node failures or network partitions without requiring a full background scan.

However, read repair has limits. It is reactive — it only heals replicas that happen to be read. Infrequently accessed cold data can remain divergent for long periods. The solution is a periodic **anti-entropy scrub**: background workers compare replica checksums using Merkle trees, which efficiently identify divergent subtrees without comparing every chunk individually. Discrepancies trigger targeted repair transfers. Combined with read repair, this provides strong eventual consistency guarantees even under sustained partial failures, without requiring a synchronous repair path in the hot read/write loop.