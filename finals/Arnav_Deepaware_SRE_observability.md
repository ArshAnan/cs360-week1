# System Degradation & Chaos Testing

## System Degradation

In a distributed file-sync and storage system, failures are expected rather than exceptional. A
user may be uploading a file while one storage rack becomes unreachable, another client may
be replaying offline edits, and background workers may simultaneously be repairing
under-replicated chunks. The SRE responsibility is to make sure these partial failures do not
turn into full-system failures. The system should degrade gracefully: users may experience
slower downloads, delayed syncing, or temporary retry messages, but the system must not lose
committed file versions, corrupt chunk manifests, violate permissions, or accept writes that
cannot be safely recovered.
The most important distinction is between **availability** and **safe availability**. It is not enough for
the API to keep returning success if the system is silently losing data or committing metadata
that points to missing chunks. In our architecture, the metadata control plane is the source of
truth for file versions, folder structure, permissions, and chunk manifests. Therefore, the system
should only commit a new file version after the necessary chunks have been durably written to
the storage layer and the metadata transaction can safely point to them. If storage is degraded,
the system should prefer a retryable error or delayed sync over an unsafe success.
Consider the failure scenario where a storage rack goes offline. Since file bytes are stored
separately from metadata, the system can continue serving many requests as long as enough
replicas or erasure-coded fragments remain available. The placement service marks the failed
rack as unhealthy and removes it from the candidate set for new writes. For reads, the storage
client asks the placement layer for the chunk locations in the file manifest and attempts to fetch
the chunk from a healthy replica. If the first replica fails, the client retries another replica or
reconstructs the object from erasure-coded fragments, depending on the data-plane design.
This allows downloads to continue even though one rack is unavailable.
The system should also use **bulkheads** to prevent one part of the system from consuming all
shared resources. User-facing sync operations, metadata commits, background repair jobs,
cleanup jobs, and rebalancing jobs should run in separate worker pools or queues. If a rack
failure creates a large repair backlog, repair workers should not be allowed to consume all
network bandwidth, disk I/O, or database connections. User-facing uploads and metadata reads
should have higher priority than non-urgent background cleanup. This separation limits the blast
radius of the failure.
Another important degradation pattern is **load shedding**. During overload, the system should
reject or delay low-priority work before the entire system collapses. For example, if the metadata
database becomes slow, the sync gateway can temporarily return 429 Too Many Requests
or 503 Service Unavailable for background sync operations while still serving permission
checks, folder listings, and already-started file commits. Clients should respect Retry-After


headers and use exponential backoff with jitter so that thousands of devices do not retry at the
same time. This connects directly to the client sync design, where retries must be safe and
idempotent.
The major tradeoff in this section is **aggressive automatic repair vs. protecting the healthy
system during failure**. Aggressive repair means the system immediately tries to restore every
missing replica as soon as a rack goes offline. This improves durability quickly, but it can also
overload the remaining healthy racks. If every under-replicated chunk triggers repair traffic at
once, the recovery process can saturate the network, increase disk I/O, slow down user reads,
and cause more timeouts. Those timeouts can then cause client retries, which increases load
even more. This is how a small rack failure can become a cascading failure.
Our design chooses a controlled repair strategy instead. The system still repairs missing
replicas, but repair is rate-limited, prioritized, and isolated from user-facing traffic. Chunks that
are below the minimum safe durability level are repaired first. Chunks that are still available from
multiple replicas are repaired later. Repair workers use token-bucket rate limits, per-rack
bandwidth limits, and queue priorities so that recovery work does not overwhelm the healthy
system. This means the system may spend more time in a degraded state, but it remains stable,
predictable, and safe for users.


## Chaos Engineering Runbook

A useful chaos engineering experiment for this system is a controlled **storage rack isolation
test**. The goal is to verify that the system can survive the loss of one storage rack without losing
committed data, corrupting metadata, or triggering cascading overload.
The experiment should begin in a staging environment or a limited production cell with a small
blast radius. Before injecting failure, the team should confirm that observability dashboards and
alerts are working. The selected files should have sufficient redundancy: for a replicated design,
each chunk should have at least three replicas across different racks; for an erasure-coded
design, the system should have enough fragments available to reconstruct the file even after
one rack is lost. The team should also confirm that the placement service knows which chunks
are stored on which racks and that the metadata layer has correct manifests for test files.
The failure injection is to simulate a rack becoming unreachable. This can be done by blocking
network access to the rack, marking the rack unhealthy in the placement service, or disabling
the storage nodes in a controlled test environment. The experiment should not delete data. The
purpose is to test behavior during unavailability, not permanent data destruction.
The expected behavior is as follows. First, health checks should detect that the rack is
unavailable. The placement service should stop assigning new writes to that rack. Second, read
requests should fall back to healthy replicas. The storage client should not rely on a single
hardcoded location; it should ask the placement layer for all available locations for a chunk and
try another location if one fails. Third, writes should continue only if the system can satisfy the
required durability rule. For example, if the policy requires writing to two out of three replicas
before committing metadata, the system should only commit the new file version after the write
quorum succeeds. If the system cannot meet the write requirement, it should return a retryable
error instead of committing unsafe metadata. Fourth, background repair should begin, but
slowly. The repair queue should be rate-limited and lower priority than user-facing reads and
writes.
The abort conditions should be clearly defined before the experiment begins. The test should
stop if metadata commit failures spike, if sync queue age grows without bound, if user-facing
error rate exceeds the agreed threshold, if repair traffic saturates healthy racks, or if permission
checks become unreliable. After the rack is restored, the team should verify convergence:
under-replicated chunks should return to the target replication level, manifests should still match
available chunks, checksums should pass, and no committed file versions should be missing.
The failure we should **not** chaos-test first is a full metadata control-plane outage or
permission-service corruption. Metadata is the source of truth for file versions, chunk manifests,
folder hierarchy, and access control. If metadata fails in the wrong way, the system may point
users to the wrong file version, lose track of chunks, or allow unauthorized access. That blast


radius is too large for a first experiment. A single-rack storage isolation test is safer because it
exercises graceful degradation while keeping the source of truth intact.


## Observability

The observability goal is to detect when the system is falling behind, when it is unsafe to accept
more work, and whether recovery is actually making progress. Metrics should be organized
around the sync pipeline, metadata health, storage health, and user-visible symptoms.
For the sync pipeline, the most important metrics are queue depth, oldest message age,
consumer lag, retry count, dead-letter queue size, upload success rate, upload failure rate, and
end-to-end sync delay. Queue depth alone is not enough because a large queue may be
acceptable if consumers are processing it quickly. Oldest message age is more meaningful
because it shows how long the oldest client operation has been waiting. If the oldest message
age keeps increasing, the system is not keeping up.
For metadata health, the system should track read latency, write latency, transaction abort rate,
lock contention, shard-level load, replication lag, permission-check failures, and manifest
commit failures. This is critical because metadata commits define which file version is current
and which chunks belong to that version. If metadata writes slow down, the entire sync pipeline
slows down even if the storage layer is healthy.
For storage health, the system should track chunk read latency, chunk write latency, missing
chunk count, checksum mismatch count, under-replicated chunk count, repair backlog, repair
throughput, rack availability, and reconstruction rate for erasure-coded files. These metrics show
whether files are physically available and whether the repair system is restoring redundancy
without overwhelming the cluster.
For user-visible health, the system should track failed uploads, failed downloads, stale reads,
conflict rate, average sync delay per device, and the percentage of clients currently backing off.
These metrics connect infrastructure symptoms to user experience. A user does not care that a
repair queue is large; they care that their file is not syncing or that their download fails.
Backpressure becomes visible when the metadata database slows down. Sync workers can
upload chunks to storage, but they cannot finalize the file version until the metadata commit
succeeds. As metadata latency rises, workers spend more time waiting on database
transactions. The sync queue grows, oldest message age increases, retry counts rise, and
clients may begin resending operations. If this is not controlled, the system can enter a retry
storm where the overload causes failures, failures cause retries, and retries create even more
overload.
The SRE response is to apply backpressure deliberately. The sync gateway should slow down
new background sync requests, return retryable errors with Retry-After, prioritize
already-started commits, pause non-critical cleanup and rebalancing jobs, and increase client
backoff. The goal is not to hide the failure. The goal is to keep the system stable while
preserving the core invariants: metadata must point to the correct file version and chunk set,


permissions must remain safe, duplicate retries must not corrupt manifests, and recovery must
not lose or invent committed file versions.


