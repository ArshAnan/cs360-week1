# Modern Distributed Computing Final Project

## Project Selected

**Project 2: Distributed File Sync & Storage**

Our group is designing a Dropbox-style distributed file synchronization and storage system. The system must store petabytes of user data, synchronize file changes across devices that may frequently go offline, preserve metadata and permission correctness, and resolve concurrent edits without losing committed user work.

## System Overview

At a high level, the architecture separates the system into a metadata control plane, a file data plane, a client-facing sync engine, and operational reliability services.

User devices interact with a sync gateway that accepts file chunk uploads, metadata updates, and sync cursor requests. The metadata control plane tracks folders, file versions, permissions, manifests, and consistency-sensitive state. The data plane stores the actual file bytes as content-addressed chunks across replicated or erasure-coded storage nodes. The sync engine handles offline edits through durable local operation logs, logical clocks, idempotent retries, and conflict detection. Background repair, rebalancing, observability, and chaos testing complete the design by making sure the system degrades safely during partial failures.

The most important invariant in our design is that metadata must always point to the correct file version and chunk set. Permission changes must preserve security rules, duplicate retries must not corrupt manifests or store duplicate chunks, and recovery must never invent or lose committed file versions.

## How to Read This Submission

Each group member wrote a separate Markdown file for one architectural area. The files are intended to be read in this order:

1. Control plane: metadata scaling, sharding, rebalancing, and isolation.
2. Data plane: chunk storage, integrity, replication, erasure coding, quorums, and repair.
3. Sync engine: offline edits, logical clocks, conflict resolution, idempotency, and retries.
4. SRE: graceful degradation, chaos testing, observability, and backpressure.

The current submission includes the completed sections listed below. The remaining sections should be added to this directory as separate `.md` files and linked here before final submission.

## Group Member Sections

| Section | Group Member | File | Status |
| --- | --- | --- | --- |
| The Control Plane: Metadata Scaling & Sharding | Stephanie Mata | [stephanie_mata_the_control_plane.md](stephanie_mata_the_control_plane.md) | Complete |
| Sync Engine & Conflict Resolution: The Client Interface | Arsh Anand | [arsh_anand_sync_engine_and_conflict_resolution.md](arsh_anand_sync_engine_and_conflict_resolution.md) | Complete |
| The Data Plane: Block Storage & Integrity | TBD | TBD | Pending |
| System Degradation & Chaos Testing: The SRE | Arnav Deepaware | [Arnav_Deepaware_SRE_observability.md](Arnav_Deepaware_SRE_observability.md) | Complete |


## Presentation Notes

- Control plane: range sharding improves locality for folder scans, while hash sharding improves write distribution; the design chooses a hybrid strategy and uses virtual slots for safer rebalancing.
- Data plane: replication improves availability and simpler reads, while erasure coding reduces storage overhead at the cost of more complex repair and reconstruction.
- Sync engine: physical timestamps are unsafe for correctness, so the client protocol uses logical clocks and vector clocks to detect concurrent offline edits.
- SRE: the system should degrade by isolating failures with bulkheads, applying backpressure, preserving durable data, and surfacing sync delay honestly to users.

## Final Submission Checklist

- Add the Data Plane Markdown file.
- Add the SRE Markdown file. (complete)
- Replace the `TBD` rows above with the correct names and links.
- Confirm all filenames use relative links that work from this README.
- Make sure the final design document reads as one cohesive system rather than four unrelated sections.
