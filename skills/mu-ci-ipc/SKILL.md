---
name: mu-ci-ipc
description: "Implement zero-copy IPC framing, Apache Fory control messages, and Apache Arrow streaming batches with mu-ci-ipc. Use when working on MuCI controller-worker communication."
---

# `mu-ci-ipc` Zero-Copy Wire Protocol

Use these guidelines when modifying or consuming `mu-ci-ipc` between `mu-ci-controller` and `mu-ci` workers:

## 1. Hybrid Wire Architecture

- **Fixed Header Framing (`framing`)**:
  - Frame headers use `zerocopy` (`FromBytes`, `IntoBytes`, `KnownLayout`, `Immutable`) for alignment-safe, zero-allocation header parsing over WebSocket/TLS streams.
- **Control Plane (`control` via Apache Fory)**:
  - Stage dispatch envelopes, worker heartbeats, capability registration, and terminal stage outcomes use schema-evolvable `fory` serialization.
- **Data Plane (`arrow_stream` via Apache Arrow IPC)**:
  - High-volume live log lines (`stdout`/`stderr`), structured test results, and criterion/custom benchmark metrics stream as columnar Apache Arrow `RecordBatch` frames.

## 2. Single-Slot Worker Concurrency Invariant

- Worker capability advertisements and stage assignment frames enforce `max_slots = 1` (at most one active CI stage process per worker node at a time), while bounding intra-stage parallelism via thread budget environment variables (`CARGO_BUILD_JOBS`, `RAYON_NUM_THREADS`, `RUST_TEST_THREADS`).

## 3. Shared Compiler Cache Policy (`SccacheMode`)

- `SccacheMode` (`Off`, `Try` [default], `Mandatory`) is registered in Apache Fory under `FORY_ID_SCCACHE_MODE = 15` and carried on both `StageDispatchTask` and `TypedTaskInvocation`.
- Custom `serde` deserialization accepts booleans (`true` -> `Try`, `false` -> `Off`) and case-insensitive strings (`"try"`, `"mandatory"`, `"off"`).
