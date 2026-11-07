# ActionRunner-ipc

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs">
    <img src="https://hook.lma1980.duckdns.org/cards/LMA1980/ActionRunner-ipc/card.svg" alt="ActionRunner-ipc Factory Scorecard" width="520"/>
  </a>
</p>

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-ipc/status.svg" alt="CI Status"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-ipc/score.svg" alt="Quality Score"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-ipc/coverage.svg" alt="Coverage"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-ipc/benchmark.svg" alt="Benchmark"/></a>
</p>

---

High-performance, zero-copy inter-process communication (IPC) and distributed fabric protocol crate for the **ActionRunner** ecosystem.

## Core Capabilities

- **Wire Protocol Framing (`zerocopy`)**: Aligned 24-byte C-representation header (`IpcHeader`) with zero-copy buffer slicing via `zerocopy::FromBytes` and `IntoBytes`.
- **Control Plane Contracts (`Apache Fory`)**: High-speed, compile-time derived binary serialization for worker registrations, heartbeat telemetry, stage dispatch tasks (`StageDispatchTask`, `TypedTaskInvocation`), sandbox constraints (`SandboxConstraintSpec`), and shared compiler cache policies (`SccacheMode`, `FORY_ID_SCCACHE_MODE = 15`).
- **Data Plane Streaming (`Apache Arrow`)**: Columnar streaming for build output logs and factory pipeline metrics (`coverage`, `benchmarks`, `pass-ratios`) via Arrow IPC `StreamWriter` and `StreamReader`.

## `SccacheMode` Wire Policy (`FORY_ID_SCCACHE_MODE = 15`)

`StageDispatchTask` and `TypedTaskInvocation` carry `sccache_mode: SccacheMode` across the controller-worker tunnel:

| Variant | Accepted YAML / JSON Literals | Semantics |
| :--- | :--- | :--- |
| `SccacheMode::Try` *(default)* | `true`, `"try"`, `"auto"`, `"yes"`, `"1"` | Use `sccache` for eligible compiler tools when installed and healthy on the worker; fall back cleanly to uncached `rustc` if unavailable. |
| `SccacheMode::Mandatory` | `"mandatory"`, `"required"`, `"strict"` | Require `sccache`; controller routes compiler stages to `"sccache"`-tagged workers and worker fails closed (`exit 126`) if `sccache` is missing or unhealthy. |
| `SccacheMode::Off` | `false`, `"off"`, `"disabled"`, `"no"`, `"0"` | Disable `sccache` and strip `RUSTC_WRAPPER` / `CARGO_BUILD_RUSTC_WRAPPER` for the stage or task. |

## Protocol Layout

```text
+-----------------------+----------------------------------------------------+
| IpcHeader (24 Bytes)  | Payload (Variable Length)                          |
+-----------------------+----------------------------------------------------+
| Magic: b"K3CI" (4B)   | If MsgType == 0x01: Apache Fory Binary Object      |
| Version: u16          | If MsgType == 0x02: Arrow IPC Log RecordBatch      |
| MsgType: u16          | If MsgType == 0x03: Arrow IPC Metric RecordBatch   |
| PayloadLen: u32       |                                                    |
| Flags: u32            |                                                    |
| Sequence: u64         |                                                    |
+-----------------------+----------------------------------------------------+
```

## License

Licensed under the GNU Lesser General Public License v3.0 with the LGPL-3.0 Linking Exception (LGPL-3.0-only WITH LGPL-3.0-linking-exception). See LICENSE for details.
