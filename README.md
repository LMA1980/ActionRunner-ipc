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
- **Control Plane Contracts (`Apache Fory`)**: High-speed, compile-time derived binary serialization for worker registrations, heartbeat telemetry, stage dispatch tasks, and completion reports.
- **Data Plane Streaming (`Apache Arrow`)**: Columnar streaming for build output logs and factory pipeline metrics (`coverage`, `benchmarks`, `pass-ratios`) via Arrow IPC `StreamWriter` and `StreamReader`.

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

Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See LICENSE for details.
