# agent-transport Wire Format

> This document is the normative description of `src/transport/packet.rs` and the
> wire-format basis for the IETF agent-transport draft (v00).
> **The implementation in `packet.rs` is authoritative — if you change the code,
> change this document.**

## Transport unit

- Carrier: **UDP datagram** — a message is split into fixed-size fragments, one datagram per fragment.
- Default path-MTU budget: `packet_size = 1200` bytes (22-byte header + payload).
- Byte order: **little-endian**, no alignment padding.

## Packet header (22 bytes, fixed)

| Offset | Length | Field | Meaning |
|---|---|---|---|
| 0 | 1 | `type` | Packet type (below) |
| 1 | 1 | `priority` | 8-level dynamic priority, 0 = highest (DATA only) |
| 2 | 8 | `msg_id` | Message ID, assigned by the sender, unique within the message |
| 10 | 4 | `msg_len` | DATA: total message length; control packets: 0 |
| 14 | 4 | `offset` | See the field-reuse table |
| 18 | 4 | `length` | See the field-reuse table |

A DATA packet carries the payload after the 22-byte header; **the DATA payload
length must equal `length`**, otherwise decoding fails.

## Packet types

| Value | Type | Meaning |
|---|---|---|
| 0 | `DATA` | A `[offset, offset+length)` fragment of a message; `msg_len` = total message length |
| 1 | `GRANT` | Receiver authorizes the sender to transmit up to `offset` (cumulative); `length` = new grant amount |
| 2 | `RESEND` | Receiver requests re-send of the `[offset, offset+length)` range (missing within the granted window) |
| 3 | `BUSY` | Receiver has too many concurrent messages; ask the sender to retry later |

Unknown types (> 3) fail decoding and are dropped.

## Priority

`priority_for_len` maps total message length to 8 levels (0 = highest):

| Level | Message length |
|---|---|
| 0 | ≤ 100 B |
| 1 | ≤ 1 KB |
| 2 | ≤ 10 KB |
| 3 | ≤ 100 KB |
| 4 | ≤ 1 MB |
| 5 | ≤ 10 MB |
| 6 | ≤ 100 MB |
| 7 | > 100 MB |

Control packets (GRANT / RESEND / BUSY) bypass the send queue and go out directly —
they are small and their timeliness drives scheduler liveness.

## Scheduling semantics (normative points)

1. **Unscheduled window** — the first `unscheduled_bytes` (default 10 KiB) of a message
   arrive in their first RTT without waiting for a grant — **single-packet round trips
   for short messages**.
2. **Grant-driven (GRANT / SRPT)** — bytes beyond the window must wait for the receiver's
   cumulative grant. The receiver ranks incomplete messages by **shortest remaining
   processing time** and grants the top K (`overcommit`, default 2); a newly arrived,
   shorter message can preempt a long message's grant cadence. Cumulative grants guarantee
   progress — no starvation.
3. **Loss recovery** — a missing packet inside the granted window exceeding `resend_timeout`
   triggers a batched `RESEND`; a grant making no progress beyond `grant_timeout` triggers a
   re-sent `GRANT`; a stalled sender beyond `poke_timeout` re-sends its last fragment as a
   probe. After completing a message, the sender lingers for `linger` to answer late `RESEND`s.
4. **Anti-starvation** — a message waiting for a grant beyond `starve_threshold` (default
   200 ms) is forced into the grant set.
5. **at-least-once** — the RPC layer re-sends a timed-out request wholesale (same `rpc_id`);
   the server's idempotent dedup cache guarantees each `rpc_id` is executed at most once.

## Reproducibility

The core state machines (sender / receiver / txqueue) are **I/O-free and zero-randomness**,
so they can be driven directly by deterministic unit tests and fuzzing; only the network side
uses socket2 for buffering and timeouts. The benchmark uses a fixed workload and fixed seed
(see `benches/mixed.rs`), so anyone can rerun it and get the same numbers on the same load.

---

## 中文版

**线格式规范**：UDP 数据报承载，`packet_size = 1200`（含 22 字节头 + 负载），小端、无对齐。

- **包头（22 字节固定）**：`type`(1) + `priority`(1, 0 最高) + `msg_id`(8) + `msg_len`(4, DATA 为消息总长/控制包为 0) + `offset`(4) + `length`(4)；DATA 负载长度必须等于 `length`。
- **包类型**：`DATA`(0) / `GRANT`(1, 累计授权到 offset) / `RESEND`(2, 请求重发窗口内缺包) / `BUSY`(3, 并发超限请重试)；未知类型解码报错丢弃。
- **优先级**：`priority_for_len` 按消息长度映射 8 级（0 最高，≤100B → >100MB）；控制包直发不排队。
- **调度语义**：①免授权窗口（默认 10 KiB）内首 RTT 直达——短消息单包往返；②窗口外字节等接收端累计授权，接收端按 SRPT（剩余字节最少优先）选前 K 条（`overcommit` 默认 2）发 GRANT，短消息可抢占长消息节奏；③丢包恢复：RESEND / 重发 GRANT / 发送端 poke，完成消息留 `linger` 应答迟到 RESEND；④防饿死：等授权超 `starve_threshold`（200ms）强制入授权集合；⑤at-least-once：超时整请求重发（rpc_id 不变），服务端幂等去重。
- **复现性**：核心状态机无 IO 耦合、零随机，可确定性单测/fuzz；benchmark 固定负载 + 固定 seed。

*agent-transport wire format v0.1 · 2026-08-18 · determinism is the promise, contribution is the acceptance criterion.*
