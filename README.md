# agent-transport

**A message-oriented agent transport — a QUIC-style fast channel for MCP / A2A agent messaging over UDP.**

Receiver-driven scheduling in the Homa tradition: **single-packet round trips** for short messages, **8-level QoS**, and **loss resilience** — designed so interactive short RPCs are never queued behind large transfers.

> 中文版见文末 [中文版](#中文版)

## Why

Agent protocols (MCP, A2A) today run on HTTP / SSE / WebSocket: byte streams, per-message framing, head-of-line blocking. Their real workload is structurally different:

- **Many short, interactive round trips** — tool calls, elicitation prompts, small JSON-RPC requests (latency-dominated)
- **Occasional large transfers** — context snapshots, tool outputs, media artifacts (bandwidth-dominated)
- **Both mixed on one connection** — a large transfer must not queue interactive requests behind it

`agent-transport` treats **messages as first-class citizens** instead of bytes.

## Features

- **Single-packet round trips** — messages ≤ 10 KiB arrive in their first RTT via an unscheduled window: no handshake, no grant, no frame parsing.
- **Short RPCs are not starved by long transfers** — receiver-driven GRANT / SRPT scheduling plus 8-level priority queues. A long message's grant bursts never block a high-priority short RPC.
- **Loss resilience** — receiver-driven RESEND / GRANT batch-repair; under 1% loss, short-RPC P99 drops from 5.1 s to 453 ms (see [homa-rpc evidence](https://github.com/directwire/directwire/blob/main/docs/benchmarks/net-sim-v0.2.md)).
- **at-least-once + idempotent dedup** — timed-out requests are resent wholesale with the same `rpc_id`; the server caches and dedups, so handlers can be idempotent.
- **Determinism by construction** — core is pure state machines (no I/O coupling), fixed-seed tests and benchmarks, bit-level reproducible.

Architecture reference: Stanford's [Homa](https://homa-transport.org) (Ousterhout et al.) — a Homa-lite on UDP that keeps the message semantics (GRANT / SRPT / 8-level priority) and drops the data-center-specific machinery.

## Quick start

```toml
[dependencies]
agent-transport = "0.1"
```

```rust
use agent_transport::rpc::{RpcClient, RpcServer};

// Server: at-least-once; handlers can be idempotent
let server = RpcServer::spawn("127.0.0.1:0", |req| req.to_vec())?;
// Client: short messages in a single round trip
let client = RpcClient::new("127.0.0.1:0")?;
let resp = client.call(server.addr(), b"hello")?;
assert_eq!(resp, b"hello");
```

See [`WIRE.md`](WIRE.md) for the 22-byte wire format and packet semantics, and `src/transport/*` for the pure state machines.

## Reproducible numbers (ships its own benchmark)

`cargo bench --bench mixed` — fixed workload, fixed seed, zero randomness; anyone can rerun it:

```text
| impl             | short n | short P50 | short P90 | short P99 | long n | long P50 | long P90  | long P99   |
|------------------|---------|-----------|-----------|-----------|--------|----------|-----------|------------|
| agent-transport  | 500     | 848.5     | 1253.7    | 4327.7    | 50     | 36377.6  | 48194.8   | 234411.6   |
| tcp-baseline     | 500     | 1265.1    | 1755.5    | 21924.4   | 50     | 2353.8   | 3023.5    | 24498.3    |
```

Short-RPC **P99 speedup ≈ 5.1×**, P50 ≈ 1.5×.

- Reference machine: Windows 11 x86_64 (32-core), release build.
- Baseline pinned: `bench-data/baseline.json` (workload-hash checked; drift is rejected).
- Gate: `python scripts/gate.py` (>1% regression turns the gate red).

**Honest boundary**: this is a loopback benchmark — no real-network congestion, no NIC priority queues, no multi-hop relays. It demonstrates the scheduling architecture (short-not-starved-by-long), *not* a claim of reproducing Homa's 19–72× data-center numbers.

## The MCP / A2A angle

The MCP roadmap invites the community to experiment with custom transports while official transports stay fixed this cycle. This crate is the reference implementation behind that experiment: see [transports-wg issue #52](https://github.com/modelcontextprotocol/transports-wg/issues/52) (*receiver-driven SRPT over UDP vs. the sender-driven MOQT drafts*) and the technical comparison in [`docs/moq-vs-srpt-comparison.md`](docs/moq-vs-srpt-comparison.md). An IETF draft of the wire format lives in [`ietf/`](ietf/).

## License

MIT OR Apache-2.0.

---

## 中文版

**消息导向的 Agent 传输层 —— MCP / A2A 的 QUIC 式快速通道。** 今天的 agent 通信（MCP / A2A）跑在 HTTP/SSE/WebSocket 之上：字节流 + 每消息帧头 + 队头阻塞。`agent-transport` 把**消息当作一等公民**：

- **单包往返**：≤ 10 KiB 的消息首 RTT 即送达（免授权窗口），无需建连、无帧解析。
- **短 RPC 不被长传输阻塞**：接收端 GRANT/SRPT 调度 + 8 级优先级队列，长消息的授权突发不会挡住高优先级短 RPC。
- **丢包弹性**：接收端驱动 RESEND / GRANT 批量修复，1% 丢包下短 RPC P99 从 5.1s 降到 453ms。
- **at-least-once + 幂等去重**：超时整请求重发（rpc_id 不变），服务端缓存去重，handler 可幂等。
- **确定性**：核心为纯状态机（无 IO 耦合），测试与 benchmark 固定 seed、位级可复现。

可复现 benchmark：`cargo bench --bench mixed`，混合负载（500 短 + 50 长）下短 RPC P99 相对 TCP 基线加速约 **5.1×**（诚实边界：loopback，展示架构特性非复现 Homa 论文数字）。MCP 官方本周期把 custom transports 当实验通道——本 crate 是该实验的参考实现，见 [transports-wg issue #52](https://github.com/modelcontextprotocol/transports-wg/issues/52)。

许可：MIT OR Apache-2.0。
