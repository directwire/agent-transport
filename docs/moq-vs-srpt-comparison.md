# Sender-Driven vs. Receiver-Driven Scheduling for Agent Transports

**A technical comparison of the MOQ-based agent transport drafts and `agent-transport` (receiver-driven SRPT over UDP)**

> 中文摘要：Agent 工作负载以大量短往返 + 偶发大块传输为特征。当前 IETF 的 MOQT 系代理传输 draft（Nandakumar / Jennings / Liu 三家，7–8 份）全部采用**发送端驱动**调度：publisher 在 MOQT 对象头上盖优先级，relay 按标签排序，订阅者**无法表达自身紧迫度**，且全部继承 QUIC 拥塞控制、无应用层调度算法，多数未处理 HOL/拥塞。`agent-transport` 走相反设计点：**接收端驱动**——短消息单包往返（unscheduled window，无握手）、接收方 GRANT 授权近似 SRPT、8 级优先级、接收方 RESEND 控损。本仓库的可复现 benchmark（固定负载 + FNV-1a 指纹 + 门禁）给出实测：混合负载下短 RPC P50 814.6µs / P99 4231.1µs，对 TCP 基线加速 P99 5.2×。两份方案是互补而非敌对：MOQ 继承 QUIC 生态成熟度，SRPT-over-UDP 抢占交互式短消息延迟——MCP 官方正把 custom transports 当实验通道，这正是落点。

---

## 1. Why the transport decision matters for agents

Agent protocols (MCP, A2A) carry a workload that is structurally different from the byte-stream workloads HTTP/WebSocket were built for:

- **Many short, interactive round trips**: tool calls, elicitation prompts, small JSON-RPC requests — a handful of bytes each, latency-dominated.
- **Occasional large transfers**: context snapshots, tool outputs, media artifacts — megabyte-scale payloads whose completion time is bandwidth-dominated.
- **Mixed on one connection**: a large artifact transfer must not queue interactive requests behind it.

A transport for agents should therefore deliver short messages in **one round trip** and keep them **ahead of bulk transfers**. This is exactly the regime where receiver-driven scheduling wins — and exactly what the current IETF agent-transport drafts do not optimize for.

## 2. The MOQ-based agent transport drafts

As of 2026-08, IETF has a family of drafts carrying agent protocols over **Media over QUIC (MOQT)**. They fall into four author families:

| Draft | Authors | Targets | Status |
|---|---|---|---|
| `draft-nandakumar-ai-agent-moq-transport-00` | Nandakumar, Jennings (Cisco) | Generic + A2A/MCP/AutoGen | Active, expires 2026-09 |
| `draft-nandakumar-agentproto-moq-pace-00` (PACE) | Nandakumar, Jennings (Cisco) | Session layer, protocol-agnostic | Active, expires 2026-12 |
| `draft-nandakumar-a2a-moqt-transport-00` | Nandakumar, Jennings (Cisco) | A2A | Expired 2026-04 |
| `draft-jennings-mcp-over-moqt-00` | Jennings (Cisco), Swett (Google), Rosenberg (Five9), Nandakumar (Cisco) | MCP | Expired; superseded |
| `draft-jennings-ai-mcp-over-moq-00` | same | MCP + Agent Skills | Expired 2026-09 |
| `draft-jennings-agentproto-mcp-over-moqt-00` | same | MCP + Agent Skills (27pp, current) | Active, expires 2027-01 |
| `draft-liu-agent-protocol-over-moq-00` | Liu (Alibaba), Krishnan (Cisco) | Generic A2A (custom 21-byte frame) | Active, expires 2026-09 |
| `draft-liu-moq-live-agent-interaction-01` | Liu, Liu (Alibaba) | Live agent interaction | Active, expires 2027-01 |

All are **sender-driven**. The publisher stamps a per-object priority (0–255 in Nandakumar's family, 1–127 in Jennings', 0x00–0x05 in Liu's live-interaction draft), sets `GROUP_ORDER`, and the relay/scheduler delivers in that order. Subscribers express interest via SUBSCRIBE/SUBSCRIBE_NAMESPACE but have **no mechanism to reorder transmission by their own urgency**. Congestion control is inherited from QUIC everywhere; no draft adds an application-layer scheduling policy. Several drafts do not address head-of-line blocking or congestion control beyond "QUIC streams prevent interference."

## 3. The structural gap in the sender-driven model

The sender-driven model has one blind spot that matters precisely for agent workloads:

1. **The sender cannot know what the receiver needs first.** A relay carrying 100 concurrent agents sees only the per-object priority stamps the *publishers* assigned. When 50 bulk transfers and 5 interactive requests arrive together, the receiver's interactive requests wait behind whichever stream the sender prioritized — the sender is guessing at the receiver's urgency.
2. **Short-message latency is bounded below by the subscription handshake.** MOQT object delivery rides on QUIC streams set up via SUBSCRIBE_NAMESPACE/SUBSCRIBE/OBJECT exchanges (the A2A draft's request-response path is a 7-step dance). A 100-byte tool call that could be answered in one RTT pays stream establishment.
3. **Priority is a stamp, not a schedule.** Per-object priority orders *delivery of already-admitted* data. It does not schedule *admission*: a long message already in flight can consume the whole bandwidth budget while shorter ones wait — the classic SRPT-vs-FIFO gap.
4. **Determinism is not a design goal.** None of the drafts specifies a fixed workload, a reproducible benchmark, or a regression gate. Performance claims are asserted, not auditable.

## 4. The receiver-driven alternative: SRPT over UDP

`agent-transport` implements the complementary design point, in the Homa tradition [HOMA], on plain UDP:

- **Messages, not bytes.** A 22-byte header (packet type, 8-level priority, 64-bit message ID, length, offset) + payload. Any number of messages multiplex on one socket.
- **Single-packet round trips.** A message within the unscheduled window is delivered in one RTT: first chunk sent immediately, no handshake, no grant, no frame parsing.
- **Receiver-driven admission.** The receiver grants transmission credits (GRANT) up to a cumulative offset. It ranks incomplete messages by remaining bytes (**Shortest Remaining Processing Time**) and overcommits to K (default 2), so a single grant does not starve other messages. A newly arrived short message preempts a longer message's grant cadence.
- **Receiver-driven loss repair.** RESEND covers all missing chunks in the granted window in one request; a silent grant is re-issued; an idle sender re-pokes. At-least-once semantics + message-ID dedup give exactly-once handlers.
- **Eight sender-side priority queues** (derived from message length) drain highest-first, so a burst of low-priority chunks never blocks a short high-priority request.
- **Determinism by construction.** Pure state machines, no I/O coupling, no runtime randomness.

## 5. Head-to-head

| Dimension | MOQ-based drafts | `agent-transport` |
|---|---|---|
| Scheduling direction | Sender-driven (publisher priority stamps, relay orders) | **Receiver-driven** (GRANT + SRPT admission) |
| Short-message latency | Bounded by subscription/setup + queuing behind bulk | **One RTT within unscheduled window** |
| HOL blocking | Inherited from QUIC streams; largely unaddressed | Bounded by SRPT grant + 8-level priority queues |
| Admission control | None beyond per-object priority ordering | SRPT ranking + overcommit K |
| Loss recovery | QUIC congestion control (inherited) | Receiver-driven RESEND / grant re-issue / poke |
| Congestion control | Inherited from QUIC; none adds app-layer policy | None at transport layer (determinism) — relies on UDP pacing + receiver grants |
| QoS granularity | Per-object 0–255 / 1–127 / 0x00–0x05 | 8 levels derived from message length |
| Determinism | Not a stated goal | Fixed workload + FNV-1a fingerprint + committed baseline + >1% regression gate |
| Reference implementation | One Go impl (`mcp-moqt`, stale) | Rust, published on crates.io + GitHub, IETF draft v00 |

## 6. Benchmark evidence

The reference implementation ships a **deterministic** benchmark (`benches/mixed.rs`, `scripts/gate.py`): a fixed workload of 550 RPCs across 8 threads, ~91% 100-byte short calls + ~9% 1 MiB long calls, zero randomness, workload locked by an FNV-1a fingerprint, baseline committed, any regression >1% turns the gate red.

Reference machine (32-core, Windows, release build), best-of-3:

| | agent-transport (UDP) | tcp-baseline | Speedup |
|---|---|---|---|
| Short RPC **P50** | 814.6 µs | 1265.1 µs | 1.5× |
| Short RPC **P99** | 4231.1 µs | 21924.4 µs | **5.2×** |
| Throughput | 1210.8 ops/s | — | — |

What the number shows is the architecture property the design claims: in a mixed workload, short RPCs are **not** pushed to the tail by long transfers (TCP byte-stream queues them; SRPT/priority scheduling does not). 

**Honest boundary**: this is a loopback benchmark — there is no real-network congestion, no NIC priority queues, no multi-hop relays. It demonstrates the scheduling architecture (short-not-starved-by-long), **not** a claim of reproducing Homa's 19–72× data-center numbers. A head-to-head against a MOQT implementation on a real network testbed is open work — and the deterministic harness is the tool to do it fairly.

## 7. Complementarity, not rivalry

The two approaches are complementary, and both fit MCP's official custom-transports lane ("we will not be introducing additional official transports this cycle; the community should experiment via custom transports"):

- **MOQT brings** QUIC's mature ecosystem — TLS, multiplexing, congestion control, WebTransport bridging — and is the right answer where a full QUIC deployment is acceptable.
- **`agent-transport` brings** the latency tail that QUIC's stream/session machinery cannot remove: single-packet round trips and receiver-driven SRPT for the interactive short-message regime, plus an auditable determinism story that no current draft provides.

The reference implementation's interface is transport-pluggable at the SDK boundary (MCP SEP-2598's "custom transports plug in beneath the SDK"), and the deterministic harness exists so the two can be compared on equal terms.

## References

- MOQ-based drafts: see §2 (datatracker.ietf.org, 2026-08)
- [HOMA] Montazeri et al., "Homa: A Receiver-Driven Low-Latency Transport Protocol Using Network Priorities", ACM SIGCOMM 2018.
- MCP roadmap: "no additional official transports this cycle; experiment via custom transports" (modelcontextprotocol.io/development/roadmap).
- MCP spec transports page (2026-07-28): clients/servers MAY implement additional custom transport mechanisms.
- This repository: `WIRE.md` (wire format), `benches/mixed.rs` + `scripts/gate.py` + `bench-data/baseline.json` (reproducible benchmark), `ietf/draft-directwire-agent-fast-transport-00` (IETF draft v00).
