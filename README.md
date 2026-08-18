# agent-transport

**消息导向的 Agent 传输层 —— MCP / A2A 的 QUIC 式快速通道。**

A message-oriented agent transport: a QUIC-style fast channel for
[MCP](https://modelcontextprotocol.io) / [A2A](https://a2a-protocol.org) agent
messaging. **Single-packet round-trips, 8-level QoS, loss resilience** —
HTTP 做兼容主通道，`agent-transport` 做加速层。

## 为什么需要它

今天的 agent 通信（MCP / A2A）跑在 HTTP/SSE/WebSocket 之上：字节流 + 每消息帧头 +
队头阻塞。`agent-transport` 把**消息当作一等公民**：

- **单包往返**：≤ 10 KiB 的消息首 RTT 即送达（免授权窗口），无需建连、无帧解析。
- **短 RPC 不被长传输阻塞**：接收端 GRANT/SRPT 调度 + 8 级优先级队列，长消息的
  授权突发不会挡住高优先级短 RPC。
- **丢包弹性**：接收端驱动 RESEND / GRANT 批量修复，1% 丢包下短 RPC P99 从 5.1s
  降到 453ms。
- **at-least-once + 幂等去重**：超时整请求重发，服务端缓存去重，handler 可幂等。

架构参考：Stanford [Homa](https://homa-transport.org)（Ousterhout et al.），
此处为 UDP 上的 Homa-lite（保留消息语义，去掉数据中心专用机制）。

## 快速开始

```toml
[dependencies]
agent-transport = "0.1"
```

```rust
use agent_transport::rpc::{RpcClient, RpcServer};

// 服务端：at-least-once，handler 可幂等
let server = RpcServer::spawn("127.0.0.1:0", |req| req.to_vec())?;
// 客户端：短消息单包往返
let client = RpcClient::new("127.0.0.1:0")?;
let resp = client.call(server.addr(), b"hello")?;
assert_eq!(resp, b"hello");
```

## 可复现的数字（出厂自带 benchmark）

`cargo bench --bench mixed` — 固定负载、固定 seed、零随机，任何人可重跑：

```text
| 实现 | 短样本 | 短P50 | 短P90 | 短P99 | 长样本 | 长P50 | 长P90 | 长P99 |
|---|---|---|---|---|---|---|---|---|
| agent-transport | 500 | 848.5 | 1253.7 | 4327.7 | 50 | 36377.6 | 48194.8 | 234411.6 |
| tcp-baseline   | 500 | 1265.1 | 1755.5 | 21924.4 | 50 | 2353.8 | 3023.5 | 24498.3 |

短 RPC P99 加速比: 5.1×
短 RPC P50 加速比: 1.5×
```

- 参考机：Windows 11 x86_64（32 核），release 构建。
- 基线锁定：`bench-data/baseline.json`（负载哈希校验，漂移即拒）。
- 门禁：`python scripts/gate.py`（回归 >1% 红门）。

> 诚实边界：loopback 没有真实网络拥塞与网卡优先级队列，本 benchmark 展示的是
> **架构特性**（短 RPC 不被长 RPC 阻塞），不声称复现 Homa 论文在数据中心交换网络
> 下的 19–72× 数字；1% 丢包下的 P99 恢复数据来自网络模拟器（随 B2 fuzz 一并发布）。

## 文档

- [`WIRE.md`](WIRE.md) —— 线格式规范（IETF agent 传输 draft v00 底稿）
- `src/transport/` —— 传输核心（纯状态机：packet / priority / sender / receiver / txqueue）
- `src/rpc/` —— 薄 RPC 层（RpcClient / RpcServer + TCP 对照基线）

## 许可

MIT OR Apache-2.0
