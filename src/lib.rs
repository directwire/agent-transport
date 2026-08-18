//! # agent-transport
//!
//! **消息导向的 Agent 传输层 —— MCP / A2A 的 QUIC 式快速通道。**
//!
//! A message-oriented agent transport: a QUIC-style fast channel for
//! [MCP](https://modelcontextprotocol.io) / [A2A](https://a2a-protocol.org)
//! agent messaging, designed for **single-packet round-trips**, **8-level QoS**,
//! and **loss resilience** — built for deterministic recognition and
//! reproducible performance.
//!
//! ## 为什么需要它（Why）
//!
//! 今天的 agent 通信（MCP / A2A）跑在 HTTP/SSE/WebSocket 之上：字节流 + 每消息
//! 帧头 + 队头阻塞。`agent-transport` 把**消息当作一等公民**：短消息在免授权
//! 窗口内首 RTT 直达（单包往返），长消息由接收端 GRANT/SRPT 调度，8 级优先级
//! 队列让高优先级短 RPC 在长传输的授权突发中插队——**短 RPC 不被长传输阻塞**。
//!
//! 架构参考：Stanford 的 [Homa 传输协议](https://homa-transport.org)（Ousterhout
//! et al.），此处实现为 UDP 上的 Homa-lite（保留 GRANT/SRPT/8 级优先级的消息
//! 语义，去掉数据中心专用机制）。
//!
//! ## 核心特性
//!
//! - **单包往返**：`≤ unscheduled_bytes` 的消息首 RTT 即送达，免授权。
//! - **多路复用**：64-bit `msg_id` 在一条 UDP socket 上并发任意多消息。
//! - **丢包弹性**：接收端驱动 RESEND / GRANT，批量修复突发丢包（含授权窗口内）。
//! - **8 级优先级 QoS**：发送侧分级队列，高优先级（短 RPC）先出队。
//! - **at-least-once + 幂等去重**：超时整请求重发（rpc_id 不变），服务端缓存去重，
//!   业务 handler 可幂等。
//! - **确定性**：核心为纯状态机（无 IO 耦合），测试与 benchmark 全部固定 seed、
//!   固定输入集、位级可复现。
//!
//! ## 分层
//!
//! - [`transport`](transport/index.html)：传输核心（`Transport` + 纯状态机）。
//! - [`rpc`](rpc/index.html)：薄 RPC 层（`RpcClient` / `RpcServer`，at-least-once）。
//!
//! ## 可复现的数字
//!
//! 出厂自带确定性 benchmark（`cargo bench --bench mixed`）：固定负载、固定 seed，
//! 输出对比 HTTP/TCP 的 P50/P90/P99——**任何人可重跑**。见 `bench-data/baseline.json`。
//!
//! ## 许可
//!
//! MIT OR Apache-2.0

pub mod rpc;
pub mod transport;

pub use rpc::{RpcClient, RpcServer};
pub use transport::{Transport, TransportConfig};
