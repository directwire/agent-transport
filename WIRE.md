# agent-transport 线格式（Wire Format）

> 本文档是 `src/transport/packet.rs` 的规范描述，也是 IETF agent 传输
> draft v00 的线格式底稿。**实现与文档以 `packet.rs` 为准，改码必改文。**

## 传输单位

- 载体：**UDP 数据报**（单条消息切分为定长分片，每分片一个数据报）。
- 默认路径 MTU 预算：`packet_size = 1200` 字节（含 22 字节头 + 负载）。
- 字节序：**小端（little-endian）**，无对齐填充。

## 包头（22 字节，固定）

| 偏移 | 长度 | 字段 | 说明 |
|---|---|---|---|
| 0 | 1 | `type` | 包类型（见下） |
| 1 | 1 | `priority` | 8 级动态优先级，0 最高（仅 DATA 有效） |
| 2 | 8 | `msg_id` | 消息 ID，发送方分配，消息内唯一 |
| 10 | 4 | `msg_len` | DATA：消息总长度；控制包：0 |
| 14 | 4 | `offset` | 见字段复用表 |
| 18 | 4 | `length` | 见字段复用表 |

DATA 包在 22 字节头后携带负载；**DATA 负载长度必须等于 `length`**，否则解码报错。

## 包类型

| 值 | 类型 | 语义 |
|---|---|---|
| 0 | `DATA` | 携带消息的 `[offset, offset+length)` 分片；`msg_len` = 消息总长 |
| 1 | `GRANT` | 接收端授予发送端「可发送到 `offset`」的累计授权；`length` = 本次新增授权量 |
| 2 | `RESEND` | 接收端请求重发 `[offset, offset+length)` 分片（授予窗口内缺包） |
| 3 | `BUSY` | 接收端并发消息数超限，让发送端稍后重试 |

未知类型（>3）解码报错，丢弃。

## 优先级

`priority_for_len` 按消息总长映射 8 级（0 最高）：

| 级别 | 消息长度范围 |
|---|---|
| 0 | ≤ 100 B |
| 1 | ≤ 1 KB |
| 2 | ≤ 10 KB |
| 3 | ≤ 100 KB |
| 4 | ≤ 1 MB |
| 5 | ≤ 10 MB |
| 6 | ≤ 100 MB |
| 7 | > 100 MB |

控制包（GRANT/RESEND/BUSY）不入发送队列直发——小且影响调度活性。

## 调度语义（规范要点）

1. **免授权窗口（unscheduled）**：消息前 `unscheduled_bytes`（默认 10 KiB）首 RTT 直达，
   无需等待授权——**短消息单包往返**。
2. **授权驱动（GRANT/SRPT）**：窗口外字节必须等接收端累计授权。接收端按
   **剩余字节最少优先**（SRPT）选前 K 条消息发 GRANT（K = `overcommit`，默认 2），
   新到的更短消息可抢占长消息的授权节奏；累计授权保底，不饿死。
3. **丢包恢复**：授予窗口内缺包超 `resend_timeout` → `RESEND` 批量请求重发；
   授权无进展超 `grant_timeout` → 重发 `GRANT`；发送端停滞超 `poke_timeout` →
   重发最后分片作探针。发送端完成消息后保留 `linger` 期以响应迟到 RESEND。
4. **防饿死**：等待授权超过 `starve_threshold`（默认 200 ms）的消息强制进入授权集合。
5. **at-least-once**：RPC 层超时整请求重发（同一 `rpc_id`），服务端幂等去重缓存
   保证 handler 对每个 `rpc_id` 至多执行一次。

## 复现性

核心状态机（sender / receiver / txqueue）**无 IO 耦合、零随机性**，可直接以确定性
单元测试与 fuzz 驱动；网络侧仅 socket2 做缓冲与超时。benchmark 固定负载 + 固定
seed（见 `benches/mixed.rs`），任何人可重跑出同负载数字。

*agent-transport wire format v0.1 · 2026-08-18 · 确定性是承诺，贡献性是验收。*
