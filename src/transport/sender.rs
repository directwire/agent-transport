//! 发送端核心：无 IO 纯状态机。
//!
//! 职责：
//! - 消息切分为定长分片（chunk），首 RTT 的 unscheduled 窗口立即发出；
//! - 之后的字节必须等接收端 GRANT（累计授权）才能发；
//! - 响应 RESEND 重发丢失分片；
//! - 停滞时重发最后一个已发分片作为「探针」（at-least-once 的发送端正）；
//! - 发完后保留状态一小段 linger 期：尾部分片若丢失，接收端迟到的 RESEND 仍能触发重发，
//!   否则「发完即销毁」会造成对端永久等包（Homa 中由 DONE/ACK 机制解决，这里简化）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use super::packet::{Packet, PacketType};
use super::priority::priority_for_len;
use super::{Action, TransportConfig};

/// 发送中的单条消息状态
#[derive(Debug)]
struct OutMsg {
    data: Vec<u8>,
    /// 接收端累计授权到的偏移（不含）
    granted_to: usize,
    /// 下一个待发送偏移
    next_send: usize,
    /// 全部发出的时刻（进入 linger 期）；None = 还在发
    done_at: Option<Instant>,
    /// 上次有发送进展的时刻（用于停滞探测）
    last_progress: Instant,
}

impl OutMsg {
    /// offset 所在分片的 [start, end)
    fn chunk_range(&self, offset: usize, packet_size: usize) -> (usize, usize) {
        let start = (offset / packet_size) * packet_size;
        (start, (start + packet_size).min(self.data.len()))
    }
}

/// 发送端核心状态机
pub struct SenderCore {
    cfg: TransportConfig,
    out: HashMap<(SocketAddr, u64), OutMsg>,
}

/// 构造一个 DATA 包（自由函数，规避借用冲突）
fn data_packet(data: &[u8], msg_id: u64, start: usize, end: usize) -> Vec<u8> {
    let payload = &data[start..end];
    Packet::new(
        PacketType::Data,
        priority_for_len(data.len()),
        msg_id,
        data.len() as u32,
        start as u32,
        payload.len() as u32,
    )
    .encode(payload)
}

impl SenderCore {
    pub fn new(cfg: TransportConfig) -> Self {
        Self { cfg, out: HashMap::new() }
    }

    /// 启动一条新消息：立即发出 unscheduled 窗口内的所有分片（首 RTT 不等授权）
    pub fn start(&mut self, dest: SocketAddr, msg_id: u64, data: Vec<u8>, now: Instant) -> Vec<Action> {
        let unscheduled = self.cfg.unscheduled_bytes.min(data.len());
        let msg = OutMsg {
            data,
            granted_to: unscheduled,
            next_send: 0,
            done_at: None,
            last_progress: now,
        };
        let key = (dest, msg_id);
        self.out.insert(key, msg);
        let mut actions = Vec::new();
        self.pump(key, unscheduled, now, &mut actions);
        actions
    }

    /// 处理 GRANT：推进累计授权，补发新解锁的分片
    pub fn handle_grant(&mut self, src: SocketAddr, pkt: &Packet, now: Instant, actions: &mut Vec<Action>) {
        let key = (src, pkt.msg_id);
        let new_limit = (pkt.offset as usize).min(self.out.get(&key).map_or(0, |m| m.data.len()));
        let Some(msg) = self.out.get(&key) else { return };
        if new_limit <= msg.granted_to {
            return; // 重复/过期的累计授权
        }
        self.pump(key, new_limit, now, actions);
    }

    /// 处理 RESEND：重发 [offset, offset+length) 覆盖的分片。
    /// 注意：对端的授权视角可能超前于本地 next_send（末尾 GRANT 丢失时），
    /// RESEND 范围仍在授权窗口内，重发是合法的——但必须把这部分补进记账，
    /// 否则对端收全交付后不再授权，本地 next_send 永远追不上，造成停滞。
    pub fn handle_resend(&mut self, src: SocketAddr, pkt: &Packet, now: Instant, actions: &mut Vec<Action>) {
        let key = (src, pkt.msg_id);
        let pkt_size = self.cfg.packet_size;
        let Some(msg) = self.out.get(&key) else { return };
        let start = (pkt.offset as usize).min(msg.data.len());
        let end = (start + pkt.length as usize).min(msg.data.len());
        let mut off = start;
        while off < end {
            let (s, e) = msg.chunk_range(off, pkt_size);
            actions.push(Action::Send { dest: src, bytes: data_packet(&msg.data, pkt.msg_id, s, e) });
            off = e.max(s + 1);
        }
        let msg = self.out.get_mut(&key).unwrap();
        // RESEND 范围即授权范围：补记 granted_to 与 next_send
        msg.granted_to = msg.granted_to.max(end);
        if msg.next_send < end {
            msg.next_send = end;
            if msg.next_send >= msg.data.len() && msg.done_at.is_none() {
                msg.done_at = Some(now);
            }
        }
        msg.last_progress = now;
    }

    /// 处理 BUSY：接收端过载，重置停滞计时，稍后由 tick 探针重新发起
    pub fn handle_busy(&mut self, src: SocketAddr, pkt: &Packet, now: Instant) {
        if let Some(m) = self.out.get_mut(&(src, pkt.msg_id)) {
            m.last_progress = now;
        }
    }

    /// 周期滴答：对停滞消息重发最后一个已发分片作探针，触发对端 RESEND/GRANT；
    /// 同时回收 linger 期已过的已完成消息
    pub fn tick(&mut self, now: Instant, actions: &mut Vec<Action>) {
        let timeout = self.cfg.poke_timeout;
        let linger = self.cfg.linger;
        let pkt_size = self.cfg.packet_size;
        // 回收 linger 期结束的完成消息
        self.out.retain(|_, m| m.done_at.is_none_or(|t| now.duration_since(t) < linger));
        let keys: Vec<_> = self.out.keys().copied().collect();
        for key in keys {
            let Some(msg) = self.out.get(&key) else { continue };
            if msg.done_at.is_some() || now.duration_since(msg.last_progress) < timeout {
                continue;
            }
            let last_off = if msg.next_send == 0 { 0 } else { ((msg.next_send - 1) / pkt_size) * pkt_size };
            let (s, e) = msg.chunk_range(last_off, pkt_size);
            actions.push(Action::Send { dest: key.0, bytes: data_packet(&msg.data, key.1, s, e) });
            self.out.get_mut(&key).unwrap().last_progress = now;
        }
    }

    /// 消息是否已完全发出（linger 期内的消息视为 done，但仍可响应 RESEND）
    pub fn is_done(&self, dest: SocketAddr, msg_id: u64) -> bool {
        self.out.get(&(dest, msg_id)).is_none_or(|m| m.done_at.is_some())
    }

    /// 立即回收（超时放弃等场景）；正常完成的消息靠 linger 自动回收
    pub fn finish(&mut self, dest: SocketAddr, msg_id: u64) {
        self.out.remove(&(dest, msg_id));
    }

    /// 调试快照
    pub fn debug_dump(&self) -> String {
        let mut s = String::new();
        for ((addr, id), m) in &self.out {
            s.push_str(&format!(
                "  [{addr}#{id}] len={} sent={}/{} granted_to={} done={:?}\n",
                m.data.len(),
                m.next_send,
                m.data.len(),
                m.granted_to,
                m.done_at.is_some()
            ));
        }
        s
    }

    /// 将 [next_send, limit) 区间内的分片发出
    fn pump(&mut self, key: (SocketAddr, u64), limit: usize, now: Instant, actions: &mut Vec<Action>) {
        let pkt_size = self.cfg.packet_size;
        let Some(msg) = self.out.get_mut(&key) else { return };
        if msg.done_at.is_some() {
            return;
        }
        let limit = limit.min(msg.data.len());
        while msg.next_send < limit {
            let (s, e) = (msg.next_send, (msg.next_send + pkt_size).min(msg.data.len()));
            actions.push(Action::Send { dest: key.0, bytes: data_packet(&msg.data, key.1, s, e) });
            msg.next_send = e;
        }
        msg.granted_to = msg.granted_to.max(limit);
        msg.last_progress = now;
        if msg.next_send >= msg.data.len() {
            msg.done_at = Some(now); // 进入 linger 期，继续响应迟到 RESEND
        }
    }
}
