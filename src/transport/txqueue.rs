//! 发送侧 8 级优先级 QoS 队列：DATA 包按包头优先级入队，高优先级（编号小）先出队。
//!
//! 控制包（GRANT/RESEND/BUSY）不入队直发——它们小且影响调度活性。
//! 队列在授权窗口突发（长消息低优先级批量分片）时形成积压，
//! 此时短消息（高优先级）的未调度分片可以插队，QoS 由此真实生效。

use std::collections::VecDeque;
use std::net::SocketAddr;

use super::packet::PacketType;
use super::priority::NUM_PRIORITIES;

/// 单次冲刷的最大包数。限额让同批积压内高优先级包插队（QoS 生效窗口），
/// 但不宜过小：backlog 主要靠收包迭代与 5ms tick 消耗，太小会节流长消息
pub const FLUSH_BUDGET: usize = 256;

/// 8 级优先级发送队列
pub struct TxQueues {
    queues: [VecDeque<(SocketAddr, Vec<u8>)>; NUM_PRIORITIES as usize],
}

impl TxQueues {
    pub fn new() -> Self {
        Self { queues: Default::default() }
    }

    /// 把一个待发包按类型归类：DATA 按优先级入队，控制包返回给调用方直发
    pub fn classify(&mut self, dest: SocketAddr, bytes: Vec<u8>) -> Option<(SocketAddr, Vec<u8>)> {
        // 头两字节 = 类型 + 优先级（见 packet.rs 线格式）
        if bytes.len() >= 2 && bytes[0] == PacketType::Data as u8 {
            let prio = bytes[1].min(NUM_PRIORITIES - 1);
            self.queues[prio as usize].push_back((dest, bytes));
            None
        } else {
            Some((dest, bytes))
        }
    }

    /// 按优先级从高到低弹出至多 budget 个包（同级 FIFO）
    pub fn pop_batch(&mut self, budget: usize) -> Vec<(SocketAddr, Vec<u8>)> {
        let mut out = Vec::new();
        for q in &mut self.queues {
            while out.len() < budget {
                match q.pop_front() {
                    Some(item) => out.push(item),
                    None => break,
                }
            }
            if out.len() >= budget {
                break;
            }
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// 观测用：积压总数
    pub fn backlog(&self) -> usize {
        self.queues.iter().map(|q| q.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::packet::Packet;

    fn data_pkt(prio: u8, msg_id: u64) -> Vec<u8> {
        Packet::new(PacketType::Data, prio, msg_id, 100, 0, 4).encode(b"data")
    }

    #[test]
    fn 高优先级先出队() {
        let mut tx = TxQueues::new();
        let dest: SocketAddr = "127.0.0.1:1".parse().unwrap();
        // 乱序入队：5, 0, 3, 7, 0
        for (prio, id) in [(5u8, 1u64), (0, 2), (3, 3), (7, 4), (0, 5)] {
            assert!(tx.classify(dest, data_pkt(prio, id)).is_none());
        }
        let batch = tx.pop_batch(100);
        let order: Vec<u64> = batch
            .iter()
            .map(|(_, b)| Packet::decode(b).unwrap().0.msg_id)
            .collect();
        // 优先级 0 的两个按 FIFO 在前，然后 3、5、7
        assert_eq!(order, vec![2, 5, 3, 1, 4]);
        assert!(tx.is_empty());
    }

    #[test]
    fn 限额制造积压且高优先级插队() {
        let mut tx = TxQueues::new();
        let dest: SocketAddr = "127.0.0.1:1".parse().unwrap();
        // 先入队 FLUSH_BUDGET+40 个低优先级（长消息突发），超过单次 budget
        for i in 0..(FLUSH_BUDGET as u64 + 40) {
            tx.classify(dest, data_pkt(7, i));
        }
        // 再来 1 个高优先级（短 RPC）
        tx.classify(dest, data_pkt(0, 999));
        let batch = tx.pop_batch(FLUSH_BUDGET);
        assert_eq!(batch.len(), FLUSH_BUDGET);
        // 高优先级包必须在本批第一个出队，而不是排在所有低优先级之后
        assert_eq!(Packet::decode(&batch[0].1).unwrap().0.msg_id, 999);
        assert_eq!(tx.backlog(), 40 + 1);
    }

    #[test]
    fn 控制包直发不入队() {
        let mut tx = TxQueues::new();
        let dest: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let grant = Packet::new(PacketType::Grant, 0, 1, 0, 100, 100).encode(&[]);
        assert!(tx.classify(dest, grant).is_some());
        assert!(tx.is_empty());
    }
}
