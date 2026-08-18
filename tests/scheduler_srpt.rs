//! GRANT 调度器 SRPT 正确性测试：纯状态机，无网络 IO，确定性。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use agent_transport::transport::packet::{Packet, PacketType};
use agent_transport::transport::receiver::ReceiverCore;
use agent_transport::transport::{Action, TransportConfig};

fn test_cfg() -> TransportConfig {
    TransportConfig {
        packet_size: 1000,
        unscheduled_bytes: 1000, // 每条消息只有第 1 个分片免授权
        grant_increment: 2000,   // 每次授予 2 个分片
        resend_timeout: Duration::from_millis(50),
        grant_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}

fn src() -> SocketAddr {
    "127.0.0.1:9999".parse().unwrap()
}

/// 构造消息 msg_id 的首个 DATA 分片（前 1000 字节）
fn first_chunk(msg_id: u64, total: u32) -> (Packet, Vec<u8>) {
    let payload = vec![msg_id as u8; 1000.min(total as usize)];
    (
        Packet::new(PacketType::Data, 0, msg_id, total, 0, payload.len() as u32),
        payload,
    )
}

/// 从动作列表中提取 GRANT 包
fn grants(actions: &[Action]) -> Vec<Packet> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { bytes, .. } => {
                let (pkt, _) = Packet::decode(bytes).ok()?;
                (pkt.typ == PacketType::Grant).then_some(pkt)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn 调度器按剩余字节数srpt授权() {
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(cfg);
    let now = Instant::now();
    let mut actions = Vec::new();

    // 三条消息同时到达：5MB / 1MB / 100KB（用缩小尺寸：5000/3000/2000 字节）
    let (p1, d1) = first_chunk(1, 5000);
    let (p2, d2) = first_chunk(2, 3000);
    let (p3, d3) = first_chunk(3, 2000);
    rx.handle_data(src(), &p1, &d1, now, &mut actions);
    rx.handle_data(src(), &p2, &d2, now, &mut actions);
    rx.handle_data(src(), &p3, &d3, now, &mut actions);

    // 最后到达的 msg3 最短（剩余 1000 字节）→ SRPT 应优先授权它
    let g = grants(&actions);
    assert!(!g.is_empty(), "应有 GRANT 发出");
    let last = g.last().unwrap();
    assert_eq!(last.msg_id, 3, "SRPT 应优先授权剩余字节最少的 msg3");
    // 累计授权偏移 = 已收 1000 + 增量 2000 = 3000，封顶 2000
    assert_eq!(last.offset, 2000);
}

#[test]
fn 短消息可抢占长消息的授权() {
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(cfg);
    let now = Instant::now();
    let mut actions = Vec::new();

    // 先只有长消息 msg1（5000B），它被授权
    let (p1, d1) = first_chunk(1, 5000);
    rx.handle_data(src(), &p1, &d1, now, &mut actions);
    assert_eq!(grants(&actions).last().unwrap().msg_id, 1);

    // 短消息 msg2（2000B）到达 → 抢占
    actions.clear();
    let (p2, d2) = first_chunk(2, 2000);
    rx.handle_data(src(), &p2, &d2, now, &mut actions);
    let g = grants(&actions);
    assert_eq!(g.last().unwrap().msg_id, 2, "更短的 msg2 应抢占授权");
}

#[test]
fn 随进度推进授权窗口且完成后切换下一条() {
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(cfg.clone());
    let now = Instant::now();
    let mut actions = Vec::new();

    // msg1 = 3000B（3 分片），msg2 = 5000B（5 分片）
    let (p1, d1) = first_chunk(1, 3000);
    rx.handle_data(src(), &p1, &d1, now, &mut actions);
    let (p2, d2) = first_chunk(2, 5000);
    rx.handle_data(src(), &p2, &d2, now, &mut actions);

    // overcommit=2：两条消息都在授权集合内，各自拿到授权窗口 1000+2000=3000
    let g = grants(&actions);
    assert_eq!(g.iter().filter(|p| p.msg_id == 1).last().unwrap().offset, 3000);
    assert_eq!(g.iter().filter(|p| p.msg_id == 2).last().unwrap().offset, 3000);

    // 模拟发送端按授权把 msg1 剩余两个分片发完 → msg1 交付
    actions.clear();
    for i in 1..3u32 {
        let payload = vec![1u8; 1000];
        let p = Packet::new(PacketType::Data, 0, 1, 3000, i * 1000, 1000);
        rx.handle_data(src(), &p, &payload, now, &mut actions);
    }
    // msg1 应已交付
    assert!(actions.iter().any(|a| matches!(a, Action::Deliver { msg_id: 1, .. })));

    // msg2 继续按授权窗口推进：再补两个分片（收到 3000），在途额度耗尽后应追加 GRANT 到 5000
    actions.clear();
    for i in 1..3u32 {
        let payload = vec![2u8; 1000];
        let p = Packet::new(PacketType::Data, 0, 2, 5000, i * 1000, 1000);
        rx.handle_data(src(), &p, &payload, now, &mut actions);
    }
    let g = grants(&actions);
    assert_eq!(
        g.iter().filter(|p| p.msg_id == 2).last().map(|p| p.offset),
        Some(5000),
        "msg2 窗口耗尽后应追加授权到全长"
    );
}

#[test]
fn 短消息全程无需授权直接交付() {
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(cfg);
    let now = Instant::now();
    let mut actions = Vec::new();

    // 900B < unscheduled 1000B：一个分片直接收全
    let payload = vec![7u8; 900];
    let p = Packet::new(PacketType::Data, 0, 42, 900, 0, 900);
    rx.handle_data(src(), &p, &payload, now, &mut actions);

    assert!(actions.iter().any(|a| matches!(a, Action::Deliver { msg_id: 42, data, .. } if data == &payload)));
    assert!(grants(&actions).is_empty(), "短消息不应触发 GRANT");
}

#[test]
fn overcommit同时授权前k短消息() {
    // overcommit=2：三条消息，新一轮调度应只覆盖最短的两条
    let cfg = TransportConfig { overcommit: 2, ..test_cfg() };
    let mut rx = ReceiverCore::new(cfg);
    let now = Instant::now();
    let mut actions = Vec::new();

    // msg1 先到，独占授权集合时拿到过授权（正常）
    let (p1, d1) = first_chunk(1, 9000);
    rx.handle_data(src(), &p1, &d1, now, &mut actions);
    let (p2, d2) = first_chunk(2, 5000);
    rx.handle_data(src(), &p2, &d2, now, &mut actions);
    actions.clear();
    // msg3（最短）到达后的一轮调度：前 K=2 = {msg3, msg2}，msg1 被挤出
    let (p3, d3) = first_chunk(3, 2000);
    rx.handle_data(src(), &p3, &d3, now, &mut actions);
    let g = grants(&actions);
    assert!(g.iter().any(|p| p.msg_id == 3), "最短的 msg3 必须获得授权");
    assert!(!g.iter().any(|p| p.msg_id == 1), "最长的 msg1 不应在本轮再获新授权");
}

#[test]
fn 长消息授权耗尽且等待超阈值后强制获得授权() {
    // overcommit=1：msg1（长）授权窗口耗尽后 msg2（短）独占授权集合；
    // msg1 无窗口可用且等待超过 starve_threshold → 必须被强制授权（防饿死）
    let cfg = TransportConfig {
        overcommit: 1,
        starve_threshold: Duration::from_millis(50),
        ..test_cfg()
    };
    let mut rx = ReceiverCore::new(cfg.clone());
    let t0 = Instant::now();
    let mut actions = Vec::new();

    // t0: msg1 首分片 → 独占授权集合，拿到窗口 3000
    let (p1, d1) = first_chunk(1, 9000);
    rx.handle_data(src(), &p1, &d1, t0, &mut actions);
    // t0+10: 更短的 msg2 到达 → 抢占授权集合（msg1 窗口还剩 2000 在途，不算饿死）
    let (p2, d2) = first_chunk(2, 5000);
    rx.handle_data(src(), &p2, &d2, t0 + Duration::from_millis(10), &mut actions);
    // t0+20: msg1 把在途窗口用尽（收到 3000 = granted_to），此后来不到新授权 → 开始挨饿
    for i in 1..3u32 {
        let payload = vec![1u8; 1000];
        let p = Packet::new(PacketType::Data, 0, 1, 9000, i * 1000, 1000);
        rx.handle_data(src(), &p, &payload, t0 + Duration::from_millis(20), &mut actions);
    }
    actions.clear();

    // t0+70（超过 starve_threshold）：msg2 再来一个分片触发调度
    let t2 = t0 + cfg.starve_threshold + Duration::from_millis(20);
    let payload = vec![2u8; 1000];
    let p = Packet::new(PacketType::Data, 0, 2, 5000, 1000, 1000);
    rx.handle_data(src(), &p, &payload, t2, &mut actions);

    let g = grants(&actions);
    assert!(
        g.iter().any(|p| p.msg_id == 1),
        "msg1 授权耗尽且等待超阈值后必须被强制授权，防饿死"
    );
}
