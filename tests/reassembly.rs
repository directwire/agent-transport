//! 乱序重组测试：分片乱序、重复、缺口场景下的重组正确性。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use agent_transport::transport::packet::{Packet, PacketType};
use agent_transport::transport::receiver::ReceiverCore;
use agent_transport::transport::{Action, TransportConfig};

fn test_cfg() -> TransportConfig {
    TransportConfig {
        packet_size: 100,
        unscheduled_bytes: 1000, // 全部免授权，聚焦重组逻辑
        resend_timeout: Duration::from_millis(50),
        ..Default::default()
    }
}

fn src() -> SocketAddr {
    "127.0.0.1:8888".parse().unwrap()
}

fn chunk(msg_id: u64, total: u32, offset: u32, payload: &[u8]) -> (Packet, Vec<u8>) {
    (
        Packet::new(PacketType::Data, 0, msg_id, total, offset, payload.len() as u32),
        payload.to_vec(),
    )
}

fn delivers(actions: &[Action]) -> Vec<Vec<u8>> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Deliver { data, .. } => Some(data.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn 乱序分片正确重组() {
    let mut rx = ReceiverCore::new(test_cfg());
    let now = Instant::now();
    let mut actions = Vec::new();

    // 原始消息 350B，4 个分片：[0,100) [100,200) [200,300) [300,350)
    let data: Vec<u8> = (0..350u32).map(|i| (i % 251) as u8).collect();
    // 乱序投递：2, 0, 3, 1
    for (off, len) in [(200u32, 100u32), (0, 100), (300, 50), (100, 100)] {
        let (p, d) = chunk(1, 350, off, &data[off as usize..(off + len) as usize]);
        rx.handle_data(src(), &p, &d, now, &mut actions);
        if off != 100 {
            assert!(delivers(&actions).is_empty(), "缺口未补齐前不应交付");
        }
    }
    let d = delivers(&actions);
    assert_eq!(d.len(), 1, "恰好交付一次");
    assert_eq!(d[0], data, "重组内容必须与原消息一致");
}

#[test]
fn 重复分片幂等且只交付一次() {
    let mut rx = ReceiverCore::new(test_cfg());
    let now = Instant::now();
    let mut actions = Vec::new();

    let data = vec![9u8; 250];
    // 同一分片重复投 3 次
    for _ in 0..3 {
        let (p, d) = chunk(1, 250, 0, &data[0..100]);
        rx.handle_data(src(), &p, &d, now, &mut actions);
    }
    for _ in 0..2 {
        let (p, d) = chunk(1, 250, 100, &data[100..200]);
        rx.handle_data(src(), &p, &d, now, &mut actions);
    }
    let (p, d) = chunk(1, 250, 200, &data[200..250]);
    rx.handle_data(src(), &p, &d, now, &mut actions);
    // 交付后再来迟到重复包也不应二次交付
    let (p, d) = chunk(1, 250, 0, &data[0..100]);
    rx.handle_data(src(), &p, &d, now, &mut actions);

    let d = delivers(&actions);
    assert_eq!(d.len(), 1);
    assert_eq!(d[0], data);
}

#[test]
fn 多条消息交错到达互不串扰() {
    let mut rx = ReceiverCore::new(test_cfg());
    let now = Instant::now();
    let mut actions = Vec::new();

    let a: Vec<u8> = vec![1u8; 200];
    let b: Vec<u8> = vec![2u8; 200];
    // 两条消息分片交错
    let (p, d) = chunk(1, 200, 100, &a[100..200]);
    rx.handle_data(src(), &p, &d, now, &mut actions);
    let (p, d) = chunk(2, 200, 0, &b[0..100]);
    rx.handle_data(src(), &p, &d, now, &mut actions);
    let (p, d) = chunk(1, 200, 0, &a[0..100]);
    rx.handle_data(src(), &p, &d, now, &mut actions);
    let (p, d) = chunk(2, 200, 100, &b[100..200]);
    rx.handle_data(src(), &p, &d, now, &mut actions);

    let d = delivers(&actions);
    assert_eq!(d.len(), 2);
    assert!(d.contains(&a) && d.contains(&b));
}
