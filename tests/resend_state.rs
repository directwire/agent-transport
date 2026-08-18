//! 丢包重传状态机测试：RESEND 生成、发送端重发、GRANT 丢失重发、端到端 loopback 丢包恢复。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use agent_transport::transport::packet::{Packet, PacketType};
use agent_transport::transport::receiver::ReceiverCore;
use agent_transport::transport::sender::SenderCore;
use agent_transport::transport::{Action, Transport, TransportConfig};

fn test_cfg() -> TransportConfig {
    TransportConfig {
        packet_size: 100,
        unscheduled_bytes: 200, // 前 2 个分片免授权
        grant_increment: 200,
        resend_timeout: Duration::from_millis(50),
        grant_timeout: Duration::from_millis(80),
        poke_timeout: Duration::from_millis(50),
        ..Default::default()
    }
}

fn peer() -> SocketAddr {
    "127.0.0.1:7777".parse().unwrap()
}

fn sends_of(actions: &[Action], typ: PacketType) -> Vec<Packet> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { bytes, .. } => {
                let (pkt, _) = Packet::decode(bytes).ok()?;
                (pkt.typ == typ).then_some(pkt)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn 授予窗口内缺包触发resend() {
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(cfg.clone());
    let t0 = Instant::now();
    let mut actions = Vec::new();

    // 500B 消息（5 分片），只收到分片 0，分片 1 丢失
    let d = vec![1u8; 100];
    let p = Packet::new(PacketType::Data, 0, 1, 500, 0, 100);
    rx.handle_data(peer(), &p, &d, t0, &mut actions);
    // 授权节流语义下：unscheduled=200，已收 100 → 在途 100 >= 增量200/2，暂不追加 GRANT，
    // 授予窗口 = 200，分片 1（offset 100）在授予窗口内

    // 快进超过 resend_timeout → 应对 offset 100 发 RESEND
    let t1 = t0 + cfg.resend_timeout + Duration::from_millis(10);
    actions.clear();
    rx.tick(t1, &mut actions);
    let resends = sends_of(&actions, PacketType::Resend);
    assert_eq!(resends.len(), 1, "应恰好发出一个 RESEND");
    assert_eq!(resends[0].offset, 100, "RESEND 应指向第一个缺失分片");
    // 批量修复：长度覆盖授予窗口内剩余范围（授予窗口=200，故为 100）
    assert_eq!(resends[0].length, 100);
}

#[test]
fn 发送端响应resend重发指定分片() {
    let cfg = test_cfg();
    let mut tx = SenderCore::new(cfg.clone());
    let t0 = Instant::now();

    // 发送 500B 消息：unscheduled 200B → 立即发出分片 0、1
    let data: Vec<u8> = (0..500u32).map(|i| (i % 253) as u8).collect();
    let actions = tx.start(peer(), 1, data.clone(), t0);
    let datas = sends_of(&actions, PacketType::Data);
    assert_eq!(datas.len(), 2, "首 RTT 只发 unscheduled 窗口");
    assert!(!tx.is_done(peer(), 1));

    // 对端请求重发 offset 100 起 100B → 应重发分片 1
    let mut actions = Vec::new();
    let resend_req = Packet::new(PacketType::Resend, 0, 1, 0, 100, 100);
    tx.handle_resend(peer(), &resend_req, t0, &mut actions);
    let datas = sends_of(&actions, PacketType::Data);
    assert_eq!(datas.len(), 1);
    assert_eq!(datas[0].offset, 100);
    // 重发内容与原数据一致
    if let Action::Send { bytes, .. } = &actions[0] {
        let (_, payload) = Packet::decode(bytes).unwrap();
        assert_eq!(payload, &data[100..200]);
    } else {
        panic!("应为发包动作");
    }
}

#[test]
fn grant驱动发送端推进并完成() {
    let cfg = test_cfg();
    let mut tx = SenderCore::new(cfg);
    let t0 = Instant::now();

    let data = vec![5u8; 500];
    tx.start(peer(), 7, data, t0);
    assert!(!tx.is_done(peer(), 7));

    // 收到累计授权到 400 → 发出分片 2、3
    let mut actions = Vec::new();
    let grant = Packet::new(PacketType::Grant, 0, 7, 0, 400, 200);
    tx.handle_grant(peer(), &grant, t0, &mut actions);
    assert_eq!(sends_of(&actions, PacketType::Data).len(), 2);
    assert!(!tx.is_done(peer(), 7));

    // 授权到 500 → 发出最后一个分片，done
    actions.clear();
    let grant = Packet::new(PacketType::Grant, 0, 7, 0, 500, 100);
    tx.handle_grant(peer(), &grant, t0, &mut actions);
    assert_eq!(sends_of(&actions, PacketType::Data).len(), 1);
    assert!(tx.is_done(peer(), 7));
}

#[test]
fn 停滞超时后接收端发出恢复动作() {
    // 场景：消息只到了分片 0 之后完全停滞（无论丢的是 DATA 还是 GRANT）。
    // 接收端 tick 超时后必须主动恢复：授予窗口内缺包 → RESEND；授权未推进 → 重发 GRANT。
    let cfg = test_cfg();
    let mut rx = ReceiverCore::new(TransportConfig {
        unscheduled_bytes: 100, // 只免授权分片 0
        ..cfg.clone()
    });
    let t0 = Instant::now();
    let mut actions = Vec::new();

    let d = vec![2u8; 100];
    let p = Packet::new(PacketType::Data, 0, 9, 500, 0, 100);
    rx.handle_data(peer(), &p, &d, t0, &mut actions);
    // handle_data 内的 schedule 已发出过 GRANT(300)，假设它丢了
    assert!(!sends_of(&actions, PacketType::Grant).is_empty());
    actions.clear();

    let t1 = t0 + cfg.grant_timeout + Duration::from_millis(20);
    rx.tick(t1, &mut actions);
    let recovery =
        sends_of(&actions, PacketType::Grant).len() + sends_of(&actions, PacketType::Resend).len();
    assert!(recovery >= 1, "停滞超时后接收端必须发出恢复动作（GRANT 或 RESEND）");
}

#[test]
fn loopback端到端大消息经grant送达() {
    // 真实 UDP loopback：400KB 消息远超 unscheduled 窗口，必须走完整 GRANT 流程
    let cfg = TransportConfig::default();
    let server = Transport::bind("127.0.0.1:0", cfg.clone()).unwrap();
    let client = Transport::bind("127.0.0.1:0", cfg).unwrap();
    let server_addr = server.local_addr().unwrap();

    let data: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
    let d = data.clone();
    let h = std::thread::spawn(move || client.send_to(server_addr, &d).unwrap());

    let (_src, got) = server.recv(Duration::from_secs(15)).unwrap();
    h.join().unwrap();
    assert_eq!(got.len(), data.len());
    assert_eq!(got, data);
}
