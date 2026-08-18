//! RPC 层端到端测试（loopback）：echo、并发、超时重传与幂等去重。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agent_transport::rpc::{RpcClient, RpcServer};

#[test]
fn echo短请求响应() {
    let server = RpcServer::spawn("127.0.0.1:0", |req| {
        let mut r = b"pong:".to_vec();
        r.extend_from_slice(req);
        r
    })
    .unwrap();
    let client = RpcClient::new("127.0.0.1:0").unwrap();

    let resp = client.call(server.addr(), b"ping").unwrap();
    assert_eq!(resp, b"pong:ping");
}

#[test]
fn echo大消息走完整授权流程() {
    let server = RpcServer::spawn("127.0.0.1:0", |req| req.to_vec()).unwrap();
    let client = RpcClient::new("127.0.0.1:0").unwrap();

    let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 241) as u8).collect();
    let resp = client
        .call_with_timeout(server.addr(), &payload, Duration::from_secs(10), 3)
        .unwrap();
    assert_eq!(resp, payload);
}

#[test]
fn 多线程并发调用互串() {
    let server = Arc::new(RpcServer::spawn("127.0.0.1:0", |req| req.to_vec()).unwrap());
    let client = Arc::new(RpcClient::new("127.0.0.1:0").unwrap());

    let mut handles = Vec::new();
    for t in 0..8 {
        let c = Arc::clone(&client);
        let addr = server.addr();
        handles.push(std::thread::spawn(move || {
            for i in 0..20u32 {
                let payload = format!("thread-{t}-call-{i}").into_bytes();
                let resp = c.call(addr, &payload).unwrap();
                assert_eq!(resp, payload, "响应必须与请求一一对应");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn 超时重传至少一次且服务端去重() {
    // handler 故意睡 600ms（超过客户端单次超时 100ms）→ 客户端必然重试。
    // 服务端幂等缓存应保证 handler 对每个 rpc_id 只执行一次。
    let calls = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::clone(&calls);
    let server = RpcServer::spawn("127.0.0.1:0", move |req| {
        c2.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(600));
        req.to_vec()
    })
    .unwrap();
    let mut client = RpcClient::new("127.0.0.1:0").unwrap();
    client.attempt_timeout = Duration::from_millis(100);
    client.max_attempts = 10;

    let resp = client.call(server.addr(), b"slow").unwrap();
    assert_eq!(resp, b"slow");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "幂等去重：重试不应导致 handler 重算");
}

#[test]
fn 服务端不在时超时失败而非挂死() {
    let mut client = RpcClient::new("127.0.0.1:0").unwrap();
    client.attempt_timeout = Duration::from_millis(100);
    client.max_attempts = 3;
    let dead: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let err = client.call(dead, b"x").unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
}
