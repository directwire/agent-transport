//! 极简 RPC 层：请求/响应抽象，构建在消息导向 Transport 之上。
//!
//! 语义：**at-least-once**。客户端超时未收到响应会整请求重发（同一个 rpc_id），
//! 服务端用 (client, rpc_id) 去重缓存直接回放已算好的响应。
//! ⇒ 代价是 handler 可能被调用多次，**业务 handler 必须幂等**；
//!   若需要 exactly-once 效果，请在业务层基于 rpc_id 做去重/状态机。

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::transport::Transport;

/// RPC 帧头长度：8 字节 rpc_id
const RPC_HDR: usize = 8;

/// 默认每次调用的单次尝试超时
const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
/// 默认最大尝试次数（at-least-once 重传）
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

fn encode_frame(rpc_id: u64, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(RPC_HDR + body.len());
    v.extend_from_slice(&rpc_id.to_le_bytes());
    v.extend_from_slice(body);
    v
}

fn decode_frame(frame: &[u8]) -> Option<(u64, &[u8])> {
    if frame.len() < RPC_HDR {
        return None;
    }
    Some((u64::from_le_bytes(frame[..8].try_into().unwrap()), &frame[RPC_HDR..]))
}

/// RPC 服务端：收请求 → 调 handler → 回响应；带幂等去重缓存。
pub struct RpcServer {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RpcServer {
    /// 绑定并启动服务线程。handler 在独立线程中被并发调用（每请求一个线程）。
    pub fn spawn<F>(bind: &str, handler: F) -> io::Result<Self>
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        let transport = Arc::new(Transport::bind(bind, Default::default())?);
        let addr = transport.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = Arc::clone(&shutdown);
        let handler = Arc::new(handler);

        let thread = std::thread::Builder::new()
            .name("homa-rpc-server".into())
            .spawn(move || {
                // 幂等去重缓存：(client, rpc_id) -> None=计算中 / Some=已完成响应。
                // 计算中的重复请求直接丢弃——响应算好后用同一 rpc_id 送达，
                // 客户端重试时复用同一 rpc_id，仍能收到，handler 只执行一次。
                let dedup: Arc<Mutex<HashMap<(SocketAddr, u64), Option<Vec<u8>>>>> =
                    Arc::new(Mutex::new(HashMap::new()));
                while !sd.load(Ordering::Relaxed) {
                    let Ok((src, frame)) = transport.recv(Duration::from_millis(50)) else { continue };
                    let Some((rpc_id, body)) = decode_frame(&frame) else { continue };
                    {
                        let mut map = dedup.lock().unwrap();
                        match map.get(&(src, rpc_id)) {
                            Some(Some(cached)) => {
                                // 已完成：直接回放缓存响应（幂等保护）
                                let resp = cached.clone();
                                let tp = Arc::clone(&transport);
                                drop(map);
                                std::thread::spawn(move || {
                                    let _ = tp.send_to(src, &resp);
                                });
                                continue;
                            }
                            Some(None) => continue, // 计算中：丢弃重复请求
                            None => {
                                map.insert((src, rpc_id), None); // 占位，防并发重算
                            }
                        }
                    }
                    let h = Arc::clone(&handler);
                    let tp = Arc::clone(&transport);
                    let dd = Arc::clone(&dedup);
                    let body = body.to_vec();
                    // 每请求一个工作线程：长请求/长响应不阻塞接收循环
                    std::thread::spawn(move || {
                        let resp_body = h(&body);
                        let resp = encode_frame(rpc_id, &resp_body);
                        let _ = tp.send_to(src, &resp);
                        dd.lock().unwrap().insert((src, rpc_id), Some(resp));
                    });
                }
            })?;
        Ok(Self { addr, shutdown, thread: Some(thread) })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// RPC 客户端：内部有分发线程，按 rpc_id 把响应路由到对应调用，支持多线程并发 call。
pub struct RpcClient {
    transport: Arc<Transport>,
    next_id: AtomicU64,
    waiters: Arc<Mutex<HashMap<u64, Sender<Vec<u8>>>>>,
    shutdown: Arc<AtomicBool>,
    dispatcher: Option<JoinHandle<()>>,
    /// 单次尝试超时
    pub attempt_timeout: Duration,
    /// 最大尝试次数
    pub max_attempts: u32,
}

impl RpcClient {
    pub fn new(bind: &str) -> io::Result<Self> {
        let transport = Arc::new(Transport::bind(bind, Default::default())?);
        let waiters: Arc<Mutex<HashMap<u64, Sender<Vec<u8>>>>> = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let w = Arc::clone(&waiters);
        let sd = Arc::clone(&shutdown);
        let tp = Arc::clone(&transport);
        let dispatcher = std::thread::Builder::new()
            .name("homa-rpc-dispatch".into())
            .spawn(move || {
                while !sd.load(Ordering::Relaxed) {
                    let Ok((_src, frame)) = tp.recv(Duration::from_millis(50)) else { continue };
                    let Some((rpc_id, body)) = decode_frame(&frame) else { continue };
                    let tx = w.lock().unwrap().remove(&rpc_id);
                    if let Some(tx) = tx {
                        let _ = tx.send(body.to_vec());
                    }
                }
            })?;

        Ok(Self {
            transport,
            next_id: AtomicU64::new(1),
            waiters,
            shutdown,
            dispatcher: Some(dispatcher),
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }

    /// 发起一次 RPC：at-least-once，超时整请求重试（rpc_id 不变，服务端可去重）。
    pub fn call(&self, server: SocketAddr, payload: &[u8]) -> io::Result<Vec<u8>> {
        self.call_with_timeout(server, payload, self.attempt_timeout, self.max_attempts)
    }

    /// 完整参数版：自定义单次超时与最大尝试次数
    pub fn call_with_timeout(
        &self,
        server: SocketAddr,
        payload: &[u8],
        attempt_timeout: Duration,
        max_attempts: u32,
    ) -> io::Result<Vec<u8>> {
        let rpc_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_frame(rpc_id, payload);
        for _ in 0..max_attempts.max(1) {
            let (tx, rx) = channel::<Vec<u8>>();
            self.waiters.lock().unwrap().insert(rpc_id, tx);
            self.transport.send_to(server, &frame)?;
            match rx.recv_timeout(attempt_timeout) {
                Ok(body) => return Ok(body),
                Err(_) => {
                    // 超时：摘掉等待者，用同一 rpc_id 整体重发
                    self.waiters.lock().unwrap().remove(&rpc_id);
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::TimedOut, format!("rpc {rpc_id} timeout after {max_attempts} attempts")))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.transport.shutdown();
        if let Some(t) = self.dispatcher.take() {
            let _ = t.join();
        }
    }
}

/// 简单 TCP 对照实现（仅用于 benchmark 对比）：4 字节长度前缀帧，thread-per-conn。
pub mod tcp_baseline {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    pub struct TcpEchoServer {
        pub addr: SocketAddr,
        shutdown: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl TcpEchoServer {
        pub fn spawn(bind: &str) -> io::Result<Self> {
            let listener = TcpListener::bind(bind)?;
            listener.set_nonblocking(true)?;
            let addr = listener.local_addr()?;
            let shutdown = Arc::new(AtomicBool::new(false));
            let sd = Arc::clone(&shutdown);
            let thread = std::thread::spawn(move || {
                while !sd.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            // Windows 上 accept 继承 listener 的非阻塞标志，必须显式改回阻塞
                            if stream.set_nonblocking(false).is_err() {
                                continue;
                            }
                            std::thread::spawn(move || handle_conn(stream));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            });
            Ok(Self { addr, shutdown, thread: Some(thread) })
        }
    }

    impl Drop for TcpEchoServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    fn handle_conn(mut s: TcpStream) {
        loop {
            let mut hdr = [0u8; 4];
            if s.read_exact(&mut hdr).is_err() {
                return;
            }
            let len = u32::from_le_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            if s.read_exact(&mut body).is_err() {
                return;
            }
            let mut out = Vec::with_capacity(4 + len);
            out.extend_from_slice(&(len as u32).to_le_bytes());
            out.extend_from_slice(&body);
            if s.write_all(&out).is_err() {
                return;
            }
        }
    }

    /// TCP 客户端调用：短连接（每次新建，体现 TCP 建连开销）；返回 (响应, 耗时)
    pub fn call(addr: SocketAddr, payload: &[u8]) -> io::Result<(Vec<u8>, Duration)> {
        let start = Instant::now();
        let mut s = TcpStream::connect(addr)?;
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        s.write_all(&out)?;
        let mut hdr = [0u8; 4];
        s.read_exact(&mut hdr)?;
        let len = u32::from_le_bytes(hdr) as usize;
        let mut body = vec![0u8; len];
        s.read_exact(&mut body)?;
        Ok((body, start.elapsed()))
    }
}
