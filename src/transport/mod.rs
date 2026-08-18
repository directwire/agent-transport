//! Homa-lite 传输层：消息导向、无连接、接收端驱动调度。
//!
//! 架构：[`SenderCore`] / [`ReceiverCore`] 是不碰 socket 的纯状态机（输出 [`Action`]），
//! [`Transport`] 只负责把 Action 落到 UDP socket 上，并用一个 IO 线程驱动收包与周期滴答。
//! 这样调度器、重组、重传状态机都可以脱离网络做确定性单测。

pub mod packet;
pub mod priority;
pub mod receiver;
pub mod sender;
pub mod txqueue;

use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use packet::{Packet, PacketType};
use receiver::ReceiverCore;
use sender::SenderCore;
use txqueue::{TxQueues, FLUSH_BUDGET};

/// 状态机产出的动作
#[derive(Debug)]
pub enum Action {
    /// 向 dest 发一个 UDP 数据报
    Send { dest: SocketAddr, bytes: Vec<u8> },
    /// 一条消息重组完成，交付给上层
    Deliver { src: SocketAddr, msg_id: u64, data: Vec<u8> },
}

/// 传输层参数（对标 Homa 默认值的可调版本）
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// UDP 数据报负载分片大小（字节）
    pub packet_size: usize,
    /// 未调度窗口：每条消息首 RTT 无需授权直接发送的字节数（Homa 默认 ~10KB）
    pub unscheduled_bytes: usize,
    /// 每次 GRANT 的授予增量（约一个 BDP）
    pub grant_increment: usize,
    /// 接收端判定授予窗口内缺包的超时
    pub resend_timeout: Duration,
    /// 接收端判定 GRANT 丢失（授权后无进展）的超时
    pub grant_timeout: Duration,
    /// 发送端停滞探测间隔
    pub poke_timeout: Duration,
    /// 消息发完后保留状态响应迟到 RESEND 的驻留时间
    pub linger: Duration,
    /// send_to 的整体超时（超过则放弃，由上层 RPC 重试）
    pub send_timeout: Duration,
    /// 接收端允许的最大并发在收消息数，超出回 BUSY
    pub max_incoming: usize,
    /// overcommit：同时授权的消息条数（Homa 默认 8，loopback 骨架默认 2）
    pub overcommit: usize,
    /// 防饿死：等待授权超过该时长强制入授权集合
    pub starve_threshold: Duration,
    /// UDP 收发缓冲区大小（loopback 突发吞包主因是默认缓冲太小）
    pub socket_buf: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            packet_size: 1200,
            unscheduled_bytes: 10_240,
            grant_increment: 65_536,
            resend_timeout: Duration::from_millis(20),
            grant_timeout: Duration::from_millis(40),
            poke_timeout: Duration::from_millis(50),
            linger: Duration::from_secs(5),
            send_timeout: Duration::from_secs(30),
            max_incoming: 1024,
            overcommit: 2,
            starve_threshold: Duration::from_millis(200),
            socket_buf: 8 << 20,
        }
    }
}

/// 共享状态（IO 线程与 API 调用线程并发访问）
struct State {
    sender: SenderCore,
    receiver: ReceiverCore,
    /// 已重组完成、等待上层 recv 取走的消息
    completed: VecDeque<(SocketAddr, Vec<u8>)>,
    /// 发送侧 8 级优先级 QoS 队列（仅 DATA 包入队）
    tx: TxQueues,
}

struct Inner {
    socket: UdpSocket,
    state: Mutex<State>,
    /// 有消息交付 / 发送完成时通知等待者
    cv: Condvar,
    msg_counter: AtomicU64,
    shutdown: AtomicBool,
    send_timeout: Duration,
}

/// 消息导向传输句柄。克隆廉价（内部 Arc），可多线程并发 send/recv。
pub struct Transport {
    inner: Arc<Inner>,
    io_thread: Option<JoinHandle<()>>,
}

impl Transport {
    /// 绑定本地地址并启动 IO 线程
    pub fn bind(addr: &str, cfg: TransportConfig) -> io::Result<Self> {
        // 用 socket2 放大 UDP 收发缓冲：默认缓冲在授权窗口突发下会整片丢包
        let s2 = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None)?;
        s2.set_recv_buffer_size(cfg.socket_buf)?;
        s2.set_send_buffer_size(cfg.socket_buf)?;
        s2.bind(&addr.parse::<SocketAddr>().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?.into())?;
        let socket: UdpSocket = s2.into();
        // IO 线程需要周期性 tick（RESEND / 重发 GRANT / 发送端探针），给读设短超时
        socket.set_read_timeout(Some(Duration::from_millis(5)))?;
        let send_timeout = cfg.send_timeout;
        let inner = Arc::new(Inner {
            socket,
            state: Mutex::new(State {
                sender: SenderCore::new(cfg.clone()),
                receiver: ReceiverCore::new(cfg),
                completed: VecDeque::new(),
                tx: TxQueues::new(),
            }),
            cv: Condvar::new(),
            msg_counter: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            send_timeout,
        });

        let io_inner = Arc::clone(&inner);
        let io_thread = std::thread::Builder::new()
            .name("homa-io".into())
            .spawn(move || io_loop(io_inner))?;

        Ok(Self { inner, io_thread: Some(io_thread) })
    }

    /// 本地绑定地址
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    /// 调试：导出内部状态机快照（在发消息、在收消息、待交付数）
    pub fn debug_state(&self) -> String {
        let st = self.inner.state.lock().unwrap();
        format!(
            "发送中: {}\n在收: {}\n待交付: {}",
            st.sender.debug_dump(),
            st.receiver.debug_dump(),
            st.completed.len()
        )
    }

    /// 发送一条完整消息（消息导向语义：对端要么收全要么不收）。
    /// 阻塞直到全部字节发出（含等待 GRANT），超时返回 TimedOut。
    pub fn send_to(&self, dest: SocketAddr, data: &[u8]) -> io::Result<u64> {
        let msg_id = self.inner.msg_counter.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let deadline = now + self.inner.send_timeout;

        let actions = {
            let mut st = self.inner.state.lock().unwrap();
            st.sender.start(dest, msg_id, data.to_vec(), now)
        };
        self.dispatch(actions);

        // 等待全部字节发出
        let mut st = self.inner.state.lock().unwrap();
        loop {
            if st.sender.is_done(dest, msg_id) {
                // 不立即销毁：留 linger 期响应对端迟到的 RESEND，由 sender.tick 自动回收
                return Ok(msg_id);
            }
            let remain = deadline.saturating_duration_since(Instant::now());
            if remain.is_zero() {
                st.sender.finish(dest, msg_id);
                return Err(io::Error::new(io::ErrorKind::TimedOut, "homa send_to timeout"));
            }
            let (guard, _) = self.inner.cv.wait_timeout(st, remain).unwrap();
            st = guard;
        }
    }

    /// 接收一条完整消息。timeout 内无消息返回 WouldBlock。
    pub fn recv(&self, timeout: Duration) -> io::Result<(SocketAddr, Vec<u8>)> {
        let deadline = Instant::now() + timeout;
        let mut st = self.inner.state.lock().unwrap();
        loop {
            if let Some((src, data)) = st.completed.pop_front() {
                return Ok((src, data));
            }
            let remain = deadline.saturating_duration_since(Instant::now());
            if remain.is_zero() {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "homa recv timeout"));
            }
            let (guard, _) = self.inner.cv.wait_timeout(st, remain).unwrap();
            st = guard;
        }
    }

    /// 把状态机产出的发包动作落到 QoS 队列/直发（send_to 路径）
    fn dispatch(&self, actions: Vec<Action>) {
        let mut st = self.inner.state.lock().unwrap();
        drain(&self.inner, &mut st, actions);
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        if let Some(t) = self.io_thread.take() {
            let _ = t.join();
        }
    }
}

/// 处理状态机产出：Deliver 进完成队列并唤醒 recv；
/// DATA 包进 8 级优先级队列、控制包直发，随后按优先级限额冲刷队列
fn drain(inner: &Inner, st: &mut State, actions: Vec<Action>) {
    let mut sends = Vec::new();
    let mut delivered = false;
    for a in actions {
        match a {
            Action::Deliver { src, data, .. } => {
                st.completed.push_back((src, data));
                delivered = true;
            }
            Action::Send { dest, bytes } => {
                // DATA 包按优先级入队；控制包（GRANT/RESEND/BUSY）直发保证调度活性
                if let Some(direct) = st.tx.classify(dest, bytes) {
                    sends.push(direct);
                }
            }
        }
    }
    // 限额冲刷：高优先级先走； backlog 由 IO 线程后续迭代继续冲刷
    sends.extend(st.tx.pop_batch(FLUSH_BUDGET));
    if delivered {
        inner.cv.notify_all();
    }
    // 注意：这里持有的是 &mut State 借用，进入发送循环前已结束状态修改
    for (dest, bytes) in sends {
        // UDP 发送失败（如 ICMP 不可达）不致命，忽略继续
        let _ = inner.socket.send_to(&bytes, dest);
    }
}

/// IO 线程主循环：收包 → 驱动状态机 → 落盘发包。
/// tick 按时间触发（每 5ms）：高负载期收包队列不空、读永不超时，
/// 若只在读超时 tick 会导致 RESEND/重授权被饿死，造成系统性停滞。
fn io_loop(inner: Arc<Inner>) {
    let mut buf = vec![0u8; 1 << 16];
    let tick_interval = Duration::from_millis(5);
    let mut last_tick = Instant::now();
    while !inner.shutdown.load(Ordering::Relaxed) {
        match inner.socket.recv_from(&mut buf) {
            Ok((n, src)) => {
                let Ok((pkt, payload)) = Packet::decode(&buf[..n]) else { continue };
                let now = Instant::now();
                let mut st = inner.state.lock().unwrap();
                let mut actions = Vec::new();
                match pkt.typ {
                    PacketType::Data => st.receiver.handle_data(src, &pkt, payload, now, &mut actions),
                    PacketType::Grant => {
                        st.sender.handle_grant(src, &pkt, now, &mut actions);
                        if st.sender.is_done(src, pkt.msg_id) {
                            inner.cv.notify_all(); // 唤醒 send_to 等待者
                        }
                    }
                    PacketType::Resend => st.sender.handle_resend(src, &pkt, now, &mut actions),
                    PacketType::Busy => st.sender.handle_busy(src, &pkt, now),
                }
                drain(&inner, &mut st, actions);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {}
            Err(_) => {
                if inner.shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
        // 按时间驱动 tick（无论本次是否收到包）
        let now = Instant::now();
        if now.duration_since(last_tick) >= tick_interval {
            last_tick = now;
            let mut st = inner.state.lock().unwrap();
            let mut actions = Vec::new();
            st.sender.tick(now, &mut actions);
            st.receiver.tick(now, &mut actions);
            drain(&inner, &mut st, actions);
        }
    }
}
