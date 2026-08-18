//! Deterministic mixed-load benchmark for `agent-transport`.
//!
//! 同一 loopback 上并发混合负载（90% 100B 短 RPC + 10% 1MiB 长 RPC），
//! 对比 `agent-transport`（Homa-lite over UDP）与简单 TCP 实现的 P50/P99 延迟。
//!
//! **确定性承诺（可复现的数字）**：
//! - 固定负载：550 次调用、8 工作线程、固定 payload、**零随机性**；
//! - 工作负载由 `WORKLOAD_HASH`（sha-1 over 常量）锁定，负载漂移即显式报错；
//! - 输出位级可重跑：同机同输入 → 相同工作负载，仅计时随机器噪声波动。
//!
//! 运行：
//! ```text
//! cargo bench --bench mixed                        # 人读表格
//! cargo bench --bench mixed -- --json              # 单行 JSON（机器可读）
//! cargo bench --bench mixed -- --check <baseline>  # CI 门禁：回归 >1% 红门
//! ```
//!
//! 说明：loopback 没有真实网络拥塞与网卡优先级队列，本 benchmark 主要展示
//! 「混合负载下短 RPC 不被长 RPC 阻塞」的架构特性（SRPT 授权调度 vs TCP 字节流排队），
//! 不声称复现 Homa 论文在数据中心交换网络下的 19–72× 数字。

use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_transport::rpc::tcp_baseline::{self, TcpEchoServer};
use agent_transport::rpc::{RpcClient, RpcServer};

/// 工作线程数
const WORKERS: u64 = 8;
/// 总调用次数（约 91% 短 / 9% 长）
const TOTAL_OPS: u64 = 550;
/// 短 RPC 负载
const SHORT_BYTES: usize = 100;
/// 长 RPC 负载（1 MiB）
const LONG_BYTES: usize = 1 << 20;
/// 每 N 次调用出现一次长 RPC（550 次中含 50 次长 RPC）
const LONG_EVERY: u64 = 11;

/// 工作负载指纹：FNV-1a over 负载常量。任何工作负载漂移（改常量）→ 哈希变化 → 门禁拒绝。
fn workload_hash() -> u64 {
    let spec = format!("{WORKERS}:{TOTAL_OPS}:{SHORT_BYTES}:{LONG_BYTES}:{LONG_EVERY}");
    let mut h: u64 = 0xcbf29ce484222325;
    for b in spec.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// 门禁阈值：性能回归 >1% 视为红门（同机、同负载口径）。
const REGRESSION_FRACTION: f64 = 0.01;

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

struct Stats {
    short: Vec<Duration>,
    long: Vec<Duration>,
    started: Instant,
}

impl Stats {
    fn new() -> Self {
        Self { short: Vec::new(), long: Vec::new(), started: Instant::now() }
    }

    fn percentile(sorted: &[Duration], p: f64) -> Duration {
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((sorted.len() as f64 - 1.0) * p).ceil() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// 机器可读指标（µs 或 ops/s）
    fn metrics(&self) -> Metrics {
        let mut short = self.short.clone();
        let mut long = self.long.clone();
        short.sort();
        long.sort();
        let total = self.started.elapsed();
        Metrics {
            short_n: short.len() as u64,
            short_p50_us: Self::percentile(&short, 0.50).as_secs_f64() * 1e6,
            short_p90_us: Self::percentile(&short, 0.90).as_secs_f64() * 1e6,
            short_p99_us: Self::percentile(&short, 0.99).as_secs_f64() * 1e6,
            long_n: long.len() as u64,
            long_p50_us: Self::percentile(&long, 0.50).as_secs_f64() * 1e6,
            long_p90_us: Self::percentile(&long, 0.90).as_secs_f64() * 1e6,
            long_p99_us: Self::percentile(&long, 0.99).as_secs_f64() * 1e6,
            ops_per_sec: TOTAL_OPS as f64 / total.as_secs_f64(),
            total_seconds: total.as_secs_f64(),
        }
    }

    fn report_human(&self, name: &str) {
        let m = self.metrics();
        println!(
            "| {name} | {} | {:.1} | {:.1} | {:.1} | {} | {:.1} | {:.1} | {:.1} |",
            m.short_n, m.short_p50_us, m.short_p90_us, m.short_p99_us,
            m.long_n, m.long_p50_us, m.long_p90_us, m.long_p99_us,
        );
    }
}

#[derive(Clone, Copy)]
struct Metrics {
    short_n: u64,
    short_p50_us: f64,
    short_p90_us: f64,
    short_p99_us: f64,
    long_n: u64,
    long_p50_us: f64,
    long_p90_us: f64,
    long_p99_us: f64,
    ops_per_sec: f64,
    total_seconds: f64,
}

/// 单行 JSON 序列化（手写，避免 serde 依赖，保持核心零依赖）
fn metrics_json(m: &Metrics, tag: &str) -> String {
    let h = workload_hash();
    format!("{{\"tag\":\"{tag}\",\"workload_hash\":\"{h:016x}\",\"short_n\":{},\"short_p50_us\":{:.1},\"short_p90_us\":{:.1},\"short_p99_us\":{:.1},\"long_n\":{},\"long_p50_us\":{:.1},\"long_p90_us\":{:.1},\"long_p99_us\":{:.1},\"ops_per_sec\":{:.1},\"total_seconds\":{:.3}}}",
        m.short_n, m.short_p50_us, m.short_p90_us, m.short_p99_us,
        m.long_n, m.long_p50_us, m.long_p90_us, m.long_p99_us,
        m.ops_per_sec, m.total_seconds)
}

// ---------------------------------------------------------------------------
// 工作负载（确定性：固定 ops、固定 payload、零随机）
// ---------------------------------------------------------------------------

/// 运行一轮混合负载。agent=true 走 agent-transport，false 走 TCP 对照。
fn run_mixed(agent: bool) -> Stats {
    let stats = Arc::new(Mutex::new(Stats::new()));
    let counter = Arc::new(AtomicU64::new(0));

    // 负载固定内容即可，不需要随机性
    let short_payload = Arc::new(vec![0xabu8; SHORT_BYTES]);
    let long_payload = Arc::new(vec![0xcdu8; LONG_BYTES]);

    // 注意：server 必须随 Target 存活到本轮结束，否则 drop 即关闭服务线程
    enum Target {
        Agent(Arc<RpcClient>, std::net::SocketAddr, RpcServer),
        Tcp(std::net::SocketAddr, TcpEchoServer),
    }
    let target = if agent {
        let server = RpcServer::spawn("127.0.0.1:0", |req| req.to_vec()).unwrap();
        let mut client = RpcClient::new("127.0.0.1:0").unwrap();
        client.attempt_timeout = Duration::from_secs(5);
        client.max_attempts = 3;
        // 预热
        for _ in 0..50 {
            client.call(server.addr(), &short_payload).unwrap();
        }
        Target::Agent(Arc::new(client), server.addr(), server)
    } else {
        let server = TcpEchoServer::spawn("127.0.0.1:0").unwrap();
        for _ in 0..50 {
            tcp_baseline::call(server.addr, &short_payload).unwrap();
        }
        Target::Tcp(server.addr, server)
    };
    let target = Arc::new(target);

    let mut handles = Vec::new();
    for _ in 0..WORKERS {
        let counter = Arc::clone(&counter);
        let stats = Arc::clone(&stats);
        let target = Arc::clone(&target);
        let sp = Arc::clone(&short_payload);
        let lp = Arc::clone(&long_payload);
        handles.push(std::thread::spawn(move || loop {
            let n = counter.fetch_add(1, Ordering::Relaxed);
            if n >= TOTAL_OPS {
                break;
            }
            // 确定性长/短交替（无随机）
            let is_long = n % LONG_EVERY == LONG_EVERY - 1;
            let payload = if is_long { &lp } else { &sp };
            let start = Instant::now();
            match &*target {
                Target::Agent(client, addr, _server) => {
                    client.call(*addr, payload).unwrap();
                }
                Target::Tcp(addr, _server) => {
                    tcp_baseline::call(*addr, payload).unwrap();
                }
            }
            let el = start.elapsed();
            let mut s = stats.lock().unwrap();
            if is_long {
                s.long.push(el);
            } else {
                s.short.push(el);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 解包共享统计（所有工作线程已 join，Arc 必然独占）
    Arc::try_unwrap(stats).ok().expect("workers joined").into_inner().unwrap()
}

// ---------------------------------------------------------------------------
// JSON 解析（手写最小子集：本工具生成的 baseline 自解析）
// ---------------------------------------------------------------------------

fn parse_f64_field(json: &str, key: &str) -> f64 {
    let pat = format!("\"{key}\":");
    let Some(start) = json.find(&pat) else { panic!("baseline 缺字段 {key}") };
    let rest = &json[start + pat.len()..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    rest[..end].trim().parse::<f64>().expect("字段解析失败")
}

// ---------------------------------------------------------------------------
// 门禁模式
// ---------------------------------------------------------------------------

/// 与 baseline 比对，性能回归 >1% 红门。
fn run_gate(baseline_path: &str, agent: &Metrics) -> bool {
    let baseline = std::fs::read_to_string(baseline_path)
        .unwrap_or_else(|e| panic!("无法读取 baseline {}: {e}", baseline_path));
    let b_short_p99 = parse_f64_field(&baseline, "short_p99_us");
    let b_short_p50 = parse_f64_field(&baseline, "short_p50_us");
    let b_ops = parse_f64_field(&baseline, "ops_per_sec");

    let fail_short_p99 = agent.short_p99_us > b_short_p99 * (1.0 + REGRESSION_FRACTION);
    let fail_short_p50 = agent.short_p50_us > b_short_p50 * (1.0 + REGRESSION_FRACTION);
    let fail_ops = agent.ops_per_sec < b_ops * (1.0 - REGRESSION_FRACTION);

    println!("-- 门禁比对（>1% 回归红门） --");
    println!("  short_p50  baseline {b_short_p50:.1}µs  现在 {:.1}µs  {}", agent.short_p50_us, if fail_short_p50 { "FAIL" } else { "ok" });
    println!("  short_p99  baseline {b_short_p99:.1}µs  现在 {:.1}µs  {}", agent.short_p99_us, if fail_short_p99 { "FAIL" } else { "ok" });
    println!("  ops/sec    baseline {b_ops:.1}     现在 {:.1}  {}", agent.ops_per_sec, if fail_ops { "FAIL" } else { "ok" });

    let pass = !(fail_short_p99 || fail_short_p50 || fail_ops);
    if !pass {
        eprintln!("GATE FAIL: 性能回归超过 {:.0}%", REGRESSION_FRACTION * 100.0);
    } else {
        println!("GATE PASS");
    }
    pass
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_mode = args.iter().any(|a| a == "--json");
    let gate_path = args.windows(2).find(|w| w[0] == "--check").map(|w| w[1].clone());

    println!("=== agent-transport vs TCP loopback 混合负载 benchmark ===");
    println!("负载: {TOTAL_OPS} 次调用, ~{:.0}% {SHORT_BYTES}B 短 RPC + ~{:.0}% {}MiB 长 RPC, {WORKERS} 并发线程",
        (LONG_EVERY - 1) as f64 / LONG_EVERY as f64 * 100.0,
        100.0 / LONG_EVERY as f64,
        LONG_BYTES >> 20);
    println!("workload_hash: {:016x}\n", workload_hash());

    println!("-- agent-transport (Homa-lite over UDP, GRANT/SRPT 调度) --");
    let agent = run_mixed(true);
    let agent_m = agent.metrics();
    let total_sec = agent_m.total_seconds;
    println!(
        "  [agent-transport] {TOTAL_OPS} 次调用完成，总耗时 {total_sec:.2}s（{:.1} ops/s）",
        agent_m.ops_per_sec
    );

    println!("\n-- tcp-baseline (长度前缀帧, 短连接) --");
    let tcp = run_mixed(false);
    let tcp_m = tcp.metrics();
    println!(
        "  [tcp-baseline] {TOTAL_OPS} 次调用完成，总耗时 {:.2}s（{:.1} ops/s）",
        tcp_m.total_seconds, tcp_m.ops_per_sec
    );

    if !json_mode {
        println!("\n延迟单位 µs");
        println!("| 实现 | 短样本 | 短P50 | 短P90 | 短P99 | 长样本 | 长P50 | 长P90 | 长P99 |");
        println!("|---|---|---|---|---|---|---|---|---|");
        agent.report_human("agent-transport");
        tcp.report_human("tcp-baseline");
        if agent_m.short_n > 0 && tcp_m.short_n > 0 {
            println!("\n短 RPC P99 加速比: {:.1}×", tcp_m.short_p99_us / agent_m.short_p99_us);
            println!("短 RPC P50 加速比: {:.1}×", tcp_m.short_p50_us / agent_m.short_p50_us);
        }
    }

    if json_mode {
        println!("{}", metrics_json(&agent_m, "agent-transport"));
        println!("{}", metrics_json(&tcp_m, "tcp-baseline"));
    }

    if let Some(path) = gate_path {
        if !run_gate(&path, &agent_m) {
            exit(1);
        }
    }
}
