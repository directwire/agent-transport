//! 8 级动态优先级：按消息长度映射，越短优先级越高（编号越小越高）。
//!
//! Homa 用网卡/内核队列的 8 个优先级队列近似 SRPT；
//! 用户态 UDP 无法操控网卡队列，本实现把优先级写进包头，
//! 并由接收端 GRANT 调度器以 SRPT（剩余字节最少者优先）实际生效。

/// 优先级级数
pub const NUM_PRIORITIES: u8 = 8;

/// 各级别消息长度上界（字节），最后一级为无穷大
const THRESHOLDS: [u64; NUM_PRIORITIES as usize] = [
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    u64::MAX,
];

/// 按消息长度映射优先级：0 = 最高（最短消息）
pub fn priority_for_len(len: usize) -> u8 {
    let len = len as u64;
    for (i, &upper) in THRESHOLDS.iter().enumerate() {
        if len <= upper {
            return i as u8;
        }
    }
    (NUM_PRIORITIES - 1) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 短消息优先级更高() {
        assert_eq!(priority_for_len(64), 0);
        assert_eq!(priority_for_len(1024), 2);
        assert_eq!(priority_for_len(1 << 20), 5); // 1MiB 落在第 5 级（阈值 1_000_000 之上）
        assert!(priority_for_len(1 << 30) > priority_for_len(64));
    }
}
