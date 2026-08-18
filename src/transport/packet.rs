//! Homa-lite 线格式：固定 22 字节小端头 + 可选负载。
//!
//! 包类型对齐 Homa：DATA / GRANT / RESEND / BUSY。
//! 不引入任何序列化 crate，手工编解码，保持零依赖。

use std::io;

/// 包头长度（字节）
pub const HEADER_LEN: usize = 22;

/// 包类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    /// 数据包：携带消息的一个分片
    Data = 0,
    /// 授权包：接收端授予发送端「可发送到 offset」的累计授权
    Grant = 1,
    /// 重传请求：接收端发现授予窗口内缺包，请求重发 [offset, offset+length)
    Resend = 2,
    /// 忙信号：接收端并发消息数超限，让发送端稍后再试探
    Busy = 3,
}

impl PacketType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Grant),
            2 => Some(Self::Resend),
            3 => Some(Self::Busy),
            _ => None,
        }
    }
}

/// 解码后的包头。各字段按包类型复用：
/// - DATA:   msg_len=消息总长, offset=负载偏移, length=负载长度
/// - GRANT:  offset=累计授权到的字节偏移（不含）, length=本次新增授权量, priority=建议优先级
/// - RESEND: offset=请求重发起始偏移, length=请求重发字节数
/// - BUSY:   无有效载荷字段
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packet {
    pub typ: PacketType,
    /// 8 级动态优先级，0 为最高
    pub priority: u8,
    /// 消息 ID（每条消息内唯一，由发送方分配）
    pub msg_id: u64,
    /// DATA：消息总长度；其余类型为 0
    pub msg_len: u32,
    /// 见上文字段复用说明
    pub offset: u32,
    /// 见上文字段复用说明
    pub length: u32,
}

impl Packet {
    pub fn new(typ: PacketType, priority: u8, msg_id: u64, msg_len: u32, offset: u32, length: u32) -> Self {
        Self { typ, priority, msg_id, msg_len, offset, length }
    }

    /// 编码包头 + 负载为一个 UDP 数据报
    pub fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LEN + payload.len());
        buf.push(self.typ as u8);
        buf.push(self.priority);
        buf.extend_from_slice(&self.msg_id.to_le_bytes());
        buf.extend_from_slice(&self.msg_len.to_le_bytes());
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    /// 从数据报解码包头，返回 (包头, 负载切片)
    pub fn decode(datagram: &[u8]) -> io::Result<(Packet, &[u8])> {
        if datagram.len() < HEADER_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "datagram too short for header"));
        }
        let typ = PacketType::from_u8(datagram[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown packet type"))?;
        let priority = datagram[1];
        let msg_id = u64::from_le_bytes(datagram[2..10].try_into().unwrap());
        let msg_len = u32::from_le_bytes(datagram[10..14].try_into().unwrap());
        let offset = u32::from_le_bytes(datagram[14..18].try_into().unwrap());
        let length = u32::from_le_bytes(datagram[18..22].try_into().unwrap());
        let pkt = Packet { typ, priority, msg_id, msg_len, offset, length };
        let payload = &datagram[HEADER_LEN..];
        if pkt.typ == PacketType::Data && payload.len() != length as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "DATA payload length mismatch"));
        }
        Ok((pkt, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 编解码往返一致() {
        let pkt = Packet::new(PacketType::Data, 3, 0xdead_beef, 5000, 2400, 11);
        let bytes = pkt.encode(b"hello world");
        let (decoded, payload) = Packet::decode(&bytes).unwrap();
        assert_eq!(decoded, pkt);
        assert_eq!(payload, b"hello world");
    }

    #[test]
    fn 拒绝坏包() {
        assert!(Packet::decode(&[0u8; 5]).is_err());
        // 类型 99 非法
        let mut bad = vec![99u8; HEADER_LEN];
        bad.extend_from_slice(b"x");
        assert!(Packet::decode(&bad).is_err());
    }
}
