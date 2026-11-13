//! 产品 split_envelope 帧，固定控制 metadata 与路由业务体分区。

use bytes::{BufMut, Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};

/// 控制帧类型保持固定，业务动作只通过 Event 进入路由授权流程。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// 客户端提交路由绑定与认证材料。
    Auth = 1,
    /// 服务端返回待确认的握手结果，尚未开放业务收发。
    AuthResponse = 2,
    /// 客户端动作，必须进入当前路由的授权及保序流程。
    Event = 3,
    /// 原动作的关联结果，不等于下行终端回执。
    EventAck = 4,
    /// 发起受限心跳探测，不能延长身份授权。
    Ping = 5,
    /// 回应匹配的心跳探测，不提供业务权限。
    Pong = 6,
    /// 服务端业务下行帧，目标资格在实际写出前继续复验。
    Broadcast = 7,
    /// 结束当前会话，并提供封闭的关闭原因。
    Close = 8,
    /// 客户端确认本次握手绑定，拒绝旧尝试的迟到确认。
    Confirm = 9,
    /// 初始化与接纳条件成立后的业务就绪通知。
    Ready = 10,
    /// 对精确投递票据的终端回执，不授予重新投递权限。
    Receipt = 11,
    /// 当前会话的有序状态通知，与业务正文分离。
    State = 12,
}

/// 分区帧拥有共享入站缓冲，读取控制头不会复制或解析业务体。
#[derive(Debug, Clone)]
pub struct Frame {
    /// 固定传输帧类型，决定控制方向和是否允许独立正文。
    pub typ: FrameType,
    /// 有正文时的 ProtocolBytes 模式序号，必须与当前路由唯一编码一致。
    pub mode: Option<u8>,
    /// 独立控制区的共享原始字节，按方向严格解析并受容量限制。
    pub metadata: Bytes,
    /// 可选业务编码区域，与 metadata 分离且随最后 Bytes owner 保留接收预算。
    pub body: Option<Bytes>,
}

impl Frame {
    /// 业务作用：从一条完整帧取得控制头和业务体的共享切片，严格区分无 body 和空 body。
    /// 参数说明：
    /// - `bytes`：包含四字节长度前缀的完整帧。
    /// - `maximum_frame`：整帧允许的字节上限。
    /// - `maximum_metadata`：固定 JSON 控制头允许的字节上限。
    ///
    /// 返回：合法帧的共享切片；截断、尾随、未知标志或非法模式均拒绝。
    pub fn decode(
        bytes: Bytes,
        maximum_frame: usize,
        maximum_metadata: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() >= 11 && bytes.len() <= maximum_frame,
            "frame_size"
        );
        let length = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
        anyhow::ensure!(length == bytes.len() - 4, "frame_length");
        let typ = match bytes[4] {
            1 => FrameType::Auth,
            2 => FrameType::AuthResponse,
            3 => FrameType::Event,
            4 => FrameType::EventAck,
            5 => FrameType::Ping,
            6 => FrameType::Pong,
            7 => FrameType::Broadcast,
            8 => FrameType::Close,
            9 => FrameType::Confirm,
            10 => FrameType::Ready,
            11 => FrameType::Receipt,
            12 => FrameType::State,
            _ => anyhow::bail!("frame_type"),
        };
        let present = match bytes[6] {
            0 => false,
            1 => true,
            _ => anyhow::bail!("frame_flags"),
        };
        let mode = if present {
            anyhow::ensure!(bytes[5] <= 3, "frame_mode");
            Some(bytes[5])
        } else {
            anyhow::ensure!(bytes[5] == 255, "absent_body_mode");
            None
        };
        let metadata_length = u32::from_be_bytes(bytes[7..11].try_into()?) as usize;
        anyhow::ensure!(
            metadata_length > 0
                && metadata_length <= maximum_metadata
                && metadata_length <= bytes.len() - 11,
            "metadata_length"
        );
        let end = 11 + metadata_length;
        anyhow::ensure!(present || end == bytes.len(), "unexpected_body");
        Ok(Self {
            typ,
            mode,
            metadata: bytes.slice(11..end),
            body: present.then(|| bytes.slice(end..)),
        })
    }

    /// 业务作用：严格解析当前帧的固定控制模型，业务区保持原样。
    /// 参数说明: 无。
    ///
    /// 返回：拒绝重复、未知字段的模型；不执行业务体反序列化。
    pub fn metadata<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        super::json::decode(&self.metadata, self.metadata.len())
    }

    /// 业务作用：编码共享控制头，业务体作为第二段保留，供 TCP 分段或 WS 汇聚写出。
    /// 参数说明：
    /// - `typ`：服务端确定的帧类型。
    /// - `metadata`：该方向允许的固定控制模型。
    /// - `body`：存在时为唯一 codec ordinal 与原始业务字节。
    ///
    /// 返回：头部和可选业务体，不针对每个收件人复制业务正文。
    pub fn encode<M: Serialize>(
        typ: FrameType,
        metadata: &M,
        body: Option<(u8, Bytes)>,
    ) -> anyhow::Result<(Bytes, Option<Bytes>)> {
        let metadata = serde_json::to_vec(metadata)?;
        let body_length = body.as_ref().map_or(0, |(_, b)| b.len());
        let length = 7usize
            .checked_add(metadata.len())
            .and_then(|n| n.checked_add(body_length))
            .ok_or_else(|| anyhow::anyhow!("frame_size"))?;
        let length = u32::try_from(length)?;
        if let Some((mode, _)) = &body {
            anyhow::ensure!(*mode <= 3, "frame_mode");
        }
        let mut header = BytesMut::with_capacity(11 + metadata.len());
        header.put_u32(length);
        header.put_u8(typ as u8);
        header.put_u8(body.as_ref().map_or(255, |(mode, _)| *mode));
        header.put_u8(u8::from(body.is_some()));
        header.put_u32(u32::try_from(metadata.len())?);
        header.extend_from_slice(&metadata);
        Ok((header.freeze(), body.map(|(_, bytes)| bytes)))
    }
}
