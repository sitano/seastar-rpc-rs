//! Request and response frames (wire guide §6).
//!
//! Each codec implements both [`Encoder`] and [`Decoder`] for its frame type,
//! so the same code serves the client (writes requests, reads responses) and
//! a server (the reverse). The header layout depends on the negotiated
//! features, captured once in a [`FrameLayout`].

use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::rpc::codec::exception::RemoteException;
use crate::rpc::codec::negotiation::Features;
use crate::rpc::error::{Error, Result};
use crate::rpc::verb::Verb;

/// `handler_duration` value meaning "not measured".
pub const HANDLER_DURATION_UNKNOWN: u32 = u32::MAX;

/// Frame header shape for a negotiated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLayout {
    /// Request frames start with a `u64` timeout in ms.
    pub timeout: bool,
    /// Response frames carry a `u32` handler duration after the length.
    pub handler_duration: bool,
    /// Largest payload accepted by the decoders.
    pub max_frame_size: usize,
}

impl FrameLayout {
    pub fn new(negotiated: &Features, max_frame_size: usize) -> Self {
        FrameLayout {
            timeout: negotiated.timeout,
            handler_duration: negotiated.handler_duration,
            max_frame_size,
        }
    }

    fn request_header_len(&self) -> usize {
        if self.timeout { 28 } else { 20 }
    }

    fn response_header_len(&self) -> usize {
        if self.handler_duration { 16 } else { 12 }
    }

    fn check_len(&self, len: usize) -> Result<()> {
        if len > self.max_frame_size {
            return Err(Error::Protocol(format!(
                "frame payload of {len} bytes exceeds limit of {} bytes",
                self.max_frame_size
            )));
        }
        Ok(())
    }
}

/// Client → server call. Build one with [`RequestFrame::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RequestFrame {
    /// Remaining time budget in ms; `0` means no timeout. Only on the wire
    /// when `TIMEOUT` was negotiated.
    pub timeout_ms: u64,
    pub verb: Verb,
    /// Positive, never reused on a connection.
    pub msg_id: i64,
    pub payload: Bytes,
}

impl RequestFrame {
    /// A call with no timeout. `msg_id` must be positive and unique on the
    /// connection.
    pub fn new(msg_id: i64, verb: Verb, payload: Bytes) -> Self {
        RequestFrame {
            timeout_ms: 0,
            verb,
            msg_id,
            payload,
        }
    }

    /// Set the remaining time budget; `None` means no timeout.
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout_ms = Self::timeout_ms_for(timeout);
        self
    }

    /// Wire value for a remaining time budget: `0` is reserved for "no
    /// timeout", so a budget under 1 ms is rounded up.
    pub fn timeout_ms_for(remaining: Option<Duration>) -> u64 {
        match remaining {
            None => 0,
            Some(d) => u64::try_from(d.as_millis()).unwrap_or(u64::MAX).max(1),
        }
    }
}

/// Server → client reply (or exception) for a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseFrame {
    /// The call's `msg_id`, negated when the payload is an exception. Use
    /// [`ResponseFrame::call_id`] to match a reply to its call.
    pub msg_id: i64,
    /// Handler run time in µs, when `HANDLER_DURATION` was negotiated
    /// ([`HANDLER_DURATION_UNKNOWN`] if not measured).
    pub handler_duration: Option<u32>,
    pub payload: Bytes,
}

impl ResponseFrame {
    /// The `msg_id` of the call this frame answers.
    pub fn call_id(&self) -> i64 {
        self.msg_id.wrapping_abs()
    }

    pub fn is_exception(&self) -> bool {
        self.msg_id < 0
    }

    /// The reply payload, or the decoded exception as an error.
    pub fn into_result(self) -> Result<Bytes> {
        if self.is_exception() {
            Err(RemoteException::decode(self.payload)?.into())
        } else {
            Ok(self.payload)
        }
    }
}

fn payload_len(payload: &Bytes) -> u32 {
    u32::try_from(payload.len()).expect("frame payload longer than u32::MAX bytes")
}

/// Codec for [`RequestFrame`]s.
#[derive(Debug, Clone)]
pub struct RequestCodec {
    layout: FrameLayout,
}

impl RequestCodec {
    pub fn new(layout: FrameLayout) -> Self {
        RequestCodec { layout }
    }
}

impl Encoder<RequestFrame> for RequestCodec {
    type Error = Error;

    fn encode(&mut self, frame: RequestFrame, dst: &mut BytesMut) -> Result<()> {
        dst.reserve(self.layout.request_header_len() + frame.payload.len());

        if self.layout.timeout {
            dst.put_u64_le(frame.timeout_ms);
        }

        dst.put_u64_le(frame.verb);
        dst.put_i64_le(frame.msg_id);
        dst.put_u32_le(payload_len(&frame.payload));
        dst.put_slice(&frame.payload);

        Ok(())
    }
}

impl Decoder for RequestCodec {
    type Item = RequestFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<RequestFrame>> {
        let header = self.layout.request_header_len();

        if src.len() < header {
            src.reserve(header - src.len());
            return Ok(None);
        }

        let len = u32::from_le_bytes(src[header - 4..header].try_into().unwrap()) as usize;

        self.layout.check_len(len)?;

        if src.len() < header + len {
            src.reserve(header + len - src.len());
            return Ok(None);
        }

        let timeout_ms = if self.layout.timeout {
            src.get_u64_le()
        } else {
            0
        };

        let verb = src.get_u64_le();
        let msg_id = src.get_i64_le();

        src.advance(4);

        let payload = src.split_to(len).freeze();

        Ok(Some(RequestFrame {
            timeout_ms,
            verb,
            msg_id,
            payload,
        }))
    }
}

/// Codec for [`ResponseFrame`]s.
#[derive(Debug, Clone)]
pub struct ResponseCodec {
    layout: FrameLayout,
}

impl ResponseCodec {
    pub fn new(layout: FrameLayout) -> Self {
        ResponseCodec { layout }
    }
}

impl Encoder<ResponseFrame> for ResponseCodec {
    type Error = Error;

    fn encode(&mut self, frame: ResponseFrame, dst: &mut BytesMut) -> Result<()> {
        dst.reserve(self.layout.response_header_len() + frame.payload.len());

        dst.put_i64_le(frame.msg_id);
        dst.put_u32_le(payload_len(&frame.payload));

        if self.layout.handler_duration {
            dst.put_u32_le(frame.handler_duration.unwrap_or(HANDLER_DURATION_UNKNOWN));
        }

        dst.put_slice(&frame.payload);

        Ok(())
    }
}

impl Decoder for ResponseCodec {
    type Item = ResponseFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<ResponseFrame>> {
        let header = self.layout.response_header_len();

        if src.len() < header {
            src.reserve(header - src.len());
            return Ok(None);
        }

        let len = u32::from_le_bytes(src[8..12].try_into().unwrap()) as usize;

        self.layout.check_len(len)?;

        if src.len() < header + len {
            src.reserve(header + len - src.len());
            return Ok(None);
        }

        let msg_id = src.get_i64_le();

        src.advance(4);

        let handler_duration = self.layout.handler_duration.then(|| src.get_u32_le());
        let payload = src.split_to(len).freeze();

        Ok(Some(ResponseFrame {
            msg_id,
            handler_duration,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(timeout: bool, handler_duration: bool) -> FrameLayout {
        FrameLayout {
            timeout,
            handler_duration,
            max_frame_size: 1024,
        }
    }

    fn request() -> RequestFrame {
        RequestFrame {
            timeout_ms: 500,
            verb: 2,
            msg_id: 7,
            payload: Bytes::from_static(&[3, 0, 0, 0, 4, 0, 0, 0]),
        }
    }

    #[test]
    fn request_exact_bytes_with_timeout() {
        let mut buf = BytesMut::new();

        RequestCodec::new(layout(true, false))
            .encode(request(), &mut buf)
            .unwrap();

        let mut expect = Vec::new();

        expect.extend_from_slice(&500u64.to_le_bytes());
        expect.extend_from_slice(&2u64.to_le_bytes());
        expect.extend_from_slice(&7i64.to_le_bytes());
        expect.extend_from_slice(&8u32.to_le_bytes());
        expect.extend_from_slice(&[3, 0, 0, 0, 4, 0, 0, 0]);

        assert_eq!(&buf[..], &expect[..]);
    }

    #[test]
    fn request_without_timeout_omits_field() {
        let mut buf = BytesMut::new();
        let mut codec = RequestCodec::new(layout(false, false));

        codec.encode(request(), &mut buf).unwrap();

        assert_eq!(buf.len(), 20 + 8);
        assert_eq!(&buf[..8], &2u64.to_le_bytes());

        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(
            decoded,
            RequestFrame {
                timeout_ms: 0,
                ..request()
            }
        );
    }

    #[test]
    fn request_roundtrip_byte_by_byte() {
        let mut codec = RequestCodec::new(layout(true, true));
        let mut wire = BytesMut::new();

        codec.encode(request(), &mut wire).unwrap();

        let mut src = BytesMut::new();

        for (i, b) in wire.iter().enumerate() {
            src.put_u8(*b);
            let out = codec.decode(&mut src).unwrap();
            if i + 1 < wire.len() {
                assert!(out.is_none());
            } else {
                assert_eq!(out, Some(request()));
            }
        }

        assert!(src.is_empty());
    }

    #[test]
    fn response_layouts() {
        let frame = ResponseFrame {
            msg_id: -3,
            handler_duration: Some(12),
            payload: Bytes::from_static(b"abc"),
        };
        let mut with = BytesMut::new();

        ResponseCodec::new(layout(false, true))
            .encode(frame.clone(), &mut with)
            .unwrap();

        assert_eq!(with.len(), 16 + 3);
        assert_eq!(&with[12..16], &12u32.to_le_bytes());
        assert_eq!(
            ResponseCodec::new(layout(false, true))
                .decode(&mut with)
                .unwrap(),
            Some(frame.clone())
        );

        let mut without = BytesMut::new();

        ResponseCodec::new(layout(true, false))
            .encode(frame.clone(), &mut without)
            .unwrap();

        assert_eq!(without.len(), 12 + 3);

        let decoded = ResponseCodec::new(layout(true, false))
            .decode(&mut without)
            .unwrap()
            .unwrap();

        assert_eq!(decoded.handler_duration, None);
        assert_eq!(decoded.call_id(), 3);
        assert!(decoded.is_exception());
    }

    #[test]
    fn two_frames_in_one_buffer() {
        let mut codec = ResponseCodec::new(layout(false, false));
        let mut buf = BytesMut::new();

        for id in [1, 2] {
            let f = ResponseFrame {
                msg_id: id,
                handler_duration: None,
                payload: Bytes::new(),
            };
            codec.encode(f, &mut buf).unwrap();
        }

        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().msg_id, 1);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().msg_id, 2);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i64_le(1);
        buf.put_u32_le(4096);

        let err = ResponseCodec::new(layout(false, false))
            .decode(&mut buf)
            .unwrap_err();

        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn exception_result() {
        let f = ResponseFrame {
            msg_id: -9,
            handler_duration: None,
            payload: RemoteException::UnknownVerb(999).encode(),
        };

        assert!(matches!(f.into_result(), Err(Error::UnknownVerb(999))));
    }

    #[test]
    fn timeout_ms_never_zero_with_deadline() {
        assert_eq!(RequestFrame::timeout_ms_for(None), 0);
        assert_eq!(
            RequestFrame::timeout_ms_for(Some(Duration::from_micros(10))),
            1
        );
        assert_eq!(
            RequestFrame::timeout_ms_for(Some(Duration::from_millis(1500))),
            1500
        );
    }
}
