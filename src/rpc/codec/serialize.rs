//! Payload serialization.
//!
//! Seastar fixes the frame envelope but leaves argument/return payloads to an
//! application-supplied C++ `Serializer`: every value goes through an
//! unqualified `write(serializer, out, v)` / `read(serializer, in, type<T>)`
//! the application must provide. Seastar itself encodes only its wrappers
//! (`rpc::tuple`, `rpc::optional`, ...), never a scalar or string, so no
//! encoding is "Seastar-compatible" on its own. [`Encode`] and [`Decode`] are
//! the Rust side of that contract: implement them for your own types to match
//! the peer application's serializer.
//!
//! The built-in impls follow the layout of the serializer in Seastar's
//! `demos/rpc_demo.cc`, which `examples/seastar/rpc_serializer.hh` extends:
//!
//! - integers, floats and `bool` (one byte, `0`/`1`): fixed width,
//!   little-endian, no prefix. The C++ side writes host byte order, which is
//!   little-endian on every platform Seastar supports (x86-64, aarch64);
//! - strings and byte buffers: `u32` LE length, then the raw bytes;
//! - `()`: nothing (a `void` return is an empty payload);
//! - tuples: elements back to back with no framing, which is how Seastar
//!   encodes both argument lists and `rpc::tuple` returns;
//! - `Option<T>`: nothing when `None`, `T` when `Some`. Like `rpc::optional`,
//!   presence is detected by remaining input, so it only works in trailing
//!   position.
//!
//! Each type needs a matching overload on the peer. Seastar's demo defines
//! only `int32_t`, `uint32_t`, `int64_t`, `uint64_t`, `double` and `sstring`
//! (which `String`, `Vec<u8>` and `Bytes` match); the example peer adds
//! `int8_t`, `uint8_t`, `int16_t`, `uint16_t`, `float` and `bool`. A peer
//! missing those still compiles when *writing* them: C++ promotion picks the
//! `int32_t`/`double` overload and sends 4 or 8 bytes, which [`from_payload`]
//! then rejects as trailing input.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::rpc::error::DecodeError;

/// Write a value into a request or response payload.
pub trait Encode {
    fn encode(&self, out: &mut BytesMut);
}

/// Read a value from the front of a payload, advancing `input` past it.
pub trait Decode: Sized {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError>;
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode(&self, out: &mut BytesMut) {
        (**self).encode(out)
    }
}

fn need(input: &Bytes, n: usize) -> Result<(), DecodeError> {
    if input.remaining() < n {
        return Err(DecodeError::UnexpectedEof {
            needed: n - input.remaining(),
        });
    }
    Ok(())
}

macro_rules! impl_number {
    ($($t:ty => $put:ident, $get:ident;)*) => {$(
        impl Encode for $t {
            fn encode(&self, out: &mut BytesMut) {
                out.$put(*self);
            }
        }

        impl Decode for $t {
            fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
                need(input, size_of::<$t>())?;
                Ok(input.$get())
            }
        }
    )*};
}

impl_number! {
    u8 => put_u8, get_u8;
    i8 => put_i8, get_i8;
    u16 => put_u16_le, get_u16_le;
    i16 => put_i16_le, get_i16_le;
    u32 => put_u32_le, get_u32_le;
    i32 => put_i32_le, get_i32_le;
    u64 => put_u64_le, get_u64_le;
    i64 => put_i64_le, get_i64_le;
    f32 => put_f32_le, get_f32_le;
    f64 => put_f64_le, get_f64_le;
}

impl Encode for bool {
    fn encode(&self, out: &mut BytesMut) {
        out.put_u8(*self as u8);
    }
}

impl Decode for bool {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(u8::decode(input)? != 0)
    }
}

fn encode_bytes(bytes: &[u8], out: &mut BytesMut) {
    let len = u32::try_from(bytes.len()).expect("payload value longer than u32::MAX bytes");
    out.put_u32_le(len);
    out.put_slice(bytes);
}

fn decode_bytes(input: &mut Bytes) -> Result<Bytes, DecodeError> {
    let len = u32::decode(input)? as usize;
    need(input, len)?;
    Ok(input.split_to(len))
}

impl Encode for str {
    fn encode(&self, out: &mut BytesMut) {
        encode_bytes(self.as_bytes(), out)
    }
}

impl Encode for String {
    fn encode(&self, out: &mut BytesMut) {
        self.as_str().encode(out)
    }
}

impl Decode for String {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        let bytes = decode_bytes(input)?;
        String::from_utf8(bytes.to_vec()).map_err(|e| DecodeError::Invalid(e.to_string()))
    }
}

impl Encode for [u8] {
    fn encode(&self, out: &mut BytesMut) {
        encode_bytes(self, out)
    }
}

impl Encode for Vec<u8> {
    fn encode(&self, out: &mut BytesMut) {
        encode_bytes(self, out)
    }
}

impl Decode for Vec<u8> {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(decode_bytes(input)?.to_vec())
    }
}

impl Encode for Bytes {
    fn encode(&self, out: &mut BytesMut) {
        encode_bytes(self, out)
    }
}

impl Decode for Bytes {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        decode_bytes(input)
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, out: &mut BytesMut) {
        if let Some(v) = self {
            v.encode(out);
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        if input.has_remaining() {
            T::decode(input).map(Some)
        } else {
            Ok(None)
        }
    }
}

macro_rules! impl_tuple {
    ($($name:ident)*) => {
        impl<$($name: Encode),*> Encode for ($($name,)*) {
            #[allow(non_snake_case, unused_variables)]
            fn encode(&self, out: &mut BytesMut) {
                let ($($name,)*) = self;
                $($name.encode(out);)*
            }
        }

        impl<$($name: Decode),*> Decode for ($($name,)*) {
            #[allow(unused_variables)]
            fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
                Ok(($($name::decode(input)?,)*))
            }
        }
    };
}

impl_tuple!();
impl_tuple!(A);
impl_tuple!(A B);
impl_tuple!(A B C);
impl_tuple!(A B C D);
impl_tuple!(A B C D E);
impl_tuple!(A B C D E F);
impl_tuple!(A B C D E F G);
impl_tuple!(A B C D E F G H);

/// Encode a value into a fresh payload.
pub fn to_payload<T: Encode + ?Sized>(value: &T) -> Bytes {
    let mut out = BytesMut::new();
    value.encode(&mut out);
    out.freeze()
}

/// Decode a whole payload. Trailing bytes are an error, so a reply type that
/// doesn't match the peer's signature is caught instead of silently truncated.
pub fn from_payload<T: Decode>(mut payload: Bytes) -> Result<T, DecodeError> {
    let value = T::decode(&mut payload)?;
    if payload.has_remaining() {
        return Err(DecodeError::Invalid(format!(
            "{} trailing byte(s) after value",
            payload.remaining()
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_are_fixed_width_le() {
        assert_eq!(&to_payload(&1i32)[..], &[1, 0, 0, 0]);
        assert_eq!(&to_payload(&-2i64)[..], &(-2i64).to_le_bytes());
        assert_eq!(&to_payload(&1.5f64)[..], &1.5f64.to_le_bytes());
        assert_eq!(
            from_payload::<u64>(Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0])),
            Ok(1)
        );
    }

    #[test]
    fn string_has_u32_length_prefix() {
        assert_eq!(&to_payload("hi")[..], &[2, 0, 0, 0, b'h', b'i']);
        assert_eq!(
            from_payload::<String>(to_payload("héllo")),
            Ok("héllo".to_string())
        );
    }

    #[test]
    fn arguments_are_concatenated() {
        // verb 2: int (int, int)
        assert_eq!(&to_payload(&(3i32, 4i32))[..], &[3, 0, 0, 0, 4, 0, 0, 0]);
        // verb 12: void (int, sstring)
        assert_eq!(
            &to_payload(&(5i32, "ab"))[..],
            &[5, 0, 0, 0, 2, 0, 0, 0, b'a', b'b']
        );
        // verb 10 returns rpc::tuple<long, int>, flattened
        let mut p = BytesMut::new();
        p.put_i64_le(1);
        p.put_i32_le(2);
        assert_eq!(from_payload::<(i64, i32)>(p.freeze()), Ok((1, 2)));
    }

    #[test]
    fn trailing_optional() {
        assert_eq!(to_payload(&(1i64, 2i64, None::<i32>)).len(), 16);
        assert_eq!(to_payload(&(1i64, 2i64, Some(3i32))).len(), 20);
        assert_eq!(
            from_payload::<(i64, Option<i32>)>(to_payload(&7i64)),
            Ok((7, None))
        );
        assert_eq!(
            from_payload::<(i64, Option<i32>)>(to_payload(&(7i64, 9i32))),
            Ok((7, Some(9)))
        );
    }

    #[test]
    fn void_is_empty() {
        assert!(to_payload(&()).is_empty());
        assert_eq!(from_payload::<()>(Bytes::new()), Ok(()));
    }

    #[test]
    fn short_and_trailing_input_rejected() {
        assert_eq!(
            from_payload::<i32>(Bytes::from_static(&[1, 2])),
            Err(DecodeError::UnexpectedEof { needed: 2 })
        );
        assert!(from_payload::<String>(Bytes::from_static(&[5, 0, 0, 0, b'a'])).is_err());
        assert!(matches!(
            from_payload::<i32>(Bytes::from_static(&[1, 0, 0, 0, 9])),
            Err(DecodeError::Invalid(_))
        ));
    }
}
