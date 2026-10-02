/// Verb id as sent on the wire (the C++ `MsgType` cast to `u64`).
///
/// Payload types are chosen per call: multiple arguments are a tuple
/// (`(i32, i32)`), a trailing `rpc::optional<T>` is a trailing `Option<T>`,
/// and `void` is `()`.
///
/// ```
/// use seastar_rpc::rpc::Verb;
///
/// /// `int (int, int)`
/// const ADD: Verb = 2;
/// ```
pub type Verb = u64;
