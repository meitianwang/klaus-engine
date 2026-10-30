use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// Exact JavaScript binary64 identity using SameValue equality and hashing.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeNumber(u64);

impl RuntimeNumber {
    /// Construct from a JavaScript Number, canonicalizing every NaN payload.
    pub fn from_f64(value: f64) -> Self {
        let bits = if value.is_nan() {
            CANONICAL_NAN_BITS
        } else {
            value.to_bits()
        };
        Self(bits)
    }

    /// Return the canonical IEEE-754 bits.
    pub fn to_bits(self) -> u64 {
        self.0
    }

    /// Return the represented binary64 value.
    pub fn as_f64(self) -> f64 {
        f64::from_bits(self.0)
    }

    /// JavaScript strict equality (`===`).
    pub fn strict_eq(self, other: Self) -> bool {
        self.as_f64() == other.as_f64()
    }

    /// JavaScript SameValueZero equality.
    pub fn same_value_zero(self, other: Self) -> bool {
        self == other || (self.as_f64() == 0.0 && other.as_f64() == 0.0)
    }

    /// Numeric comparison, which is unordered when either operand is NaN.
    pub fn numeric_cmp(self, other: Self) -> Option<Ordering> {
        self.as_f64().partial_cmp(&other.as_f64())
    }

    /// JavaScript `JSON.stringify` token for this Number.
    pub fn to_json_token(self) -> String {
        let value = self.as_f64();
        if !value.is_finite() {
            return "null".to_owned();
        }
        if value == 0.0 {
            return "0".to_owned();
        }
        ryu_js::Buffer::new().format_finite(value).to_owned()
    }
}

impl PartialEq for RuntimeNumber {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for RuntimeNumber {}

impl Hash for RuntimeNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<f64> for RuntimeNumber {
    fn from(value: f64) -> Self {
        Self::from_f64(value)
    }
}

impl From<f32> for RuntimeNumber {
    fn from(value: f32) -> Self {
        Self::from_f64(f64::from(value))
    }
}

macro_rules! impl_integer_conversion {
    ($($integer:ty),+ $(,)?) => {
        $(
            impl From<$integer> for RuntimeNumber {
                fn from(value: $integer) -> Self {
                    Self::from_f64(value as f64)
                }
            }
        )+
    };
}

impl_integer_conversion!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);
