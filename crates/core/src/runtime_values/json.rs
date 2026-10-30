use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Index, IndexMut};

use super::{RuntimeNumber, RuntimeObject, RuntimeString};

/// Exact JSON-shaped JavaScript runtime value.
#[derive(Default)]
pub enum RuntimeJson {
    #[default]
    Null,
    Bool(bool),
    Number(RuntimeNumber),
    String(RuntimeString),
    Array(Vec<RuntimeJson>),
    Object(RuntimeObject),
}

impl RuntimeJson {
    /// Returns true only for the exact JSON null value.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub const fn is_boolean(&self) -> bool {
        matches!(self, Self::Bool(_))
    }

    pub const fn is_number(&self) -> bool {
        matches!(self, Self::Number(_))
    }

    pub const fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub const fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    pub const fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }

    /// Borrow the exact UTF-16 string payload without projecting it to UTF-8.
    pub const fn as_string(&self) -> Option<&RuntimeString> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// Serde-compatible spelling with an exact UTF-16 return type. This does
    /// not create an implicit UTF-8 projection.
    pub const fn as_str(&self) -> Option<&RuntimeString> {
        self.as_string()
    }

    /// Borrow the exact array payload.
    pub const fn as_array(&self) -> Option<&Vec<RuntimeJson>> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    /// Mutably borrow the exact array payload.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<RuntimeJson>> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    /// Borrow the exact ECMAScript-ordered object payload.
    pub const fn as_object(&self) -> Option<&RuntimeObject> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    /// Mutably borrow the exact ECMAScript-ordered object payload.
    pub fn as_object_mut(&mut self) -> Option<&mut RuntimeObject> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    /// Borrow a boolean payload.
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrow a number payload without changing its binary64 identity.
    pub const fn as_number(&self) -> Option<&RuntimeNumber> {
        match self {
            Self::Number(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        Some(self.as_number()?.as_f64())
    }

    pub fn as_i64(&self) -> Option<i64> {
        let value = self.as_f64()?;
        if value.is_finite()
            && value.fract() == 0.0
            && value >= i64::MIN as f64
            && value <= i64::MAX as f64
        {
            Some(value as i64)
        } else {
            None
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        let value = self.as_f64()?;
        if value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= u64::MAX as f64 {
            Some(value as u64)
        } else {
            None
        }
    }

    /// Look up one exact object property. Non-object values return `None`.
    pub fn get(&self, key: impl Into<RuntimeString>) -> Option<&RuntimeJson> {
        self.as_object()?.get(key)
    }

    /// Mutably look up one exact object property. Non-object values return
    /// `None`.
    pub fn get_mut(&mut self, key: impl Into<RuntimeString>) -> Option<&mut RuntimeJson> {
        self.as_object_mut()?.get_mut(key)
    }

    /// Resolve an RFC 6901 JSON pointer expressed as an ordinary scalar Rust
    /// string. Object-key comparisons remain exact UTF-16 comparisons.
    pub fn pointer(&self, pointer: &str) -> Option<&RuntimeJson> {
        let mut current = self;
        if pointer.is_empty() {
            return Some(current);
        }
        for token in pointer.strip_prefix('/')?.split('/') {
            let token = decode_pointer_token(token);
            current = match current {
                Self::Object(object) => object.get(token),
                Self::Array(array) => token
                    .try_to_utf8()
                    .ok()
                    .and_then(|token| token.parse::<usize>().ok())
                    .and_then(|index| array.get(index)),
                _ => None,
            }?;
        }
        Some(current)
    }

    /// Mutable counterpart to [`RuntimeJson::pointer`].
    pub fn pointer_mut(&mut self, pointer: &str) -> Option<&mut RuntimeJson> {
        if pointer.is_empty() {
            return Some(self);
        }
        let tokens = pointer
            .strip_prefix('/')?
            .split('/')
            .map(decode_pointer_token)
            .collect::<Vec<_>>();
        let mut current = self;
        for token in tokens {
            current = match current {
                Self::Object(object) => object.get_mut(token),
                Self::Array(array) => token
                    .try_to_utf8()
                    .ok()
                    .and_then(|token| token.parse::<usize>().ok())
                    .and_then(|index| array.get_mut(index)),
                _ => None,
            }?;
        }
        Some(current)
    }

    /// Compare object members without property-order significance.
    pub fn semantic_eq_unordered(&self, other: &Self) -> bool {
        iterative_eq(self, other, ObjectComparison::Unordered)
    }

    /// Consume a string value without cloning its exact UTF-16 storage.
    ///
    /// `RuntimeJson` owns an iterative destructor for unbounded nesting, so
    /// callers use these accessors instead of moving fields out with a match.
    pub fn into_string(mut self) -> Result<RuntimeString, Self> {
        match &mut self {
            Self::String(value) => Ok(std::mem::take(value)),
            _ => Err(self),
        }
    }

    /// Consume an array value without cloning its children.
    pub fn into_array(mut self) -> Result<Vec<RuntimeJson>, Self> {
        match &mut self {
            Self::Array(value) => Ok(std::mem::take(value)),
            _ => Err(self),
        }
    }

    /// Consume an object value without cloning its exact ordered storage.
    pub fn into_object(mut self) -> Result<RuntimeObject, Self> {
        match &mut self {
            Self::Object(value) => Ok(std::mem::take(value)),
            _ => Err(self),
        }
    }

    fn drain_children_into(&mut self, pending: &mut Vec<RuntimeJson>) {
        match self {
            Self::Array(values) => pending.append(values),
            Self::Object(object) => object.drain_values_into(pending),
            Self::Null | Self::Bool(_) | Self::Number(_) | Self::String(_) => {}
        }
    }
}

fn decode_pointer_token(token: &str) -> RuntimeString {
    RuntimeString::from(token.replace("~1", "/").replace("~0", "~"))
}

impl Index<&str> for RuntimeJson {
    type Output = RuntimeJson;

    fn index(&self, key: &str) -> &Self::Output {
        self.get(key)
            .unwrap_or_else(|| panic!("runtime JSON object has no property {key:?}"))
    }
}

impl IndexMut<&str> for RuntimeJson {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        match self {
            Self::Object(object) => {
                if !object.contains_key(key) {
                    object.insert(key, Self::Null);
                }
                object.get_mut(key).expect("property inserted above")
            }
            _ => panic!("cannot index non-object runtime JSON with {key:?}"),
        }
    }
}

impl Index<usize> for RuntimeJson {
    type Output = RuntimeJson;

    fn index(&self, index: usize) -> &Self::Output {
        self.as_array()
            .and_then(|array| array.get(index))
            .unwrap_or_else(|| panic!("runtime JSON array has no index {index}"))
    }
}

impl IndexMut<usize> for RuntimeJson {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.as_array_mut()
            .and_then(|array| array.get_mut(index))
            .unwrap_or_else(|| panic!("runtime JSON array has no index {index}"))
    }
}

impl fmt::Display for RuntimeJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stringified = super::runtime_json::stringify(self);
        let scalar = stringified
            .try_to_utf8()
            .expect("JSON stringify escapes every unpaired surrogate");
        formatter.write_str(&scalar)
    }
}

impl Clone for RuntimeJson {
    fn clone(&self) -> Self {
        enum CloneOp<'a> {
            Value(&'a RuntimeJson),
            FinishArray(usize),
            FinishObject(Vec<RuntimeString>),
        }

        let mut operations = vec![CloneOp::Value(self)];
        let mut completed = Vec::new();
        while let Some(operation) = operations.pop() {
            match operation {
                CloneOp::Value(value) => match value {
                    Self::Null => completed.push(Self::Null),
                    Self::Bool(value) => completed.push(Self::Bool(*value)),
                    Self::Number(value) => completed.push(Self::Number(*value)),
                    Self::String(value) => completed.push(Self::String(value.clone())),
                    Self::Array(values) => {
                        operations.push(CloneOp::FinishArray(values.len()));
                        operations.extend(values.iter().rev().map(CloneOp::Value));
                    }
                    Self::Object(object) => {
                        let entries = object.entries().collect::<Vec<_>>();
                        operations.push(CloneOp::FinishObject(
                            entries.iter().map(|(key, _)| key.clone()).collect(),
                        ));
                        operations.extend(
                            entries
                                .into_iter()
                                .rev()
                                .map(|(_, value)| CloneOp::Value(value)),
                        );
                    }
                },
                CloneOp::FinishArray(length) => {
                    let values = completed.split_off(completed.len() - length);
                    completed.push(Self::Array(values));
                }
                CloneOp::FinishObject(keys) => {
                    let values = completed.split_off(completed.len() - keys.len());
                    let mut object = RuntimeObject::new();
                    for (key, value) in keys.into_iter().zip(values) {
                        object.insert(key, value);
                    }
                    completed.push(Self::Object(object));
                }
            }
        }
        completed
            .pop()
            .expect("one exact clone result for one runtime value")
    }
}

impl PartialEq for RuntimeJson {
    fn eq(&self, other: &Self) -> bool {
        iterative_eq(self, other, ObjectComparison::Ordered)
    }
}

impl Eq for RuntimeJson {}

impl Hash for RuntimeJson {
    fn hash<H: Hasher>(&self, state: &mut H) {
        enum HashOp<'a> {
            Value(&'a RuntimeJson),
            Key(RuntimeString),
        }

        let mut operations = vec![HashOp::Value(self)];
        while let Some(operation) = operations.pop() {
            match operation {
                HashOp::Key(key) => key.hash(state),
                HashOp::Value(value) => match value {
                    Self::Null => 0_u8.hash(state),
                    Self::Bool(value) => {
                        1_u8.hash(state);
                        value.hash(state);
                    }
                    Self::Number(value) => {
                        2_u8.hash(state);
                        value.hash(state);
                    }
                    Self::String(value) => {
                        3_u8.hash(state);
                        value.hash(state);
                    }
                    Self::Array(values) => {
                        4_u8.hash(state);
                        values.len().hash(state);
                        operations.extend(values.iter().rev().map(HashOp::Value));
                    }
                    Self::Object(object) => {
                        5_u8.hash(state);
                        object.len().hash(state);
                        for (key, value) in object.entries().collect::<Vec<_>>().into_iter().rev() {
                            operations.push(HashOp::Value(value));
                            operations.push(HashOp::Key(key));
                        }
                    }
                },
            }
        }
    }
}

impl fmt::Debug for RuntimeJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        enum DebugOp<'a> {
            Value(&'a RuntimeJson),
            Key(RuntimeString),
            Text(&'static str),
        }

        let mut operations = vec![DebugOp::Value(self)];
        while let Some(operation) = operations.pop() {
            match operation {
                DebugOp::Text(value) => formatter.write_str(value)?,
                DebugOp::Key(value) => write!(formatter, "{value:?}")?,
                DebugOp::Value(value) => match value {
                    Self::Null => formatter.write_str("Null")?,
                    Self::Bool(value) => write!(formatter, "Bool({value:?})")?,
                    Self::Number(value) => write!(formatter, "Number({value:?})")?,
                    Self::String(value) => write!(formatter, "String({value:?})")?,
                    Self::Array(values) => {
                        formatter.write_str("Array([")?;
                        operations.push(DebugOp::Text("])"));
                        for (index, value) in values.iter().enumerate().rev() {
                            operations.push(DebugOp::Value(value));
                            if index > 0 {
                                operations.push(DebugOp::Text(", "));
                            }
                        }
                    }
                    Self::Object(object) => {
                        formatter.write_str("Object({")?;
                        operations.push(DebugOp::Text("})"));
                        for (index, (key, value)) in object
                            .entries()
                            .collect::<Vec<_>>()
                            .into_iter()
                            .enumerate()
                            .rev()
                        {
                            operations.push(DebugOp::Value(value));
                            operations.push(DebugOp::Text(": "));
                            operations.push(DebugOp::Key(key));
                            if index > 0 {
                                operations.push(DebugOp::Text(", "));
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ObjectComparison {
    Ordered,
    Unordered,
}

fn iterative_eq(left: &RuntimeJson, right: &RuntimeJson, objects: ObjectComparison) -> bool {
    let mut pending = vec![(left, right)];
    while let Some((left, right)) = pending.pop() {
        match (left, right) {
            (RuntimeJson::Null, RuntimeJson::Null) => {}
            (RuntimeJson::Bool(left), RuntimeJson::Bool(right)) if left == right => {}
            (RuntimeJson::Number(left), RuntimeJson::Number(right)) if left == right => {}
            (RuntimeJson::String(left), RuntimeJson::String(right)) if left == right => {}
            (RuntimeJson::Array(left), RuntimeJson::Array(right)) if left.len() == right.len() => {
                pending.extend(left.iter().zip(right).rev());
            }
            (RuntimeJson::Object(left), RuntimeJson::Object(right))
                if left.len() == right.len() =>
            {
                match objects {
                    ObjectComparison::Ordered => {
                        let left = left.entries().collect::<Vec<_>>();
                        let right = right.entries().collect::<Vec<_>>();
                        for ((left_key, left_value), (right_key, right_value)) in
                            left.into_iter().zip(right).rev()
                        {
                            if left_key != right_key {
                                return false;
                            }
                            pending.push((left_value, right_value));
                        }
                    }
                    ObjectComparison::Unordered => {
                        for (key, left_value) in left.entries() {
                            let Some(right_value) = right.get(key) else {
                                return false;
                            };
                            pending.push((left_value, right_value));
                        }
                    }
                }
            }
            _ => return false,
        }
    }
    true
}

impl Drop for RuntimeJson {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        self.drain_children_into(&mut pending);
        while let Some(mut value) = pending.pop() {
            value.drain_children_into(&mut pending);
            // `value` now owns no recursive children, so its ordinary drop is
            // shallow when this loop iteration ends.
        }
    }
}

impl From<bool> for RuntimeJson {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<RuntimeNumber> for RuntimeJson {
    fn from(value: RuntimeNumber) -> Self {
        Self::Number(value)
    }
}

impl From<RuntimeString> for RuntimeJson {
    fn from(value: RuntimeString) -> Self {
        Self::String(value)
    }
}

impl From<&str> for RuntimeJson {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<String> for RuntimeJson {
    fn from(value: String) -> Self {
        Self::String(value.into())
    }
}

impl From<RuntimeObject> for RuntimeJson {
    fn from(value: RuntimeObject) -> Self {
        Self::Object(value)
    }
}

impl From<Vec<RuntimeJson>> for RuntimeJson {
    fn from(value: Vec<RuntimeJson>) -> Self {
        Self::Array(value)
    }
}

macro_rules! impl_number_conversion {
    ($($number:ty),+ $(,)?) => {
        $(
            impl From<$number> for RuntimeJson {
                fn from(value: $number) -> Self {
                    Self::Number(RuntimeNumber::from(value))
                }
            }
        )+
    };
}

impl_number_conversion!(f32, f64, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);
