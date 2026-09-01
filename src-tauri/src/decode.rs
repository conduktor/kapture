use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DecodedValue {
    Primitive {
        #[serde(rename = "type")]
        ty: PrimitiveType,
        value: String,
    },
    Bytes {
        hex: String,
        length: usize,
    },
    Object {
        fields: Vec<DecodedField>,
    },
    Array {
        items: Vec<Self>,
    },
}

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    String,
    Number,
    Boolean,
    Null,
}

#[derive(Debug, Serialize, Clone, JsonSchema)]
pub struct DecodedField {
    pub name: String,
    pub value: DecodedValue,
}

impl DecodedValue {
    /// Estimated bytes retained by heap allocations below this value.
    /// Inline enum/field storage is counted by the owning vector or
    /// `CapturedMessage`; this method accounts for String and Vec buffers.
    #[must_use]
    pub fn estimated_heap_bytes(&self) -> usize {
        match self {
            Self::Primitive { value, .. } => value.capacity(),
            Self::Bytes { hex, .. } => hex.capacity(),
            Self::Object { fields } => fields
                .capacity()
                .saturating_mul(std::mem::size_of::<DecodedField>())
                .saturating_add(fields.iter().fold(0usize, |total, field| {
                    total
                        .saturating_add(field.name.capacity())
                        .saturating_add(field.value.estimated_heap_bytes())
                })),
            Self::Array { items } => items
                .capacity()
                .saturating_mul(std::mem::size_of::<Self>())
                .saturating_add(items.iter().fold(0usize, |total, item| {
                    total.saturating_add(item.estimated_heap_bytes())
                })),
        }
    }
}

/// Decode a payload using the JSON heuristic, falling back to bytes.
pub fn decode_payload(bytes: Option<&[u8]>) -> DecodedValue {
    let Some(bytes) = bytes else {
        return DecodedValue::Bytes {
            hex: String::new(),
            length: 0,
        };
    };
    if bytes.is_empty() {
        return DecodedValue::Bytes {
            hex: String::new(),
            length: 0,
        };
    }
    serde_json::from_slice::<Value>(bytes).map_or_else(
        |_| DecodedValue::Bytes {
            hex: hex::encode(bytes),
            length: bytes.len(),
        },
        |value| from_json(&value),
    )
}

/// Capture-path decode. JSON remains structurally useful for filters;
/// opaque binary values keep only their length because the owning
/// message already retains the raw bytes. Detail inspection fills hex.
pub fn decode_payload_lazy_bytes(bytes: Option<&[u8]>) -> DecodedValue {
    let Some(bytes) = bytes else {
        return DecodedValue::Bytes {
            hex: String::new(),
            length: 0,
        };
    };
    serde_json::from_slice::<Value>(bytes).map_or_else(
        |_| DecodedValue::Bytes {
            hex: String::new(),
            length: bytes.len(),
        },
        |value| from_json(&value),
    )
}

fn from_json(value: &Value) -> DecodedValue {
    match value {
        Value::Null => DecodedValue::Primitive {
            ty: PrimitiveType::Null,
            value: "null".to_owned(),
        },
        Value::Bool(b) => DecodedValue::Primitive {
            ty: PrimitiveType::Boolean,
            value: b.to_string(),
        },
        Value::Number(n) => DecodedValue::Primitive {
            ty: PrimitiveType::Number,
            value: n.to_string(),
        },
        Value::String(s) => DecodedValue::Primitive {
            ty: PrimitiveType::String,
            value: s.clone(),
        },
        Value::Array(items) => DecodedValue::Array {
            items: items.iter().map(from_json).collect(),
        },
        Value::Object(map) => DecodedValue::Object {
            fields: map
                .iter()
                .map(|(name, value)| DecodedField {
                    name: name.clone(),
                    value: from_json(value),
                })
                .collect(),
        },
    }
}

/// Render the first N bytes as a `hex` string, space-separated.
pub fn render_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_json_object() {
        let payload = br#"{"amount": 1450, "currency": "EUR"}"#;
        let decoded = decode_payload(Some(payload));
        match decoded {
            DecodedValue::Object { fields } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "amount");
            }
            other => unreachable!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_bytes() {
        let decoded = decode_payload(Some(&[0xff, 0x00, 0xab]));
        match decoded {
            DecodedValue::Bytes { hex, length } => {
                assert_eq!(hex, "ff00ab");
                assert_eq!(length, 3);
            }
            other => unreachable!("expected bytes, got {other:?}"),
        }
    }

    #[test]
    fn handles_empty_payload() {
        let decoded = decode_payload(Some(&[]));
        match decoded {
            DecodedValue::Bytes { length, .. } => assert_eq!(length, 0),
            other => unreachable!("expected bytes, got {other:?}"),
        }
    }
}
