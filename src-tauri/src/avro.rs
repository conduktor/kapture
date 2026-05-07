//! Avro decoder. Builds a `DecodedValue` tree from Avro binary bytes.
//!
//! The schema is parsed once per id (cached upstream by the schema
//! registry client), then reused across messages.
//!
//! Wired by `schema_resolver.rs`: when the proxy session has a
//! Schema Registry URL configured, the resolver fetches the Avro
//! schema for each record's id, parses it via `parse_schema`, and
//! decodes the post-envelope value bytes through `decode`. The
//! resulting `DecodedValue` tree replaces the raw-bytes payload on
//! the captured message so the inspector renders a JSON-ish view
//! instead of hex.
#![allow(dead_code)] // SchemaKind variants we don't decode (Protobuf path) live next door

use apache_avro::types::Value as AvroValue;
use apache_avro::Schema;
use thiserror::Error;

use crate::decode::{DecodedField, DecodedValue, PrimitiveType};

#[derive(Debug, Error)]
pub enum AvroError {
    #[error("avro schema parse: {0}")]
    BadSchema(String),

    #[error("avro decode: {0}")]
    BadDatum(String),
}

pub fn parse_schema(raw: &str) -> Result<Schema, AvroError> {
    Schema::parse_str(raw).map_err(|err| AvroError::BadSchema(err.to_string()))
}

pub fn decode(schema: &Schema, payload: &[u8]) -> Result<DecodedValue, AvroError> {
    let mut cursor = std::io::Cursor::new(payload);
    let value = apache_avro::from_avro_datum(schema, &mut cursor, None)
        .map_err(|err| AvroError::BadDatum(err.to_string()))?;
    Ok(avro_value_to_decoded(&value))
}

// Match arms for distinct numeric Avro variants share an identical body
// template but bind different types (i32 / i64 / f32 / f64), so they
// cannot be merged via `|`.
#[allow(clippy::cast_precision_loss, clippy::match_same_arms)]
fn avro_value_to_decoded(value: &AvroValue) -> DecodedValue {
    match value {
        AvroValue::Null => DecodedValue::Primitive {
            ty: PrimitiveType::Null,
            value: "null".to_owned(),
        },
        AvroValue::Boolean(b) => DecodedValue::Primitive {
            ty: PrimitiveType::Boolean,
            value: b.to_string(),
        },
        AvroValue::Int(n) => number_token(n),
        AvroValue::Long(n) => number_token(n),
        AvroValue::Float(n) => number_token(n),
        AvroValue::Double(n) => number_token(n),
        AvroValue::String(s) | AvroValue::Enum(_, s) => DecodedValue::Primitive {
            ty: PrimitiveType::String,
            value: s.clone(),
        },
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => DecodedValue::Bytes {
            hex: hex::encode(b),
            length: b.len(),
        },
        AvroValue::Array(items) => DecodedValue::Array {
            items: items.iter().map(avro_value_to_decoded).collect(),
        },
        AvroValue::Map(entries) => DecodedValue::Object {
            fields: entries
                .iter()
                .map(|(name, val)| DecodedField {
                    name: name.clone(),
                    value: avro_value_to_decoded(val),
                })
                .collect(),
        },
        AvroValue::Record(fields) => DecodedValue::Object {
            fields: fields
                .iter()
                .map(|(name, val)| DecodedField {
                    name: name.clone(),
                    value: avro_value_to_decoded(val),
                })
                .collect(),
        },
        AvroValue::Union(_, inner) => avro_value_to_decoded(inner),
        AvroValue::Date(days) => number_token(days),
        AvroValue::TimeMillis(n) => number_token(n),
        AvroValue::TimeMicros(n) => number_token(n),
        AvroValue::TimestampMillis(n)
        | AvroValue::TimestampMicros(n)
        | AvroValue::TimestampNanos(n)
        | AvroValue::LocalTimestampMillis(n)
        | AvroValue::LocalTimestampMicros(n)
        | AvroValue::LocalTimestampNanos(n) => number_token(n),
        AvroValue::Decimal(_)
        | AvroValue::BigDecimal(_)
        | AvroValue::Uuid(_)
        | AvroValue::Duration(_) => DecodedValue::Primitive {
            ty: PrimitiveType::String,
            value: format!("{value:?}"),
        },
    }
}

fn number_token<T: std::fmt::Display>(value: T) -> DecodedValue {
    DecodedValue::Primitive {
        ty: PrimitiveType::Number,
        value: value.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use apache_avro::{to_avro_datum, types::Record};

    #[test]
    fn roundtrip_record() {
        let schema_json = r#"{
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "amount", "type": "int"},
                {"name": "currency", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }"#;
        let schema = parse_schema(schema_json).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("amount", 1450);
        record.put("currency", "EUR");
        record.put(
            "tags",
            AvroValue::Array(vec![
                AvroValue::String("priority".into()),
                AvroValue::String("paid".into()),
            ]),
        );
        let encoded = to_avro_datum(&schema, record).unwrap();

        let decoded = decode(&schema, &encoded).unwrap();
        match decoded {
            DecodedValue::Object { fields } => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name, "amount");
                assert_eq!(fields[1].name, "currency");
                assert_eq!(fields[2].name, "tags");
            }
            other => unreachable!("expected object, got {other:?}"),
        }
    }
}
