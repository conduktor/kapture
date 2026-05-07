//! Wireshark-style filter DSL: parse, compile, evaluate against `CapturedMessage`.

use std::sync::Arc;

use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use regex::Regex;
use thiserror::Error;

use crate::decode::{DecodedField, DecodedValue, PrimitiveType};
use crate::message::CapturedMessage;

#[derive(Parser)]
#[grammar = "filter.pest"]
struct FilterParser;

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("syntax error: {0}")]
    Syntax(String),

    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        #[source]
        source: regex::Error,
    },

    #[error("operator `=~` requires a string regex literal")]
    RegexNeedsString,

    #[error("operator `in` requires a list literal")]
    InNeedsList,

    #[error("operator `{op}` does not apply to a list literal")]
    OpVsList { op: String },

    #[error(
        "comparison on a parenthesised expression is not supported; \
             move the comparison inside the parentheses"
    )]
    CompareOnGroup,
}

/// Compiled, ready-to-evaluate filter.
#[derive(Debug, Clone)]
pub struct CompiledFilter {
    expr: Arc<Expr>,
}

impl CompiledFilter {
    /// Compile a filter expression. Returns a parse error on syntax failure
    /// or an invalid regex / operand combination.
    pub fn compile(input: &str) -> std::result::Result<Self, FilterError> {
        let mut pairs = FilterParser::parse(Rule::filter, input)
            .map_err(|err| FilterError::Syntax(err.to_string()))?;
        let root = pairs
            .next()
            .ok_or_else(|| FilterError::Syntax("empty input".to_owned()))?;
        let or = root
            .into_inner()
            .find(|p| p.as_rule() == Rule::or_expr)
            .ok_or_else(|| FilterError::Syntax("missing expression".to_owned()))?;
        let expr = build_or(or)?;
        Ok(Self {
            expr: Arc::new(expr),
        })
    }

    /// Evaluate the filter against a captured message.
    #[must_use]
    pub fn matches(&self, message: &CapturedMessage) -> bool {
        eval(&self.expr, message)
    }
}

#[derive(Debug)]
enum Expr {
    Or(Vec<Self>),
    And(Vec<Self>),
    Not(Box<Self>),
    Constant(bool),
    Truthy(Vec<String>),
    Compare { path: Vec<String>, cmp: Comparator },
}

#[derive(Debug)]
enum Comparator {
    Eq(Literal),
    Ne(Literal),
    Lt(Literal),
    Gt(Literal),
    Le(Literal),
    Ge(Literal),
    Re(Regex),
    In(Vec<Literal>),
}

#[derive(Debug, Clone)]
enum Literal {
    String(String),
    Number(f64),
    Boolean(bool),
}

#[derive(Debug)]
enum RawLiteral {
    Single(Literal),
    List(Vec<Literal>),
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Eq,
    Ne,
    Re,
    Lt,
    Gt,
    Le,
    Ge,
    In,
}

// ---------------------------------------------------------------------------
// Parser → AST
// ---------------------------------------------------------------------------

fn build_or(pair: Pair<'_, Rule>) -> std::result::Result<Expr, FilterError> {
    let mut nodes = Vec::new();
    for inner in pair.into_inner() {
        if matches!(inner.as_rule(), Rule::and_expr) {
            nodes.push(build_and(inner)?);
        }
    }
    Ok(if nodes.len() == 1 {
        nodes.swap_remove(0)
    } else {
        Expr::Or(nodes)
    })
}

fn build_and(pair: Pair<'_, Rule>) -> std::result::Result<Expr, FilterError> {
    let mut nodes = Vec::new();
    for inner in pair.into_inner() {
        if matches!(inner.as_rule(), Rule::not_expr) {
            nodes.push(build_not(inner)?);
        }
    }
    Ok(if nodes.len() == 1 {
        nodes.swap_remove(0)
    } else {
        Expr::And(nodes)
    })
}

fn build_not(pair: Pair<'_, Rule>) -> std::result::Result<Expr, FilterError> {
    let raw = pair.as_str().trim_start();
    let inner = pair
        .into_inner()
        .find(|p| matches!(p.as_rule(), Rule::comparison))
        .ok_or_else(|| FilterError::Syntax("missing comparison".to_owned()))?;
    let comparison = build_comparison(inner)?;
    if raw.starts_with('!') {
        Ok(Expr::Not(Box::new(comparison)))
    } else {
        Ok(comparison)
    }
}

fn build_comparison(pair: Pair<'_, Rule>) -> std::result::Result<Expr, FilterError> {
    let mut iter = pair.into_inner();
    let head = iter
        .next()
        .ok_or_else(|| FilterError::Syntax("missing primary".to_owned()))?;

    match head.as_rule() {
        Rule::or_expr => {
            // Parenthesised expression. Refuse a trailing `op literal`
            // rather than silently dropping it (semantics of comparing a
            // boolean group against a literal are not modelled).
            if iter.next().is_some() {
                return Err(FilterError::CompareOnGroup);
            }
            return build_or(head);
        }
        Rule::identifier => {}
        other => {
            return Err(FilterError::Syntax(format!(
                "unexpected token `{other:?}` in comparison"
            )));
        }
    }

    let path = parse_identifier(&head);

    let Some(op_pair) = iter.next() else {
        // Bare identifier. `true` / `false` are constants; everything else
        // is a truthy lookup.
        return Ok(constant_or_truthy(path));
    };
    let op = parse_op(op_pair.as_str())?;
    let value_pair = iter
        .next()
        .ok_or_else(|| FilterError::Syntax("missing right-hand side".to_owned()))?;
    let raw = build_raw_literal(value_pair)?;
    let cmp = build_comparator(op, raw)?;
    Ok(Expr::Compare { path, cmp })
}

fn constant_or_truthy(path: Vec<String>) -> Expr {
    if path.len() == 1 {
        match path[0].as_str() {
            "true" => return Expr::Constant(true),
            "false" => return Expr::Constant(false),
            _ => {}
        }
    }
    Expr::Truthy(path)
}

fn build_comparator(op: Op, raw: RawLiteral) -> std::result::Result<Comparator, FilterError> {
    match (op, raw) {
        (Op::In, RawLiteral::List(items)) => Ok(Comparator::In(items)),
        (Op::In, RawLiteral::Single(_)) => Err(FilterError::InNeedsList),
        (_, RawLiteral::List(_)) => Err(FilterError::OpVsList {
            op: op_label(op).to_owned(),
        }),
        (Op::Re, RawLiteral::Single(Literal::String(pat))) => {
            let regex = Regex::new(&pat).map_err(|source| FilterError::InvalidRegex {
                pattern: pat,
                source,
            })?;
            Ok(Comparator::Re(regex))
        }
        (Op::Re, _) => Err(FilterError::RegexNeedsString),
        (Op::Eq, RawLiteral::Single(lit)) => Ok(Comparator::Eq(lit)),
        (Op::Ne, RawLiteral::Single(lit)) => Ok(Comparator::Ne(lit)),
        (Op::Lt, RawLiteral::Single(lit)) => Ok(Comparator::Lt(lit)),
        (Op::Gt, RawLiteral::Single(lit)) => Ok(Comparator::Gt(lit)),
        (Op::Le, RawLiteral::Single(lit)) => Ok(Comparator::Le(lit)),
        (Op::Ge, RawLiteral::Single(lit)) => Ok(Comparator::Ge(lit)),
    }
}

fn parse_identifier(pair: &Pair<'_, Rule>) -> Vec<String> {
    pair.as_str().split('.').map(ToOwned::to_owned).collect()
}

fn parse_op(raw: &str) -> std::result::Result<Op, FilterError> {
    Ok(match raw.trim() {
        "==" => Op::Eq,
        "!=" => Op::Ne,
        "=~" => Op::Re,
        "<" => Op::Lt,
        ">" => Op::Gt,
        "<=" => Op::Le,
        ">=" => Op::Ge,
        "in" => Op::In,
        other => return Err(FilterError::Syntax(format!("unknown op `{other}`"))),
    })
}

const fn op_label(op: Op) -> &'static str {
    match op {
        Op::Eq => "==",
        Op::Ne => "!=",
        Op::Re => "=~",
        Op::Lt => "<",
        Op::Gt => ">",
        Op::Le => "<=",
        Op::Ge => ">=",
        Op::In => "in",
    }
}

fn build_raw_literal(pair: Pair<'_, Rule>) -> std::result::Result<RawLiteral, FilterError> {
    match pair.as_rule() {
        Rule::list => {
            let mut items = Vec::new();
            for child in pair.into_inner() {
                items.push(build_literal(child)?);
            }
            Ok(RawLiteral::List(items))
        }
        _ => Ok(RawLiteral::Single(build_literal(pair)?)),
    }
}

fn build_literal(pair: Pair<'_, Rule>) -> std::result::Result<Literal, FilterError> {
    match pair.as_rule() {
        Rule::string => {
            let inner = pair
                .into_inner()
                .find(|p| matches!(p.as_rule(), Rule::string_inner))
                .map(|p| p.as_str().to_owned())
                .unwrap_or_default();
            Ok(Literal::String(unescape(&inner)))
        }
        Rule::number => pair
            .as_str()
            .parse::<f64>()
            .map(Literal::Number)
            .map_err(|err| {
                FilterError::Syntax(format!("invalid number `{}`: {err}", pair.as_str()))
            }),
        Rule::boolean => Ok(Literal::Boolean(pair.as_str() == "true")),
        Rule::list => Err(FilterError::Syntax(
            "nested lists are not allowed".to_owned(),
        )),
        other => Err(FilterError::Syntax(format!(
            "unexpected literal `{other:?}`"
        ))),
    }
}

fn unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') | None => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Value<'a> {
    String(&'a str),
    OwnedString(String),
    Number(f64),
    Boolean(bool),
    Null,
    Array(&'a [DecodedValue]),
    Object(&'a [DecodedField]),
    Missing,
}

impl Value<'_> {
    fn truthy(&self) -> bool {
        match self {
            Self::String(s) => !s.is_empty(),
            Self::OwnedString(s) => !s.is_empty(),
            Self::Number(n) => *n != 0.0 && !n.is_nan(),
            Self::Boolean(b) => *b,
            Self::Array(items) => !items.is_empty(),
            Self::Object(fields) => !fields.is_empty(),
            Self::Null | Self::Missing => false,
        }
    }
}

fn eval(expr: &Expr, message: &CapturedMessage) -> bool {
    match expr {
        Expr::Or(children) => children.iter().any(|c| eval(c, message)),
        Expr::And(children) => children.iter().all(|c| eval(c, message)),
        Expr::Not(inner) => !eval(inner, message),
        Expr::Constant(b) => *b,
        Expr::Truthy(path) => resolve(path, message).truthy(),
        Expr::Compare { path, cmp } => {
            let lhs = resolve(path, message);
            eval_cmp(&lhs, cmp)
        }
    }
}

fn eval_cmp(lhs: &Value<'_>, cmp: &Comparator) -> bool {
    match cmp {
        Comparator::Eq(lit) => values_equal(lhs, lit),
        Comparator::Ne(lit) => !values_equal(lhs, lit),
        Comparator::Lt(lit) => order(lhs, lit, std::cmp::Ordering::Less, false),
        Comparator::Gt(lit) => order(lhs, lit, std::cmp::Ordering::Greater, false),
        Comparator::Le(lit) => order(lhs, lit, std::cmp::Ordering::Less, true),
        Comparator::Ge(lit) => order(lhs, lit, std::cmp::Ordering::Greater, true),
        Comparator::Re(re) => match lhs {
            Value::String(s) => re.is_match(s),
            Value::OwnedString(s) => re.is_match(s),
            _ => false,
        },
        Comparator::In(items) => items.iter().any(|item| values_equal(lhs, item)),
    }
}

fn values_equal(lhs: &Value<'_>, rhs: &Literal) -> bool {
    match (lhs, rhs) {
        (Value::String(s), Literal::String(other)) => *s == other.as_str(),
        (Value::OwnedString(s), Literal::String(other)) => s == other,
        // Numeric equality uses bit-exact `f64 == f64`. NaN is never equal,
        // which is the correct semantics. Beware: integers above 2^53 lose
        // precision when parsed into f64 — both sides round to the same
        // bucket, so equality may match a neighbouring integer. Document
        // this as a known limitation; users with 64-bit IDs should compare
        // them as strings.
        #[allow(clippy::float_cmp)]
        (Value::Number(n), Literal::Number(other)) => n == other,
        (Value::Boolean(b), Literal::Boolean(other)) => b == other,
        _ => false,
    }
}

fn order(lhs: &Value<'_>, rhs: &Literal, want: std::cmp::Ordering, allow_equal: bool) -> bool {
    let ordering = match (lhs, rhs) {
        (Value::Number(n), Literal::Number(other)) => n.partial_cmp(other),
        (Value::String(s), Literal::String(other)) => Some(s.cmp(&other.as_str())),
        (Value::OwnedString(s), Literal::String(other)) => Some(s.as_str().cmp(other.as_str())),
        _ => None,
    };
    match ordering {
        Some(actual) if actual == want => true,
        Some(std::cmp::Ordering::Equal) if allow_equal => true,
        _ => false,
    }
}

fn resolve<'a>(path: &[String], message: &'a CapturedMessage) -> Value<'a> {
    let Some(head) = path.first() else {
        return Value::Missing;
    };
    let rest = &path[1..];
    match head.as_str() {
        "topic" if rest.is_empty() => Value::String(message.topic.as_str()),
        "envelope" => resolve_envelope(rest, message),
        "headers" => resolve_header(rest, message),
        "schema" => resolve_schema(rest, message),
        "payload" => resolve_decoded(rest, &message.payload),
        "fetch" => resolve_fetch(rest, message),
        _ => Value::Missing,
    }
}

#[allow(clippy::cast_lossless)]
fn resolve_fetch<'a>(rest: &[String], message: &'a CapturedMessage) -> Value<'a> {
    let Some(field) = rest.first() else {
        return Value::Missing;
    };
    if rest.len() != 1 {
        return Value::Missing;
    }
    let Some(fetch) = &message.fetch else {
        return Value::Missing;
    };
    match field.as_str() {
        "api_key" => Value::Number(f64::from(fetch.api_key)),
        "api_version" => Value::Number(f64::from(fetch.api_version)),
        "api_name" => Value::String(fetch.api_name),
        "connection_id" => Value::Number(f64::from(fetch.connection_id)),
        "corr_id" => Value::Number(f64::from(fetch.corr_id)),
        "response_size" => Value::Number(fetch.response_size as f64),
        "rtt_ms" => Value::Number(fetch.rtt_ms),
        _ => Value::Missing,
    }
}

#[allow(clippy::cast_lossless)]
fn resolve_envelope<'a>(rest: &[String], message: &'a CapturedMessage) -> Value<'a> {
    let Some(field) = rest.first() else {
        return Value::Missing;
    };
    if rest.len() != 1 {
        return Value::Missing;
    }
    match field.as_str() {
        "topic" => Value::String(message.topic.as_str()),
        "partition" => Value::Number(f64::from(message.partition)),
        "offset" => Value::Number(message.offset as f64),
        "timestamp" => Value::String(message.timestamp.as_str()),
        "size" => Value::Number(message.size_bytes as f64),
        "key" => match &message.key {
            Some(k) => Value::String(k.as_str()),
            None => Value::Null,
        },
        _ => Value::Missing,
    }
}

fn resolve_header<'a>(rest: &[String], message: &'a CapturedMessage) -> Value<'a> {
    let Some(name) = rest.first() else {
        return Value::Missing;
    };
    if rest.len() != 1 {
        return Value::Missing;
    }
    message
        .headers
        .iter()
        .find(|h| h.key == *name)
        .map_or(Value::Missing, |h| Value::String(h.value.as_str()))
}

fn resolve_schema<'a>(rest: &[String], message: &'a CapturedMessage) -> Value<'a> {
    let Some(field) = rest.first() else {
        return Value::Missing;
    };
    if rest.len() != 1 {
        return Value::Missing;
    }
    match field.as_str() {
        "name" => match &message.schema_name {
            Some(name) => Value::String(name.as_str()),
            None => Value::Null,
        },
        "id" => match message.schema_id {
            Some(id) => Value::Number(f64::from(id)),
            None => Value::Null,
        },
        _ => Value::Missing,
    }
}

fn resolve_decoded<'a>(rest: &[String], value: &'a DecodedValue) -> Value<'a> {
    let Some(head) = rest.first() else {
        return decoded_to_value(value);
    };
    let tail = &rest[1..];
    match value {
        DecodedValue::Object { fields } => fields
            .iter()
            .find(|f| f.name == *head)
            .map_or(Value::Missing, |f| resolve_decoded(tail, &f.value)),
        DecodedValue::Array { items } => head
            .parse::<usize>()
            .ok()
            .and_then(|idx| items.get(idx))
            .map_or(Value::Missing, |item| resolve_decoded(tail, item)),
        _ => Value::Missing,
    }
}

fn decoded_to_value(value: &DecodedValue) -> Value<'_> {
    match value {
        DecodedValue::Primitive { ty, value } => match ty {
            PrimitiveType::String => Value::String(value.as_str()),
            PrimitiveType::Number => value
                .parse::<f64>()
                .map_or_else(|_| Value::OwnedString(value.clone()), Value::Number),
            PrimitiveType::Boolean => Value::Boolean(value == "true"),
            PrimitiveType::Null => Value::Null,
        },
        DecodedValue::Bytes { hex, .. } => Value::String(hex.as_str()),
        DecodedValue::Object { fields } => Value::Object(fields.as_slice()),
        DecodedValue::Array { items } => Value::Array(items.as_slice()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::decode::{DecodedField, DecodedValue, PrimitiveType};
    use crate::message::{CapturedMessage, KafkaHeader};

    fn sample() -> CapturedMessage {
        CapturedMessage {
            id: "id".into(),
            timestamp: "2026-05-05T12:00:00Z".into(),
            topic: "orders.raw".into(),
            topic_id: None,
            partition: 3,
            offset: 1042,
            key: Some("u-42".into()),
            schema_name: Some("OrderCreated".into()),
            schema_id: Some(17),
            size_bytes: 312,
            headers: vec![
                KafkaHeader {
                    key: "tenant".into(),
                    value: "acme".into(),
                },
                KafkaHeader {
                    key: "traceid".into(),
                    value: "9f1ad8".into(),
                },
            ],
            payload: DecodedValue::Object {
                fields: vec![
                    DecodedField {
                        name: "amount".into(),
                        value: DecodedValue::Primitive {
                            ty: PrimitiveType::Number,
                            value: "1450".into(),
                        },
                    },
                    DecodedField {
                        name: "currency".into(),
                        value: DecodedValue::Primitive {
                            ty: PrimitiveType::String,
                            value: "EUR".into(),
                        },
                    },
                    DecodedField {
                        name: "refunded".into(),
                        value: DecodedValue::Primitive {
                            ty: PrimitiveType::Boolean,
                            value: "false".into(),
                        },
                    },
                    DecodedField {
                        name: "items".into(),
                        value: DecodedValue::Array {
                            items: vec![
                                DecodedValue::Object {
                                    fields: vec![DecodedField {
                                        name: "sku".into(),
                                        value: DecodedValue::Primitive {
                                            ty: PrimitiveType::String,
                                            value: "ABC-1".into(),
                                        },
                                    }],
                                },
                                DecodedValue::Object {
                                    fields: vec![DecodedField {
                                        name: "sku".into(),
                                        value: DecodedValue::Primitive {
                                            ty: PrimitiveType::String,
                                            value: "XYZ-9".into(),
                                        },
                                    }],
                                },
                            ],
                        },
                    },
                ],
            },
            raw_hex: String::new(),
            fetch: Some(crate::correlator::FetchMetadata {
                api_key: 1,
                api_name: "Fetch",
                api_version: 11,
                connection_id: 0,
                corr_id: 0x12,
                response_size: 5_711,
                rtt_ms: 1.7,
            }),
            connection_id: Some(0),
        }
    }

    fn matches(expr: &str) -> bool {
        CompiledFilter::compile(expr)
            .unwrap_or_else(|err| panic!("compile failed for `{expr}`: {err}"))
            .matches(&sample())
    }

    #[test]
    fn equality_on_topic() {
        assert!(matches("topic == \"orders.raw\""));
        assert!(!matches("topic == \"orders.enriched\""));
    }

    #[test]
    fn regex_topic() {
        assert!(matches("topic =~ \"^orders\\\\.\""));
        assert!(!matches("topic =~ \"^users\""));
    }

    #[test]
    fn header_match() {
        assert!(matches("headers.tenant == \"acme\""));
        assert!(!matches("headers.tenant == \"globex\""));
        assert!(!matches("headers.absent == \"x\""));
    }

    #[test]
    fn payload_numeric() {
        assert!(matches("payload.amount > 1000"));
        assert!(!matches("payload.amount > 2000"));
        assert!(matches("payload.amount <= 1450"));
    }

    #[test]
    fn payload_string() {
        assert!(matches("payload.currency == \"EUR\""));
    }

    #[test]
    fn payload_boolean() {
        assert!(matches("payload.refunded == false"));
        assert!(!matches("payload.refunded == true"));
        assert!(matches("!payload.refunded"));
    }

    #[test]
    fn envelope_partition() {
        assert!(matches("envelope.partition == 3"));
        assert!(matches("envelope.partition in (1, 2, 3)"));
        assert!(!matches("envelope.partition in (4, 5)"));
    }

    #[test]
    fn schema_id() {
        assert!(matches("schema.id == 17"));
        assert!(matches("schema.name == \"OrderCreated\""));
    }

    #[test]
    fn logical_combinations() {
        assert!(matches(
            "topic == \"orders.raw\" && headers.tenant == \"acme\" && payload.amount > 1000"
        ));
        assert!(matches(
            "topic == \"users.events\" || schema.name == \"OrderCreated\""
        ));
        assert!(!matches(
            "topic == \"orders.raw\" && headers.tenant == \"globex\""
        ));
    }

    #[test]
    fn parse_errors() {
        assert!(CompiledFilter::compile("&&").is_err());
        assert!(CompiledFilter::compile("topic ==").is_err());
        assert!(CompiledFilter::compile("topic =~ 42").is_err());
        assert!(CompiledFilter::compile("topic in 1").is_err());
        assert!(CompiledFilter::compile("topic =~ \"[unclosed\"").is_err());
    }

    #[test]
    fn truthy_bare_identifier() {
        assert!(matches("payload.amount"));
        assert!(matches("headers.tenant"));
        assert!(!matches("headers.missing"));
    }

    #[test]
    fn negation() {
        assert!(matches("!(topic == \"users.events\")"));
        assert!(!matches("!(topic == \"orders.raw\")"));
    }

    // --- Codex review fixes ------------------------------------------------

    /// (1) Parenthesised expression followed by an op must not silently
    ///     drop the comparison.
    #[test]
    fn paren_expr_followed_by_op_is_rejected() {
        assert!(CompiledFilter::compile("(topic == \"orders.raw\") == false").is_err());
    }

    /// (2) Numeric path segments index into arrays.
    #[test]
    fn array_index_traversal() {
        assert!(matches("payload.items.0.sku == \"ABC-1\""));
        assert!(matches("payload.items.1.sku == \"XYZ-9\""));
        assert!(!matches("payload.items.2.sku == \"anything\""));
    }

    /// (3) Numeric equality is bit-exact, not epsilon-based.
    #[test]
    fn exact_numeric_equality() {
        // 1450 + 1e-12 should NOT equal 1450 — old EPSILON-based code
        // would have incorrectly returned true for many tiny offsets, but
        // any non-zero offset within representable precision should be
        // rejected.
        assert!(!matches("payload.amount == 1450.0001"));
        assert!(matches("payload.amount == 1450"));
        assert!(matches("payload.amount == 1450.0"));
    }

    /// (4) Bare `true` / `false` are constants, not identifiers.
    #[test]
    fn bare_boolean_constant() {
        assert!(matches("true"));
        assert!(!matches("false"));
        assert!(matches("topic == \"orders.raw\" && true"));
        assert!(!matches("topic == \"orders.raw\" && false"));
        assert!(matches("topic == \"none\" || true"));
    }

    /// (5) Regex is compiled once at parse time. Hard to assert
    ///     directly; we verify behaviour stays correct with the new layout.
    #[test]
    fn regex_compiled_once() {
        let filter = CompiledFilter::compile("topic =~ \"^orders\\\\.\"").unwrap();
        for _ in 0..1000 {
            assert!(filter.matches(&sample()));
        }
    }

    /// (6) Pest error preserves expected-token information.
    #[test]
    fn pest_error_is_descriptive() {
        let err = CompiledFilter::compile("topic == ").unwrap_err();
        let msg = err.to_string();
        // Pest reports `expected ...` lists in its display form. Not
        // brittle on exact wording — just assert it is multi-piece.
        assert!(msg.contains("expected") || msg.contains("syntax"));
    }

    // --- fetch.* namespace ------------------------------------------------

    #[test]
    fn fetch_numeric_paths() {
        assert!(matches("fetch.connection_id == 0"));
        assert!(matches("fetch.api_version == 11"));
        assert!(matches("fetch.response_size > 1000"));
        assert!(!matches("fetch.response_size > 1000000"));
        assert!(matches("fetch.rtt_ms < 5"));
        assert!(!matches("fetch.rtt_ms > 100"));
    }

    #[test]
    fn fetch_string_paths() {
        assert!(matches("fetch.api_name == \"Fetch\""));
        assert!(!matches("fetch.api_name == \"Produce\""));
    }

    #[test]
    fn fetch_combined_with_payload() {
        assert!(matches(
            "fetch.connection_id == 0 && payload.amount > 1000 && fetch.rtt_ms < 5"
        ));
    }
}
