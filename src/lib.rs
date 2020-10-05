//! An asynchronous BSON parser that only looks at document fields that are explicitly
//! selected. Useful for extracting only a handful of fields from a large BSON document.
//!
//! It works by having the caller specify a `FieldSelector`, indicating which fields
//! need to be collected. The parser then goes through the BSON, collecting what was
//! asked and skipping unneeded fields.
//!
//! The emphasis on *asynchronous* -- this is a streaming parser that does not require the whole
//! BSON to be loaded into memory.
//!
//! # Examples:
//!
//! Extract some fields and an array length from the document:
//! ```ignore
//! use bson_lite::{FieldSelector, Document};
//!
//! // doc = { "foo": 1, "nested": { "array": [1, 2, 3] } }
//! let selector = FieldSelector::build()
//!     .with("foo", "/foo")
//!     .with("bar", "/nested/array/0")
//!     .with("len", "/nested/array/[]");
//!
//! let doc = Document::from_reader(rdr, &selector).await.unwrap();
//! let foo = doc.get_i32("foo").unwrap();
//! ```
//!

use std::fmt;
use std::io::{Error, ErrorKind};
use std::collections::{HashMap, HashSet};

use async_recursion::async_recursion;
use tokio::io::{self, AsyncRead, AsyncReadExt, Result};

/// Specifies the fields that we want to extract from the BSON stream.
///
#[derive(Debug)]
pub struct FieldSelector<'a> {
    // Match labels keyed by the fully qualified element name (/ as separator) or alternatively
    // with the element position (@<position number> instead of name)
    matchers: HashMap<&'a str, String>,

    // Map of subdocument prefixes that we are interested in. We're using this to skip
    // documents that don't contain anything interesting.
    match_prefixes: HashSet<&'a str>,
}

impl<'a> FieldSelector<'a> {
    pub fn build() -> Self {
        FieldSelector {
            matchers: HashMap::new(),
            match_prefixes: HashSet::new(),
        }
    }

    pub fn with(mut self, match_label: &'a str, match_pattern: &'a str) -> Self {
        self.matchers.insert(match_pattern, match_label.to_owned());

        // Now make a note of all the prefixes leading up to the exact value. So that
        // encountering /foo/bar/baz we insert /foo/bar/baz, /foo/bar and /foo
        let mut prefix = match_pattern;
        while let Some(pos) = prefix.rfind('/') {
            prefix = &prefix[..pos];
            if !prefix.is_empty() {
                self.match_prefixes.insert(prefix);
            }
        }
        self
    }

    fn get(&self, field: &str) -> Option<&String> {
        self.matchers.get(field)
    }

    fn want_prefix(&self, prefix: &str) -> bool {
        self.match_prefixes.contains(prefix)
    }
}

#[derive(Debug, Clone)]
enum BsonValue {
    Float(f64),
    String(String),
    Int32(i32),
    Int64(i64),
    ObjectId([u8; 12]),
    Boolean(bool),
    Placeholder(&'static str),
    None,
}

impl fmt::Display for BsonValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BsonValue::Float(v) => v.fmt(f),
            BsonValue::String(v) => write!(f, "\"{}\"", v),
            BsonValue::Int32(v) => v.fmt(f),
            BsonValue::Int64(v) => v.fmt(f),
            BsonValue::ObjectId(v) => write!(f, "ObjectId({:?})", v),
            BsonValue::Boolean(v) => v.fmt(f),
            BsonValue::Placeholder(v) => v.fmt(f),
            other => write!(f, "Other({:?})", other),
        }
    }
}

/// The document extracted from the BSON stream by applying `FieldSelector`.
#[derive(Debug)]
pub struct Document {
    doc: HashMap<String, BsonValue>,
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ ")?;
        for (i, (k, v)) in self.doc.iter().enumerate() {
            let comma = if i == self.doc.len() - 1 { "" } else { "," };
            write!(f, "{}: {}{} ", k, v, comma)?;
        }
        write!(f, "}}")
    }
}

impl Document {
    // Creates an empty Document.
    fn new() -> Self {
        Document {
            doc: HashMap::new(),
        }
    }

    /// Return the str value for this key, if it exists.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.doc.get(key) {
            Some(BsonValue::String(result)) => Some(result),
            _ => None,
        }
    }

    /// Returns the float value for this key, if it exists.
    pub fn get_float(&self, key: &str) -> Option<f64> {
        match self.doc.get(key) {
            Some(BsonValue::Float(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns the i32 value for this key, if it exists.
    pub fn get_i32(&self, key: &str) -> Option<i32> {
        match self.doc.get(key) {
            Some(BsonValue::Int32(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns the i64 value for this key, if it exists.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.doc.get(key) {
            Some(BsonValue::Int64(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns true if the document contains the key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.doc.contains_key(key)
    }

    fn insert(&mut self, key: String, value: BsonValue) {
        self.doc.insert(key, value);
    }

    /// Returns the number of keys in the document.
    pub fn len(&self) -> usize {
        self.doc.len()
    }

    /// Collect a new document from byte stream.
    /// Only the fields specified in the selector are collected the rest
    /// of it is simply discarded.
    pub async fn from_reader<'a, R: AsyncRead + Unpin + Send>(
        mut rdr: R,
        selector: &FieldSelector<'a>,
    ) -> Result<Document> {
        let document_size = rdr.read_i32_le().await?;

        let mut doc = Document::new();
        parse_document(&mut rdr.take(document_size as u64), &selector, "", 0, &mut doc).await?;

        Ok(doc)
    }

}

#[async_recursion]
async fn parse_document<R: AsyncRead + Unpin + Send>(
    mut rdr: &mut R,
    selector: &FieldSelector<'_>,
    prefix: &str,
    position: u32,
    mut doc: &mut Document,
) -> Result<()> {
    let mut position = position;

    loop {
        position += 1;

        let elem_type = rdr.read_u8().await?;

        if elem_type == 0x00 {
            break;
        }

        let elem_name = read_cstring(&mut rdr).await?;

        let prefix_name = format!("{}/{}", prefix, elem_name);
        let prefix_pos = format!("{}/@{}", prefix, position);

        // Check if we just want the element name
        let want_field_name_by_pos = format!("{}/#{}", prefix, position);
        if let Some(item_key) = selector.get(&want_field_name_by_pos) {
            doc.insert(
                item_key.to_string(),
                BsonValue::String(elem_name.to_string()),
            );
        }

        // Check if we just want the count of keys (i.e array len)
        // Take a simple approach and just set the array len to current position
        // we end up updating it for every value, but the benefit is simplicity.
        let want_array_len = format!("{}/[]", prefix);
        if let Some(item_key) = selector.get(&want_array_len) {
            doc.insert(item_key.to_string(), BsonValue::Int32(position as i32));
        }

        // List of wanted elements. tuple of (name prefix, name alias)
        let mut wanted_elements = Vec::new();
        for elem_prefix in [&prefix_name, &prefix_pos].iter() {
            if let Some(elem_name) = selector.get(elem_prefix) {
                wanted_elements.push(elem_name);
            }
        }

        let want_this_value = !wanted_elements.is_empty();

        let elem_value = match elem_type {
            0x01 => {
                // A float
                let mut buf = [0 as u8; 8];
                rdr.read(&mut buf).await?;
                BsonValue::Float(f64::from_le_bytes(buf))
            }
            0x02 => {
                // String
                let str_len = rdr.read_i32_le().await?;
                if want_this_value {
                    BsonValue::String(read_string_with_len(&mut rdr, str_len as usize).await?)
                } else {
                    skip_bytes(&mut rdr, str_len as usize).await?;
                    BsonValue::None
                }
            }
            0x03 | 0x04 => {
                // Embedded document or an array. Both are represented as a document.
                // We only go through the trouble of parsing this if the field selector
                // wants the document value or some element within it.
                let _doc_len = rdr.read_i32_le().await?;
                if want_this_value || selector.want_prefix(&prefix_name) {
                    parse_document(rdr, selector, &prefix_name, 0, &mut doc).await?;
                    BsonValue::Placeholder("<nested document>")
                } else {
                    skip_bytes(&mut rdr, _doc_len as usize - 4).await?;
                    BsonValue::None
                }
            }
            0x05 => {
                // Binary data
                let len = rdr.read_i32_le().await?;
                skip_bytes(&mut rdr, (len + 1) as usize).await?;
                BsonValue::Placeholder("<binary data>")
            }
            0x06 => {
                // Undefined value. Deprecated.
                BsonValue::None
            }
            0x07 => {
                let mut bytes = [0 as u8; 12];
                rdr.read_exact(&mut bytes).await?;
                BsonValue::ObjectId(bytes)
            }
            0x08 => {
                // Boolean
                let val = match rdr.read_u8().await? {
                    0x00 => false,
                    _ => true,
                };
                BsonValue::Boolean(val)
            }
            0x09 => {
                // UTC Datetime
                skip_bytes(&mut rdr, 8).await?;
                BsonValue::Placeholder("<UTC datetime>")
            }
            0x0A => {
                // Null value
                BsonValue::None
            }
            0x0B => {
                // Regular expression
                let _regx = read_cstring(&mut rdr).await?;
                let _opts = read_cstring(&mut rdr).await?;
                BsonValue::Placeholder("<regex>")
            }
            0x0C => {
                // DBPointer. Deprecated.
                let len = rdr.read_i32_le().await?;
                skip_bytes(&mut rdr, (len + 12) as usize).await?;
                BsonValue::None
            }
            0x0D => {
                // Javascript code
                skip_read_len(&mut rdr).await?;
                BsonValue::Placeholder("<Javascript>")
            }
            0x0E => {
                // Symbol. Deprecated.
                skip_read_len(&mut rdr).await?;
                BsonValue::Placeholder("<symbol>")
            }
            0x0F => {
                // Code w/ scope
                skip_read_len(&mut rdr).await?;
                BsonValue::Placeholder("<Javascript with scope>")
            }
            0x10 => {
                // Int32
                BsonValue::Int32(rdr.read_i32_le().await?)
            }
            0x11 => {
                // Timestamp
                skip_bytes(&mut rdr, 8).await?;
                BsonValue::Placeholder("<timestamp>")
            }
            0x12 => {
                // Int64
                BsonValue::Int64(rdr.read_i64_le().await?)
            }
            0x13 => {
                // Decimal128
                skip_bytes(&mut rdr, 16).await?;
                BsonValue::Placeholder("<decimal128>")
            }
            0xFF => {
                // Min key.
                BsonValue::Placeholder("<min key>")
            }
            0x7F => {
                // Min key.
                BsonValue::Placeholder("<max key>")
            }
            other => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("unrecognized type: 0x{:02x}", other),
                ));
            }
        };

        for elem_name in wanted_elements.iter() {
            doc.insert(elem_name.to_string(), elem_value.clone());
        }
    }
    Ok(())
}

async fn skip_bytes<T: AsyncRead + Unpin>(rdr: &mut T, bytes_to_skip: usize) -> Result<u64> {
    io::copy(&mut rdr.take(bytes_to_skip as u64), &mut tokio::io::sink()).await
}

async fn skip_read_len<T: AsyncRead + Unpin>(rdr: &mut T) -> Result<u64> {
    let str_len = rdr.read_i32_le().await?;
    skip_bytes(rdr, str_len as usize).await
}

/// Read a null terminated string from async stream.
pub async fn read_cstring<R: AsyncRead + Unpin>(rdr: &mut R) -> Result<String> {
    let mut bytes = Vec::new();

    // XXX: this seems terribly inefficient
    while let Ok(b) = rdr.read_u8().await {
        if b == 0x00 {
            break;
        } else {
            bytes.push(b);
        }
    }

    if let Ok(res) = String::from_utf8(bytes) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

async fn read_string_with_len<R: AsyncRead + Unpin>(rdr: R, str_len: usize) -> Result<String> {
    let mut buf = Vec::with_capacity(str_len);
    rdr.take(str_len as u64).read_to_end(&mut buf).await?;

    // Remove the trailing null, we won't need it
    let _ = buf.pop();

    if let Ok(res) = String::from_utf8(buf) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

mod tests {

    #[tokio::test]
    async fn test_parse_bson() {
        use super::*;
        use bson::doc;

        let doc = doc! {
            "a_string": "foo",
            "an_f64": 3.14,
            "an_i32": 123i32,
            "an_i64": 12345678910i64,
            "oid": bson::oid::ObjectId::with_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            "bool": true,
            "nested": {
                "monkey": {
                    "name": "nilsson",
                },
            },
            "deeply": {
                "nested": {
                    "array": [1, 2, 3],
                },
            },
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();

        let selector = FieldSelector::build()
            .with("first_elem_value", "/@1")
            .with("first_elem_name", "/#1")
            .with("string", "/a_string")
            .with("f64", "/an_f64")
            .with("i32", "/an_i32")
            .with("i64", "/an_i64")
            .with("array_len", "/deeply/nested/array/[]")
            .with("array_first", "/deeply/nested/array/@1")
            .with("monkey", "/nested/monkey/name");

        let doc = Document::from_reader(&buf[..], &selector).await.unwrap();

        assert_eq!("a_string", doc.get_str("first_elem_name").unwrap());
        assert_eq!("foo", doc.get_str("first_elem_value").unwrap());
        assert_eq!(3.14, doc.get_float("f64").unwrap());
        assert_eq!(123, doc.get_i32("i32").unwrap());
        assert_eq!(12345678910i64, doc.get_i64("i64").unwrap());
        assert_eq!(3, doc.get_i32("array_len").unwrap());
        assert_eq!(1, doc.get_i32("array_first").unwrap());
        assert_eq!("nilsson", doc.get_str("monkey").unwrap());
        assert_eq!(9, doc.len());
    }

    #[tokio::test]
    async fn test_multiple_docs() {
        use super::*;
        use bson::doc;
        use std::io::Cursor;

        let mut buf = Vec::new();

        let doc = doc! {
            "foo": 1,
        };
        doc.to_writer(&mut buf).unwrap();
        let doc = doc! {
            "bar": 2,
        };
        doc.to_writer(&mut buf).unwrap();

        let selector = FieldSelector::build()
            .with("foo", "/foo")
            .with("bar", "/bar");

        let mut cursor = Cursor::new(&buf[..]);
        let doc = Document::from_reader(&mut cursor, &selector).await.unwrap();
        assert_eq!(1, doc.get_i32("foo").unwrap());

        let doc = Document::from_reader(&mut cursor, &selector).await.unwrap();
        assert_eq!(2, doc.get_i32("bar").unwrap());
    }
    #[tokio::test]
    async fn test_nested_array() {
        use super::*;
        use bson::doc;

        let doc = doc! {
            "f": doc! {
                "array": [
                    doc! { "foo": 42 },
                    doc! { "bar": 43 },
                    doc! { "baz": 44 },
                ],
            },
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();

        let selector = FieldSelector::build()
            .with("a", "/f/array/[]")
            .with("b", "/f/array/0/foo")
            .with("c", "/f/array/2/baz");

        let doc = Document::from_reader(&buf[..], &selector).await.unwrap();

        assert_eq!(3, doc.get_i32("a").unwrap());
        assert_eq!(42, doc.get_i32("b").unwrap());
        assert_eq!(44, doc.get_i32("c").unwrap());
    }

    #[tokio::test]
    async fn test_read_cstring() {
        use super::*;
        use std::io::Cursor;

        let buf = b"kala\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).await.unwrap();
        assert_eq!(res, "kala");

        let buf = b"\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).await.unwrap();
        assert_eq!(res, "");
    }
}
