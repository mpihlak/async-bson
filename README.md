# async-bson

An asynchronous streaming BSON parser that only parses explicitly specified subset of fields. Useful for extracting a handful of fields from a larger document.

It works by having the caller initialize a `DocumentParser`, specifying the fields to be extracted. Then calling `parse_document` with a stream (or a buffer) it goes through the input, extracting the specified elements and ignoring the rest.

## Key features
- This is a streaming parser, thus does not require the whole BSON to be loaded into memory.
- The streaming is implemented through async/await. The parser expects an async reader and progresses when data is available and yields when not.
- Only explicitly selected elements are parsed and stored in the result document.
- The result document is basically a flat `HashMap` keyed by element name.
- Elements can be selected by name or position.
- In addition to element values, also element names and array lengths can be collected.
- The parser can operate in synchronous mode, but expects the full BSON to be provided then.

## Example:
```rust
use async_bson::{DocumentParser, Document};

#[tokio::main]

async fn main() {
    // This is our BSON "stream"
    let buf = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";

    // Parse the value of /hello, storing the value under "foo"
    let parser = DocumentParser::builder().match_exact("/hello", "foo");
    let doc = parser.parse_document(&buf[..]).await.unwrap();

    assert_eq!("world", doc.get_str("foo").unwrap());
}
```
