# async-bson

An asynchronous BSON parser that only parses explicitly specified subset of fields. Useful for extracting a handful of fields from a larger document.

It works by having the caller initialize a `DocumentParser`, specifying the fields to be extracted. Then calling `parse_document` with a stream it goes through the input, extracting the specified elements and ignoring the rest.

The emphasis on *asynchronous* -- this is a streaming parser that does not require the whole BSON to be loaded into memory.

## Example:
```
use async_bson::{DocumentParser, Document};

#[tokio::main]

async fn main() {
    // This is our BSON "stream"
    let buf = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";

    // Parse the value of /hello, storing the value under "foo"
    let parser = DocumentParser::new().match_exact("/hello", "foo");
    let doc = parser.parse_document(&buf[..]).await.unwrap();

    assert_eq!("world", doc.get_str("foo").unwrap());
}
```
