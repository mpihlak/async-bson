use async_bson::DocumentParser;

use bson;
use bson::doc;

// The benchmark results for unoptimized 100k iterations are:
// 9.64 seconds utime for this parser
// 5.52 seconds utime for optimised bson_lite (non-async version of this)
// 7.89 seconds utime for the MongoDb parser
//
// Our target here is to get below 7 seconds. Then we're faster
// than the Mongo parser *and* async.
//
const NUM_ITERATIONS: i32 = 100_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let doc = doc! {
        "f": doc! {
            "array": [
                doc! { "foo": "x".repeat(1000) },
                doc! { "foo": "x".repeat(1000) },
                doc! { "foo": "x".repeat(1000) },
                doc! { "foo": "x".repeat(1000) },
                doc! { "foo": "x".repeat(1000) },
                doc! { "foo": "x".repeat(1000) },
            ],
        },
    };

    let parser = DocumentParser::new()
        .field("a", "/f/array/[]")
        .field("b", "/f/array/0/foo")
        .field("c", "/f/array/2/foo");

    let mut buf = Vec::new();
    doc.to_writer(&mut buf).unwrap();

    for _ in 1..NUM_ITERATIONS {
        let _ = parser.parse_document(&buf[..]).await.unwrap();
    }

    Ok(())
}