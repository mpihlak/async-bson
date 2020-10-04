use std::io::Read;

use tokio::io::{AsyncRead, AsyncReadExt, Result};

struct Document {
    data: Vec<u8>,
}

// So the idea is that in the proxy we would send the bytes also over MPSC to
// a receiver. That receiver would then convert the Receiver to a Stream Reader
// that would return the buffers as a byte stream. And with that, we're in business :)

async fn parse_document<R: AsyncRead+Unpin>(mut stream: R) -> Result<Document> {
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await?;
    Ok(Document{ data })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse() {
        let buf: Vec<u8> = b"kala".to_vec();
        let doc = parse_document(&buf[..]).await.expect("parsing failed");
        assert_eq!(buf, doc.data);
    }

    #[tokio::test]
    async fn test_stream_reader() {
        use bytes::Buf;
        use tokio::sync::mpsc;
        use tokio::io::stream_reader;

        let (mut tx, mut rx) = mpsc::channel(16);

        let mut buf = &b"hello "[..];
        tx.send(Ok(buf)).await.unwrap();
        let mut buf = &b"world"[..];
        tx.send(Ok(buf)).await.unwrap();
        drop(tx);

        let mut r = stream_reader(rx);

        let doc = parse_document(&mut r).await.unwrap();

        assert_eq!(&b"hello world".to_vec(), &doc.data);
    }
    
}
