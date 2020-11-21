use bytes::BufMut;
use hyper::body::Bytes;
use hyper::Body;
use tokio::io::AsyncReadExt;

use disklog::reader;
use disklog::LogPosition;

use crate::query::ParsedQuery;
use crate::BoxedError;

pub struct ReadOptions {
    pub from: Option<LogPosition>,
    pub end_before: Option<LogPosition>,
    pub end_after: Option<LogPosition>,
    pub max_items: Option<usize>,
    pub wait_for_more: bool,
}

impl ReadOptions {
    pub fn from_query(mut query: ParsedQuery) -> Option<ReadOptions> {
        let mut options = ReadOptions {
            from: None,
            end_before: None,
            end_after: None,
            max_items: None,
            wait_for_more: false,
        };
        for (k, v) in query.drain() {
            match k {
                "from" => options.from = Some(v.parse().ok()?),
                "end_before" => options.end_before = Some(v.parse().ok()?),
                "end_after" => options.end_after = Some(v.parse().ok()?),
                "max_items" => options.max_items = Some(v.parse().ok()?),
                "wait_for_more" => options.wait_for_more = v.parse().ok()?,
                _ => None?,
            }
        }
        Some(options)
    }
}

enum ReaderStream {
    Between(ReadOptions, reader::Reader),
    Reading(ReadOptions, reader::LogItem),
}

async fn read_log_item_bytes(
    start_new_item: bool,
    options: ReadOptions,
    mut log_item: reader::LogItem,
) -> Result<(Bytes, ReaderStream), BoxedError> {
    let mut buf = Vec::new();
    if start_new_item {
        buf.put_u64(log_item.position());
        buf.put_u32(log_item.len());
    }
    log_item.read_buf(&mut buf).await?;

    let body_bytes = hyper::body::Bytes::from(buf);

    if log_item.left_to_read() == 0 {
        Ok((
            body_bytes,
            ReaderStream::Between(options, log_item.finish()),
        ))
    } else {
        Ok((body_bytes, ReaderStream::Reading(options, log_item)))
    }
}

async fn unfold_readerstream(
    rs: ReaderStream,
) -> Result<Option<(Bytes, ReaderStream)>, BoxedError> {
    match rs {
        ReaderStream::Between(options, reader) => {
            let end = options.max_items == Some(0)
                || options.end_before == Some(reader.position())
                || options
                    .end_after
                    .map_or(false, |pos| pos < reader.position());
            if end {
                Ok(None)
            } else {
                match reader.next(options.wait_for_more).await? {
                    reader::NextItem::Item(log_item) => {
                        read_log_item_bytes(true, options, log_item).await.map(Some)
                    }
                    reader::NextItem::End(_) => Ok(None),
                }
            }
        }
        ReaderStream::Reading(options, log_item) => read_log_item_bytes(false, options, log_item)
            .await
            .map(Some),
    }
}

pub fn read_to_body(reader: reader::Reader, options: ReadOptions) -> Body {
    let stream =
        futures::stream::try_unfold(ReaderStream::Between(options, reader), unfold_readerstream);

    Body::wrap_stream(stream)
}

#[cfg(test)]
mod tests {
    use crate::query::parse_query_string;

    use super::*;

    #[test]
    fn parse_all_options() {
        let qs = "from=1&end_before=2&end_after=3&max_items=4";
        let options = parse_query_string(qs)
            .and_then(ReadOptions::from_query)
            .expect("Valid parse");
        assert_eq!(Some(1), options.from);
        assert_eq!(Some(2), options.end_before);
        assert_eq!(Some(3), options.end_after);
        assert_eq!(Some(4), options.max_items);
    }
}
