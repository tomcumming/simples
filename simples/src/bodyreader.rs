use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use hyper::body::{Bytes, HttpBody};
use hyper::Body;
use tokio::io::ReadBuf;
use tokio::prelude::AsyncRead;

pub struct BodyReader {
    body: Body,
    left_over: Option<Bytes>,
}

impl BodyReader {
    pub fn new(body: Body) -> BodyReader {
        BodyReader {
            body,
            left_over: None,
        }
    }
}

impl AsyncRead for BodyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let br = self.get_mut();
        match br.left_over.take() {
            Some(mut left_over) => {
                let next_left_over = if left_over.len() > buf.remaining() {
                    left_over.split_off(buf.remaining())
                } else {
                    Bytes::new()
                };
                // left_over gets mutated above!
                buf.put_slice(&*left_over);
                if !next_left_over.is_empty() {
                    br.left_over = Some(next_left_over);
                }
                Poll::Ready(Ok(()))
            }
            None => {
                let body = Pin::new(&mut br.body);

                body.poll_data(cx).map(|maybe_result| match maybe_result {
                    None => Ok(()),
                    Some(Err(e)) => {
                        let wrapper_error =
                            std::io::Error::new(std::io::ErrorKind::Other, Box::new(e));
                        Err(wrapper_error)
                    }
                    Some(Ok(mut bytes)) => {
                        let next_left_over = if bytes.len() > buf.remaining() {
                            bytes.split_off(buf.remaining())
                        } else {
                            Bytes::new()
                        };
                        buf.put_slice(&*bytes);
                        if !next_left_over.is_empty() {
                            br.left_over = Some(next_left_over);
                        }
                        Ok(())
                    }
                })
            }
        }
    }
}
