use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use hyper::body::HttpBody;
use hyper::Body;
use tokio::io::ReadBuf;
use tokio::prelude::AsyncRead;

pub struct BodyReader(pub Body);

impl AsyncRead for BodyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let body = Pin::new(&mut self.get_mut().0);

        body.poll_data(cx).map(|maybe_result| match maybe_result {
            None => Ok(()),
            Some(Err(e)) => {
                let wrapper_error = std::io::Error::new(std::io::ErrorKind::Other, Box::new(e));
                Err(wrapper_error)
            }
            Some(Ok(bytes)) => {
                buf.put_slice(&*bytes);
                Ok(())
            }
        })
    }
}
