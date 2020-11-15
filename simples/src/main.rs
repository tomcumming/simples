mod error;
mod topicname;
mod writequeue;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::net::SocketAddr;
use std::path::Path;

use crate::error::{BoxedError, Error};
use crate::topicname::TopicName;
use crate::writequeue::WriteRequest;

fn parse_path_parts<'a>(path: &'a str) -> Box<[&'a str]> {
    let mut path_parts = path.split("/").skip(1).collect::<Vec<_>>();
    if path_parts.last() == Some(&"") {
        path_parts.pop();
    }
    path_parts.into_boxed_slice()
}

async fn index_page(_req: Request<Body>) -> Result<Response<Body>, BoxedError> {
    Ok(Response::new(
        format!("Simples ver. {}", env!("CARGO_PKG_VERSION")).into(),
    ))
}

async fn create_topic(_req: Request<Body>, name: &str) -> Result<Response<Body>, BoxedError> {
    let topic_name = TopicName::parse(name).ok_or(Error::InvalidTopicName)?;

    let topic_path = Path::new("topics").join(topic_name.to_str());

    let metadata = tokio::fs::metadata(&topic_path).await;
    if metadata.is_ok() {
        Ok(Response::new("false".into()))
    } else {
        tokio::fs::create_dir_all(&topic_path).await?;
        Ok(Response::new("true".into()))
    }
}

async fn append_item(_req: Request<Body>, name: &str) -> Result<Response<Body>, BoxedError> {
    todo!()
}

async fn handle(req: Request<Body>) -> Result<Response<Body>, BoxedError> {
    let path_parts = parse_path_parts(req.uri().path());

    match (req.method(), path_parts.as_ref()) {
        (&Method::GET, [""]) => index_page(req).await,
        (&Method::PUT, ["topic", name]) => {
            let name = name.to_string();
            create_topic(req, name.as_ref()).await
        }
        (&Method::POST, ["topic", name, "items"]) => {
            let name = name.to_string();
            append_item(req, name.as_ref()).await
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("404".into())
            .expect("Builder to never fail")),
    }
}

#[tokio::main]
async fn main() {
    let (_send_write, _recv_write) = tokio::sync::mpsc::channel::<WriteRequest>(1);

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, BoxedError>(service_fn(|req| handle(req))) });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
