mod appendqueue;
mod error;
mod topicname;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::net::SocketAddr;
use std::path::Path;

use disklog::LogPosition;

use crate::appendqueue::AppendRequest;
use crate::error::{BoxedError, Error};
use crate::topicname::TopicName;

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
    let body = if metadata.is_ok() {
        "false"
    } else {
        tokio::fs::create_dir_all(&topic_path).await?;
        "true"
    };
    Ok(Response::builder()
        .header("Content-Type", "application/json")
        .body(body.into())?)
}

async fn append_item(
    req: Request<Body>,
    append_writer: tokio::sync::mpsc::Sender<AppendRequest>,
    name: &str,
) -> Result<Response<Body>, BoxedError> {
    let topic_name = match topicname::TopicName::parse(name) {
        Some(topic_name) => topic_name,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid topic name".into())?)
        }
    };

    let (result_sender, result_recv) = tokio::sync::oneshot::channel();

    let request = AppendRequest {
        topic_name,
        body: req.into_body(),
        result_sender,
    };

    append_writer.send(request).await?;
    let append_result = result_recv.await?;

    match append_result {
        Err(appendqueue::Error::TopicDoesNotExist) => Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Topic does not exist".into())?),
        Err(e) => Err(e)?,
        Ok(pos) => Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(pos.to_string().into())?),
    }
}

async fn handle(
    req: Request<Body>,
    append_writer: tokio::sync::mpsc::Sender<AppendRequest>,
) -> Result<Response<Body>, BoxedError> {
    let path_parts = parse_path_parts(req.uri().path());

    match (req.method(), path_parts.as_ref()) {
        (&Method::GET, []) => index_page(req).await,
        (&Method::PUT, ["topic", name]) => {
            let name = name.to_string();
            create_topic(req, name.as_ref()).await
        }
        (&Method::POST, ["topic", name, "items"]) => {
            let name = name.to_string();
            append_item(req, append_writer, name.as_ref()).await
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("404".into())
            .expect("Builder to never fail")),
    }
}

#[tokio::main]
async fn main() {
    let (append_writer, append_recv) = tokio::sync::mpsc::channel::<AppendRequest>(1);

    let make_svc = make_service_fn(move |_conn| {
        let append_writer = append_writer.clone();
        async move {
            Ok::<_, BoxedError>(service_fn(move |req| {
                let append_writer = append_writer.clone();
                handle(req, append_writer)
            }))
        }
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let (handle_appends_result, server_result) = tokio::join!(
        appendqueue::handle_appends(append_recv),
        Server::bind(&addr).serve(make_svc)
    );

    if let Err(e) = server_result {
        eprintln!("server error: {}", e);
    }
    if let Err(e) = handle_appends_result {
        eprintln!("Append handler error: {}", e);
    }
}
