mod bodyreader;
mod error;
mod config;
mod query;
mod read;
mod topicname;

use std::collections::HashMap;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use tokio::sync::RwLock;

use crate::bodyreader::BodyReader;
use crate::error::BoxedError;
use crate::read::ReadOptions;
use crate::topicname::TopicName;

struct TopicState {
    writer: RwLock<disklog::writer::Writer>,
    reader_factory: disklog::reader::ReaderFactory,
}

struct ServerState {
    topics: RwLock<HashMap<TopicName, Arc<TopicState>>>,
}

const INVALID_TOPIC_NAME: &str = "Invalid topic name";
const TOPIC_NOT_FOUND: &str = "Topic not found";

fn parse_path_parts<'a>(path: &'a str) -> Box<[&'a str]> {
    let mut path_parts = path.split('/').skip(1).collect::<Vec<_>>();
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
    let topic_name = match topicname::TopicName::parse(name) {
        Some(topic_name) => topic_name,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(INVALID_TOPIC_NAME.into())?)
        }
    };

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

async fn open_or_create_topic_state(
    server_state: &ServerState,
    topic_name: &TopicName,
) -> Result<Option<Arc<TopicState>>, BoxedError> {
    {
        if let Some(topic_state) = server_state.topics.read().await.get(topic_name) {
            return Ok(Some(topic_state.clone()));
        }
    }

    let topic_path = Path::new("topics").join(topic_name.to_str());
    let metadata = tokio::fs::metadata(&topic_path).await;
    if metadata.is_ok() {
        let open_result = disklog::open_log(&topic_path).await?;
        if open_result.recovered {
            eprintln!("Recovered log: '{:?}'", topic_path);
        }
        let topic_state = Arc::new(TopicState {
            writer: RwLock::new(open_result.writer),
            reader_factory: open_result.reader_factory,
        });
        server_state
            .topics
            .write()
            .await
            .insert(topic_name.clone(), topic_state.clone());
        Ok(Some(topic_state))
    } else {
        Ok(None)
    }
}

async fn write_body(
    writer: &mut disklog::writer::Writer,
    body: Body,
) -> Result<Response<Body>, BoxedError> {
    println!("Writing body");
    let pos = writer.append(&mut BodyReader::new(body)).await?;
    Ok(Response::builder()
        .header("Content-Type", "application/json")
        .body(pos.to_string().into())?)
}

async fn append_item(
    req: Request<Body>,
    server_state: Arc<ServerState>,
    name: &str,
) -> Result<Response<Body>, BoxedError> {
    let topic_name = match topicname::TopicName::parse(name) {
        Some(topic_name) => topic_name,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(INVALID_TOPIC_NAME.into())?)
        }
    };

    if let Some(topic_state) = open_or_create_topic_state(&server_state, &topic_name).await? {
        let mut writer = topic_state.writer.write().await;
        write_body(writer.deref_mut(), req.into_body()).await
    } else {
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(TOPIC_NOT_FOUND.into())?)
    }
}

async fn read_items(
    req: Request<Body>,
    server_state: Arc<ServerState>,
    name: &str,
) -> Result<Response<Body>, BoxedError> {
    let options = query::parse_query_string(req.uri().query().unwrap_or(""))
        .and_then(ReadOptions::from_query);
    let topic_name = match topicname::TopicName::parse(name) {
        Some(topic_name) => topic_name,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(INVALID_TOPIC_NAME.into())?)
        }
    };

    if let Some(options) = options {
        if let Some(topic_state) = open_or_create_topic_state(&server_state, &topic_name).await? {
            let from = options.from.unwrap_or(0);
            let reader = topic_state.reader_factory.read_from(from).await?;
            Ok(Response::new(read::read_to_body(reader, options)))
        } else {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(TOPIC_NOT_FOUND.into())?)
        }
    } else {
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Could not read options".into())?)
    }
}

async fn handle(
    req: Request<Body>,
    server_state: Arc<ServerState>,
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
            append_item(req, server_state, name.as_ref()).await
        }
        (&Method::GET, ["topic", name, "items"]) => {
            let name = name.to_string();
            read_items(req, server_state, name.as_ref()).await
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("404".into())
            .expect("Builder to never fail")),
    }
}

#[tokio::main]
async fn main() {
    let config = match config::Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Error when reading config: {}", error);
            std::process::exit(1);
        }
    };

    let addr: std::net::SocketAddr = match config.address.parse() {
        Ok(addr) => addr,
        Err(error) => {
            eprintln!("Error parsing address '{}': {}", config.address, error);
            std::process::exit(1);
        }
    };

    println!("Launching simples {} on {}:{}", env!("CARGO_PKG_VERSION"), addr.ip(), addr.port());

    let server_state = Arc::new(ServerState {
        topics: RwLock::new(HashMap::new()),
    });

    let make_svc = make_service_fn(move |_conn| {
        let server_state = server_state.clone();
        async move {
            Ok::<_, BoxedError>(service_fn(move |req| {
                let server_state = server_state.clone();
                handle(req, server_state)
            }))
        }
    });

    if let Err(e) = Server::bind(&addr).serve(make_svc).await {
        eprintln!("server error: {}", e);
        std::process::exit(1);
    }
}
