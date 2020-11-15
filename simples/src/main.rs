use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::{convert::Infallible, net::SocketAddr};

async fn index_page(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    Ok(Response::new(
        format!("Simples ver. {}", env!("CARGO_PKG_VERSION")).into(),
    ))
}

async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path_parts = req.uri().path().split("/").skip(1).collect::<Vec<_>>();

    match (req.method(), path_parts.as_slice()) {
        (&Method::GET, [""]) => index_page(req).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("404".into())
            .expect("Builder to never fail")),
    }
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
