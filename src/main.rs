use model::{Context, Query, Schema};
use std::env;
use warp::{http::Response, Filter};

fn schema() -> Schema {
    Schema::new(
        Query,
        juniper::EmptyMutation::<Context>::new(),
        juniper::EmptySubscription::<Context>::new(),
    )
}

#[tokio::main]
async fn main() {
    // std::env::set_var("RUST_LOG", "warp_async");
    env_logger::init();

    let log = warp::log("warp_server");

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/graphiql\">/graphiql</a></html>",
            )
    });

    log::info!("Listening on 127.0.0.1:8080");

    let pool = sqlx::Pool::connect(&env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();

    let state = warp::any().map(move || Context::new(pool.clone()));
    let graphql_filter = juniper_warp::make_graphql_filter(schema(), state.boxed());

    warp::serve(
        warp::get()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_filter("/graphql", None))
            .or(homepage)
            .or(warp::path("graphql").and(graphql_filter))
            .with(log),
    )
    .run(([127, 0, 0, 1], 8080))
    .await
}
