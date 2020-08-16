use dataloader::cached::Loader;
use juniper::{graphql_object, FieldError, GraphQLObject, ID};
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;
pub mod auth;
mod comment;
mod post;
/// Top level concepts for Queries should be
/// Sub
/// User
/// Reports
/// Comment
/// TBD
/// These concepts need to be top level so that they can be linked to individually without having
/// to decend a chain.
mod sub;
mod user;

type Cursor = String;

#[derive(Debug)]
pub struct Edge<T> {
    pub node: T,
    pub cursor: Cursor,
}

#[derive(GraphQLObject, Debug)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Cursor,
}

#[derive(Debug)]
pub struct Page<T> {
    pub total_count: i32,
    pub edges: Vec<Edge<T>>,
    pub page_info: PageInfo,
}

type GLoader<Key, Value, L> =
    Loader<Key, Result<Value, Arc<FieldError>>, L, HashMap<Key, Result<Value, Arc<FieldError>>>>;

pub struct Context {
    pub user: auth::UserState,
    pub pool: sqlx::Pool<sqlx::Postgres>, // This should probably be any, but I didn't compile with any so ???
    pub sub_loader: GLoader<UniCase<String>, sub::Sub, sub::SubLoader>,
    pub user_loader: GLoader<UniCase<String>, user::User, user::UserLoader>,
    pub post_loader: GLoader<i32, post::Post, post::PostLoader>,
    pub comment_loader: GLoader<String, comment::Comment, comment::CommentLoader>,
}
impl Context {
    pub fn new(user: auth::UserState, pool: sqlx::Pool<sqlx::Postgres>) -> Self {
        Context {
            user,
            pool: pool.clone(),
            sub_loader: Loader::new(sub::SubLoader { pool: pool.clone() }),
            user_loader: Loader::new(user::UserLoader { pool: pool.clone() }),
            comment_loader: Loader::new(comment::CommentLoader { pool: pool.clone() }),
            post_loader: Loader::new(post::PostLoader { pool }),
        }
    }
}

impl juniper::Context for Context {}

pub struct Query;
#[graphql_object(
    context = Context,
)]
impl Query {
    fn apiVersion() -> &'static str {
        "1.0"
    }

    async fn get_subs(
        context: &Context,
        count: Option<i32>,
        after: Option<String>,
    ) -> Result<Page<sub::Sub>, FieldError> {
        sub::get_subs(context, count, after).await
    }

    async fn get_sub(context: &Context, name: String) -> Result<sub::Sub, FieldError> {
        context
            .sub_loader
            .load(name.into())
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    async fn get_post(context: &Context, id: ID) -> Result<post::Post, FieldError> {
        context
            .post_loader
            .load(id.parse::<i32>()?)
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    async fn get_home_posts(
        context: &Context,
        count: Option<i32>,
        after: Option<String>,
    ) -> Result<Page<post::Post>, FieldError> {
        post::get_home_posts(context, count, after).await
    }

    async fn get_user(context: &Context, name: String) -> Result<user::User, FieldError> {
        context
            .user_loader
            .load(name.into())
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    async fn get_comment(context: &Context, id: ID) -> Result<comment::Comment, FieldError> {
        context
            .comment_loader
            .load(id.to_string())
            .await
            .map_err(|err| format!("{:?}", err).into())
    }
}
pub struct Mutation;

pub type Schema = juniper::RootNode<
    'static,
    Query,
    juniper::EmptyMutation<Context>,
    juniper::EmptySubscription<Context>,
>;
