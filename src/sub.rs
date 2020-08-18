use crate::post::{self, Post};
use crate::{user::User, Context, Cursor, Edge, Page, PageInfo};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use dataloader::BatchFn;
use futures_util::stream::StreamExt;
use juniper::{graphql_object, FieldError, FieldResult};
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;

#[derive(Debug, Clone)]
pub struct Sub {
    sid: String,
    pub name: Option<String>,
    pub nsfw: bool,
    pub sidebar: String,
    pub title: Option<String>,
    pub creation: NaiveDateTime,
}

#[graphql_object(context = Context)]
impl Sub {
    async fn subscribers(&self, context: &Context) -> Result<i32, FieldError> {
        Ok(sqlx::query!(
            r#"
            SELECT count(distinct uid) as "cnt!"
            FROM sub_subscriber
            WHERE sid = $1
                AND status = 1
                "#,
            self.sid
        )
        .fetch_one(&context.pool)
        .await?
        .cnt as i32)
    }

    async fn posts(
        &self,
        context: &Context,
        count: Option<i32>,
        after: Option<String>,
    ) -> Result<Page<Post>, FieldError> {
        post::get_related_posts(context, vec![self.sid.clone()], count, after).await
    }

    fn name(&self, _context: &Context) -> &Option<String> {
        &self.name
    }

    fn nsfw(&self, _context: &Context) -> bool {
        self.nsfw
    }

    fn sidebar(&self, _context: &Context) -> &String {
        &self.sidebar
    }

    fn title(&self, _context: &Context) -> &Option<String> {
        &self.title
    }

    async fn mods(&self, context: &Context) -> Vec<User> {
        let ids = sqlx::query!(
            r#"
            SELECT uid
            FROM sub_mod
            WHERE sid = $1
            "#,
            self.sid
        )
        .fetch(&context.pool)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|m| match m {
            Ok(m) => Some(UniCase::new(m.uid)),
            _ => None,
        })
        .collect::<Vec<_>>();

        context
            .user_loader
            .load_many(ids)
            .await
            .values()
            .filter_map(|user| user.clone().ok())
            .collect::<Vec<_>>()
    }

    fn creation(&self, _context: &Context) -> &NaiveDateTime {
        &self.creation
    }
}

#[graphql_object(name = "PostNode", context = Context)]
impl Edge<Post> {
    fn node(&self) -> &Post {
        &self.node
    }

    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[graphql_object(name = "PostPage", context = Context)]
impl Page<Post> {
    fn edges(&self) -> &Vec<Edge<Post>> {
        &self.edges
    }

    fn page_info(&self) -> &PageInfo {
        &self.page_info
    }

    fn total_count(&self) -> i32 {
        self.total_count
    }
}

#[graphql_object(name = "SubsNode", context = Context)]
impl Edge<Sub> {
    fn node(&self) -> &Sub {
        &self.node
    }

    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[graphql_object(name = "SubsPage", context = Context)]
impl Page<Sub> {
    fn edges(&self) -> &Vec<Edge<Sub>> {
        &self.edges
    }

    fn page_info(&self) -> &PageInfo {
        &self.page_info
    }

    fn total_count(&self) -> i32 {
        self.total_count
    }
}

pub async fn get_subs(
    context: &Context,
    count: Option<i32>,
    after: Option<String>,
) -> FieldResult<Page<Sub>> {
    let count = count.unwrap_or(50);
    let after = after.unwrap_or_default();

    let edges = sqlx::query_as!(
        Sub,
        r#"
        SELECT name, nsfw, sidebar, title, creation, sid 
        FROM sub
        WHERE name > $1
        LIMIT $2
        "#,
        after,
        count as i64
    )
    .fetch(&context.pool)
    .map(|sub| -> sqlx::Result<Edge<Sub>> {
        let sub = sub?;
        let name = sub.name.as_ref().unwrap_or(&"".to_string()).clone();

        Ok(Edge {
            node: sub,
            cursor: name,
        })
    })
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    let end_cursor = edges
        .iter()
        .last()
        .map_or("".into(), |val| val.cursor.clone());

    Ok(Page {
        edges,
        total_count: sqlx::query!(r#"select count(*) as "cnt!" from sub"#)
            .fetch_one(&context.pool)
            .await?
            .cnt as i32,
        page_info: PageInfo {
            has_next_page: end_cursor != "",
            end_cursor,
        },
    })
}

pub struct SubLoader {
    pub pool: sqlx::PgPool,
}

#[async_trait]
impl BatchFn<unicase::UniCase<String>, Result<Sub, Arc<FieldError>>> for SubLoader {
    async fn load(
        &self,
        keys: &[unicase::UniCase<String>],
    ) -> HashMap<unicase::UniCase<String>, Result<Sub, Arc<FieldError>>> {
        let sql_keys = keys
            .iter()
            .map(|case| case.clone().into())
            .collect::<Vec<String>>();

        let results: Vec<_> = sqlx::query_as!(
            Sub,
            r#"SELECT sid, name, creation, title, sidebar, nsfw
            FROM sub
            WHERE lower(name) in (select lower(x) FROM unnest($1::text[]) x)
            OR sid = ANY($1::text[])
            "#,
            &sql_keys
        )
        .fetch(&self.pool)
        .collect::<Vec<_>>()
        .await;

        let mut map: HashMap<unicase::UniCase<String>, Result<Sub, Arc<FieldError>>> =
            HashMap::new();

        results.iter().for_each(|value| {
            if let Ok(value) = value {
                map.insert(value.sid.clone().into(), Ok(value.clone()));
                if let Some(ref name) = value.name {
                    map.insert(name.clone().into(), Ok(value.clone()));
                }
            }
        });

        keys.iter().for_each(|key| {
            map.entry(key.clone())
                .or_insert_with(|| Err(Arc::new(format!("Could not find {}", key).into())));
        });

        map
    }
}
