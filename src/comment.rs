use crate::post::{DeleteStatus, Post};
use crate::{user::User, Context, Cursor, Edge, Page, PageInfo};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use dataloader::BatchFn;
use futures_util::stream::StreamExt;
use juniper::{graphql_object, FieldError};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct Comment {
    cid: String,
    content: Option<String>,
    last_edit: Option<NaiveDateTime>,
    parent_cid: Option<String>,
    children: Vec<String>,
    pid: Option<i32>,
    score: Option<i32>,
    up_votes: i32,
    down_votes: i32,
    status: DeleteStatus,
    time: Option<NaiveDateTime>,
    uid: Option<String>,
}

#[graphql_object(name = "CommentNode", context = Context)]
impl Edge<Result<Comment, FieldError>> {
    fn node(&self) -> Result<Comment, FieldError> {
        match &self.node {
            Ok(ref comment) => Ok(comment.clone()),
            Err(e) => Err(format!("{:?}", e).into()),
        }
    }

    fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[graphql_object(name = "CommentPage", context = Context)]
impl Page<Result<Comment, FieldError>> {
    fn edges(&self) -> &Vec<Edge<Result<Comment, FieldError>>> {
        &self.edges
    }

    fn page_info(&self) -> &PageInfo {
        &self.page_info
    }

    fn total_count(&self) -> i32 {
        self.total_count
    }
}

#[graphql_object(context = Context)]
impl Comment {
    fn cid(&self, _ctx: &Context) -> String {
        self.cid.clone()
    }

    fn content(&self, _ctx: &Context) -> Option<String> {
        self.content.clone()
    }

    fn last_edit(&self, _ctx: &Context) -> Option<NaiveDateTime> {
        self.last_edit
    }

    async fn parent(&self, _ctx: &Context) -> Result<Comment, FieldError> {
        unimplemented!()
    }

    async fn children(
        &self,
        ctx: &Context,
        limit: Option<i32>,
        after: Option<Cursor>,
    ) -> Page<Result<Comment, FieldError>> {
        let limit = limit.unwrap_or(25);
        let after: String = after.unwrap_or_else(|| "".into());

        let page: Vec<_> = self
            .children
            .iter()
            .skip_while(|cid| (after != "") && (cid != &&after))
            .take(limit as usize)
            .cloned()
            .collect();

        let comments = ctx.comment_loader.load_many(page.clone()).await;

        let page_len = page.len();

        Page {
            total_count: self.children.len() as i32,
            page_info: PageInfo {
                has_next_page: page_len as i32 == limit,
                end_cursor: page.last().cloned().unwrap_or_else(|| "".into()),
            },
            edges: comments
                .into_iter()
                .map(|comment| Edge {
                    cursor: comment.0,
                    node: comment.1.map_err(|err| format!("{:?}", err).into()),
                })
                .collect(),
        }
    }

    async fn post(&self, ctx: &Context) -> Result<Post, FieldError> {
        ctx.post_loader
            .load(self.pid.clone().ok_or("Comment not related to post?")?)
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    fn score(&self, _ctx: &Context) -> Option<i32> {
        self.score
    }

    fn up_votes(&self, _ctx: &Context) -> i32 {
        self.up_votes
    }

    fn down_votes(&self, _ctx: &Context) -> i32 {
        self.down_votes
    }

    fn time(&self, _ctx: &Context) -> Option<NaiveDateTime> {
        self.time
    }

    async fn author(&self, ctx: &Context) -> Result<User, FieldError> {
        ctx.user_loader
            .load(
                self.uid
                    .clone()
                    .ok_or("Comment not related to post?")?
                    .into(),
            )
            .await
            .map_err(|err| format!("{:?}", err).into())
    }
}

pub struct CommentLoader {
    pub pool: sqlx::PgPool,
}

#[async_trait]
impl BatchFn<String, Result<Comment, Arc<FieldError>>> for CommentLoader {
    async fn load(&self, keys: &[String]) -> HashMap<String, Result<Comment, Arc<FieldError>>>
    where
        String: 'async_trait,
        Result<Comment, Arc<FieldError>>: 'async_trait,
    {
        let comments: Vec<_> = sqlx::query!(
            r#"
                SELECT p.cid, p.content, p.lastedit, p.parentcid, p.pid, p.score, p.upvotes, 
                       p.downvotes, p.status, p.time, p.uid, c.child_arr as children
                FROM sub_post_comment   p
                LEFT JOIN ( 
                    SELECT c.parentcid AS cid, array_agg(c.cid) as child_arr
                    FROM sub_post_comment AS c
                    GROUP BY c.parentcid
                ) c USING (cid)
                WHERE p.cid = ANY($1::text[])
            "#,
            keys
        )
        .fetch(&self.pool)
        .map(|comment| -> Result<Comment, FieldError> {
            let comment = comment?;
            Ok(Comment {
                children: comment.children.unwrap_or_default(),
                cid: comment.cid.clone(),
                uid: comment.uid,
                time: comment.time,
                status: match comment.status {
                    Some(1) => Ok(DeleteStatus::User),
                    Some(2) => Ok(DeleteStatus::Mod),
                    Some(3) => Ok(DeleteStatus::Admin),
                    Some(0) => Ok(DeleteStatus::Not),
                    None => Ok(DeleteStatus::Not),
                    _ => Err(format!("Unknown Delete Status - {}", comment.cid)),
                }?,
                score: comment.score,
                parent_cid: comment.parentcid,
                pid: comment.pid,
                content: comment.content,
                down_votes: comment.downvotes,
                up_votes: comment.upvotes,
                last_edit: comment.lastedit,
            })
        })
        .collect()
        .await;

        let mut map: HashMap<String, Result<Comment, Arc<FieldError>>> = comments
            .into_iter()
            .filter_map(|comment| {
                let comment = comment.ok()?;
                Some((comment.cid.clone(), Ok(comment)))
            })
            .collect();

        keys.iter().for_each(|key| {
            map.entry(key.clone())
                .or_insert_with(|| Err(Arc::new(format!("Could not find {}", key).into())));
        });

        map
    }
}
