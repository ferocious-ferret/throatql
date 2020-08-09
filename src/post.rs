use crate::{comment::Comment, Context, Cursor, Edge, Page, PageInfo};
use crate::{sub::Sub, user::User};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use dataloader::BatchFn;
use futures_util::stream::StreamExt;
use juniper::{graphql_object, FieldError, GraphQLEnum, ID};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, GraphQLEnum)]
pub enum DeleteStatus {
    Not,
    User,
    Mod,
    Admin,
}

#[derive(Debug, Clone, GraphQLEnum)]
pub enum PostType {
    Text,
    Link,
    Poll,
}

#[derive(Debug, Clone)]
pub struct Post {
    pub pid: i32,
    pub content: Option<String>,
    pub deleted: DeleteStatus,
    pub link: Option<String>,
    pub nsfw: bool,
    pub posted: Option<NaiveDateTime>,
    pub edited: Option<NaiveDateTime>,
    pub ptype: PostType,
    pub comments: Vec<String>,
    pub sid: Option<String>,
    pub thumbnail: Option<String>,
    pub title: Option<String>,
    pub uid: Option<String>,
    pub flair: Option<String>,
}

#[graphql_object(context = Context)]
impl Post {
    fn id(&self, _context: &Context) -> ID {
        self.pid.to_string().into()
    }

    fn content(&self, _context: &Context) -> &Option<String> {
        &self.content
    }

    fn deleted(&self, _context: &Context) -> &DeleteStatus {
        &self.deleted
    }

    fn link(&self, _context: &Context) -> &Option<String> {
        &self.link
    }

    fn nsfw(&self, _context: &Context) -> bool {
        self.nsfw
    }

    fn posted(&self, _context: &Context) -> &Option<NaiveDateTime> {
        &self.posted
    }

    fn edited(&self, _context: &Context) -> &Option<NaiveDateTime> {
        &self.edited
    }

    fn post_type(&self, _context: &Context) -> &PostType {
        &self.ptype
    }

    fn thumbnail(&self, _context: &Context) -> &Option<String> {
        &self.thumbnail
    }

    fn title(&self, _context: &Context) -> &Option<String> {
        &self.title
    }

    fn flair(&self, _context: &Context) -> &Option<String> {
        &self.flair
    }

    async fn sub(&self, context: &Context) -> Result<Sub, FieldError> {
        context
            .sub_loader
            .load(self.sid.clone().ok_or("Post not in a sub?")?.into())
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    async fn author(&self, context: &Context) -> Result<User, FieldError> {
        context
            .user_loader
            .load(self.uid.clone().ok_or("Post has no author")?.into())
            .await
            .map_err(|err| format!("{:?}", err).into())
    }

    async fn comments(
        &self,
        ctx: &Context,
        limit: Option<i32>,
        after: Option<Cursor>,
    ) -> Page<Result<Comment, FieldError>> {
        let limit = limit.unwrap_or(25);
        let after: String = after.unwrap_or_else(|| "".into());

        let page: Vec<_> = self
            .comments
            .iter()
            .skip_while(|cid| (after != "") && (cid != &&after))
            .take(limit as usize)
            .cloned()
            .collect();

        let comments = ctx.comment_loader.load_many(page.clone()).await;

        let page_len = page.len();

        Page {
            total_count: self.comments.len() as i32,
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
}

pub struct PostLoader {
    pub pool: sqlx::PgPool,
}

pub async fn get_related_posts(
    context: &Context,
    id: String,
    count: Option<i32>,
    after: Option<String>,
) -> Result<Page<Post>, FieldError> {
    let count = count.unwrap_or(25);
    let after: i64 = after.map(|v| v.parse().unwrap_or(0)).unwrap_or(0);

    let edges = sqlx::query!(
            r#"
            SELECT pid, content, deleted, link, nsfw, posted, edited, ptype, sid, thumbnail, title, uid, flair, c.child_arr as comments
            FROM sub_post
            LEFT JOIN ( 
                SELECT c.pid AS pid, array_agg(c.cid) as child_arr
                FROM sub_post_comment AS c
                where c.parentcid IS NULL
                GROUP BY c.pid
            ) c USING (pid)
            WHERE uid = $3 OR sid = $3
            ORDER BY posted
            LIMIT $1
            OFFSET $2
            "#, 
            count as i64,
            after as i64,
            id
        )
        .fetch(&context.pool)
        .enumerate()
        .map(|(i, post)| -> Result<Edge<Post>, FieldError> {
            let post = post?;

            Ok(Edge {
                node: Post{
                    posted: post.posted,
                    pid: post.pid,
                    flair: post.flair,
                    uid: post.uid,
                    title: post.title,
                    nsfw: post.nsfw.unwrap_or(false),
                    content: post.content,
                    thumbnail: post.thumbnail,
                    sid: post.sid,
                    comments: post.comments.unwrap_or_default(),
                    ptype: match post.ptype {
                        Some(0) => Ok(PostType::Text),
                        Some(1) => Ok(PostType::Link),
                        Some(3) => Ok(PostType::Poll),
                        _ => Err(format!("Unknown Post Type! {:?} - {:?}", post.pid, post.ptype))
                    }?,
                    edited: post.edited,
                    link: post.link,
                    deleted: match post.deleted {
                        Some(1) => Ok(DeleteStatus::User),
                        Some(2) => Ok(DeleteStatus::Mod),
                        Some(3) => Ok(DeleteStatus::Admin),
                        Some(0) => Ok(DeleteStatus::Not),
                        None => Ok(DeleteStatus::Not),
                        _ => Err(format!("Unknown Delete Type! {:?} - {:?}", post.pid, post.deleted))
                    }?,
                },
                cursor: format!("{}", i),
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
        total_count: sqlx::query!(
            r#"
                SELECT count(*) as "cnt!"
                FROM sub_post
                WHERE uid = $1 OR sid = $1
                "#,
            id
        )
        .fetch_one(&context.pool)
        .await?
        .cnt as i32,
        page_info: PageInfo {
            has_next_page: end_cursor != "",
            end_cursor,
        },
    })
}
#[async_trait]
impl BatchFn<i32, Result<Post, Arc<FieldError>>> for PostLoader {
    async fn load(&self, ids: &[i32]) -> HashMap<i32, Result<Post, Arc<FieldError>>> {
        let posts: Vec<Result<Post, FieldError>> = sqlx::query!(
            r#"
            SELECT pid, content, deleted, link, nsfw, posted, edited, ptype, sid, thumbnail, title, uid, flair, c.child_arr as comments
            FROM sub_post
            LEFT JOIN ( 
                SELECT c.pid AS pid, array_agg(c.cid) as child_arr
                FROM sub_post_comment AS c
                where c.parentcid IS NULL
                GROUP BY c.pid
            ) c USING (pid)
            WHERE pid = ANY($1)
            "#,
            ids
        )
        .fetch(&self.pool)
        .map(|post| -> Result<Post, FieldError> {
            let post = post?;
            Ok(Post {
                posted: post.posted,
                pid: post.pid,
                flair: post.flair,
                comments: post.comments.unwrap_or_default(),
                uid: post.uid,
                title: post.title,
                nsfw: post.nsfw.unwrap_or(false),
                content: post.content,
                thumbnail: post.thumbnail,
                sid: post.sid,
                ptype: match post.ptype {
                    Some(0) => Ok(PostType::Text),
                    Some(1) => Ok(PostType::Link),
                    Some(3) => Ok(PostType::Poll),
                    _ => Err(format!(
                        "Unknown Post Type! {:?} - {:?}",
                        post.pid, post.ptype
                    )),
                }?,
                edited: post.edited,
                link: post.link,
                deleted: match post.deleted {
                    Some(1) => Ok(DeleteStatus::User),
                    Some(2) => Ok(DeleteStatus::Mod),
                    Some(3) => Ok(DeleteStatus::Admin),
                    Some(0) => Ok(DeleteStatus::Not),
                    None => Ok(DeleteStatus::Not),
                    _ => Err(format!(
                        "Unknown Delete Type! {:?} - {:?}",
                        post.pid, post.deleted
                    )),
                }?,
            })
        })
        .collect()
        .await;

        let mut map: HashMap<i32, Result<Post, Arc<FieldError>>> = posts
            .into_iter()
            .filter_map(|post| {
                if let Ok(post) = post {
                    Some((post.pid, Ok(post)))
                } else {
                    None
                }
            })
            .collect();

        ids.iter().for_each(|id| {
            map.entry(id.to_owned())
                .or_insert_with(|| Err(Arc::new(format!("Post not found {}", id).into())));
        });

        map
    }
}
