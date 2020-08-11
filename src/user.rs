use crate::post::{self, Post};
use crate::{Context, Page};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use dataloader::BatchFn;
use futures_util::stream::StreamExt;
use juniper::{graphql_object, FieldError, GraphQLEnum};
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;

#[derive(Debug, Clone, Copy, GraphQLEnum)]
pub enum Crypto {
    BCrypt,
    KeyCloak,
}

#[derive(Debug, Clone, Copy, GraphQLEnum)]
pub enum UserStatus {
    Ok,
    Deleted,
    SiteBan,
}

#[derive(Debug, Clone)]
pub struct User {
    uid: String,
    crypto: Crypto,
    joindate: Option<NaiveDateTime>,
    name: Option<String>,
    email: Option<String>,
    password: Option<String>,

    score: i32,
    given: i32,

    status: UserStatus,
    resets: i32,
}

#[graphql_object(context = Context)]
impl User {
    fn uid(&self, _ctx: &Context) -> String {
        self.uid.clone()
    }

    fn crypto(&self, ctx: &Context) -> Result<Crypto, FieldError> {
        ctx.user.private_user_data(&self.uid)?;
        Ok(self.crypto)
    }

    fn joindate(&self, _ctx: &Context) -> Option<NaiveDateTime> {
        self.joindate
    }

    fn name(&self, _ctx: &Context) -> Option<String> {
        self.name.clone()
    }

    fn email(&self, ctx: &Context) -> Result<Option<String>, FieldError> {
        ctx.user.private_user_data(&self.uid)?;
        Ok(self.email.clone())
    }

    fn score(&self, _ctx: &Context) -> i32 {
        self.score
    }

    fn given(&self, _ctx: &Context) -> i32 {
        self.given
    }

    fn status(&self, _ctx: &Context) -> UserStatus {
        self.status
    }

    fn resets(&self, ctx: &Context) -> Result<i32, FieldError> {
        ctx.user.private_user_data(&self.uid)?;
        Ok(self.resets)
    }

    async fn posts(
        &self,
        context: &Context,
        count: Option<i32>,
        after: Option<String>,
    ) -> Result<Page<Post>, FieldError> {
        post::get_related_posts(context, self.uid.clone(), count, after).await
    }
}

pub struct UserLoader {
    pub pool: sqlx::PgPool,
}

#[async_trait]
impl BatchFn<UniCase<String>, Result<User, Arc<FieldError>>> for UserLoader {
    async fn load(
        &self,
        keys: &[UniCase<String>],
    ) -> HashMap<UniCase<String>, Result<User, Arc<FieldError>>> {
        let users: Vec<Result<User, FieldError>> = sqlx::query!(
            r#"
                SELECT uid, crypto, joindate, name, email, password, score, given, status, resets
                FROM public.user
                WHERE uid = ANY($1::text[])
                OR lower(name) = ANY($1::text[])
                "#,
            &keys
                .iter()
                .map(|key| key.to_lowercase())
                .collect::<Vec<String>>()
        )
        .fetch(&self.pool)
        .map(|user| -> Result<User, FieldError> {
            let user = user?;
            Ok(User {
                uid: user.uid.clone(),
                crypto: match user.crypto {
                    1 => Ok(Crypto::BCrypt),
                    2 => Ok(Crypto::KeyCloak),
                    _ => Err(format!(
                        "Unable to deal with crypto - {} for user {}",
                        user.crypto, user.uid
                    )),
                }?,
                status: match user.status {
                    0 => Ok(UserStatus::Ok),
                    10 => Ok(UserStatus::Deleted),
                    5 => Ok(UserStatus::SiteBan),
                    _ => Err(format!(
                        "Unable to deal with status - {} for user {}",
                        user.status, user.uid
                    )),
                }?,
                joindate: user.joindate,
                resets: user.resets,
                given: user.given,
                score: user.score,
                password: user.password,
                email: user.email,
                name: user.name,
            })
        })
        .collect()
        .await;

        log::debug!("Batch Load User - {:?}", users);

        let mut user_map: HashMap<UniCase<String>, Result<User, Arc<FieldError>>> = HashMap::new();

        users.into_iter().for_each(|user| {
            if let Ok(user) = user {
                user_map.insert(user.uid.clone().into(), Ok(user.clone()));
                if let Some(ref name) = user.name {
                    user_map.insert(name.clone().into(), Ok(user));
                }
            }
        });

        keys.iter().for_each(|id| {
            user_map
                .entry(id.to_owned())
                .or_insert_with(|| Err(Arc::new(format!("user not found - {}", id).into())));
        });

        user_map
    }
}
