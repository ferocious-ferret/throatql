use futures::executor::block_on;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
    preferred_username: String,
}

#[derive(Debug, PartialEq)]
pub enum Level {
    Owner,
    Mod,
    Janitor,
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Admin,
    Mod(String, Level),
}

#[derive(Debug, PartialEq)]
pub enum UserState {
    Anonymous,
    LoggedIn {
        name: String,
        id: String,
        roles: Vec<Role>,
    },
}

lazy_static! {
    static ref PUB_KEY: String = std::env::var("PUB_KEY").unwrap();
    static ref DECODING_KEY: Result<DecodingKey<'static>, jsonwebtoken::errors::Error> =
        DecodingKey::from_rsa_pem(&PUB_KEY.as_bytes());
}

impl UserState {
    pub fn anonymous() -> UserState {
        UserState::Anonymous
    }
    pub fn login(jwt: String, pool: sqlx::PgPool) -> UserState {
        let token = decode::<Claims>(
            &jwt,
            (*DECODING_KEY).as_ref().unwrap(),
            &Validation::new(Algorithm::RS256),
        );
        // This is probably a bad idea, but I don't have a better way
        block_on(async {
            if let Ok(token) = token {
                sqlx::query!(
                    r#"
                SELECT name, uid, a.admin, m.subs, m.level 
                FROM public.user 
                LEFT JOIN (
                    SELECT uid, 1 as admin 
                    FROM user_metadata 
                    WHERE key = 'admin' AND value = '1'
                ) a USING (uid) 
                LEFT JOIN (
                    SELECT uid, array_agg(m.sid) as subs, array_agg(m.power_level) as level 
                    FROM sub_mod as m 
                    GROUP BY m.uid 
                ) m USING (uid)  
                WHERE lower(name) = $1
            "#,
                    token.claims.preferred_username
                )
                .fetch_one(&pool)
                .await
                .map(|user| UserState::LoggedIn {
                    name: user.name.unwrap_or_else(|| "".into()),
                    id: user.uid,
                    roles: {
                        let mut roles: Vec<_> = user
                            .subs
                            .unwrap_or_default()
                            .into_iter()
                            .zip(user.level.unwrap_or_default().into_iter())
                            .map(|(sub, level)| {
                                Role::Mod(
                                    sub,
                                    match level {
                                        0 => Level::Owner,
                                        1 => Level::Mod,
                                        _ => Level::Janitor,
                                    },
                                )
                            })
                            .collect();
                        if user.admin.is_some() {
                            roles.push(Role::Admin);
                        }

                        roles
                    },
                })
                .unwrap_or(UserState::Anonymous)
            } else {
                UserState::Anonymous
            }
        })
    }

    pub fn private_user_data(&self, check_id: &str) -> Result<(), String> {
        log::debug!("Auth - {:?}", self);
        match self {
            UserState::Anonymous => Err("Not Authorized".to_string()),
            UserState::LoggedIn { id, roles, .. } => {
                if id == check_id {
                    return Ok(());
                }
                if roles.contains(&Role::Admin) {
                    return Ok(());
                }
                Err("Not Authorized".to_string())
            }
        }
    }

    pub fn can_view_deleted(&self, sub_id: &str, author_id: &str) -> bool {
        match self {
            UserState::Anonymous => false,
            UserState::LoggedIn { id, roles, .. } => {
                if id == author_id {
                    true
                } else {
                    roles.iter().any(|role| match role {
                        Role::Admin => true,
                        Role::Mod(sub, _) => sub == sub_id,
                    })
                }
            }
        }
    }

    pub fn is_anon(&self) -> bool {
        if let UserState::Anonymous = self {
            true
        } else {
            false
        }
    }
}
