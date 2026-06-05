pub mod cache;

use self::cache::UsersCache;
use crate::bot::ChannelAction;
use crate::{
    config::Config,
    db::{
        delete_user_logs, schema::StructuredMessage, update_channels, update_opt_out,
        writer::FlushBuffer,
    },
    error::Error,
    Result,
};
use anyhow::Context;
use dashmap::DashSet;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use twitch_api::{
    helix::{
        users::{GetUsersRequest, User},
        ClientRequestError, HelixRequestGetError,
    },
    twitch_oauth2::{AppAccessToken, Scope},
    HelixClient,
};

#[derive(Clone)]
pub struct App {
    pub helix_client: HelixClient<'static, reqwest::Client>,
    pub token: Arc<RwLock<AppAccessToken>>,
    pub users: UsersCache,
    pub channels: Arc<RwLock<HashSet<String>>>,
    pub optout_codes: Arc<DashSet<String>>,
    pub optout_users: Arc<DashSet<String>>,
    pub db: Arc<clickhouse::Client>,
    pub config: Arc<Config>,
    pub flush_buffer: FlushBuffer,
    pub firehose_tx: Sender<StructuredMessage<'static>>,
}

impl App {
    /// Performs a Get Users request, regenerating the app token and retrying once
    /// if Twitch rejects the current token. App access tokens can be invalidated
    /// out from under us (expiry, manual revocation), and without this the periodic
    /// channel refresh would spam 401s until the process is restarted.
    async fn fetch_users(&self, request: GetUsersRequest<'_>) -> Result<Vec<User>> {
        let result = {
            let token = self.token.read().await;
            self.helix_client.req_get(request.clone(), &*token).await
        };

        match result {
            Ok(response) => Ok(response.data),
            Err(err) if is_unauthorized(&err) => {
                warn!("Twitch rejected the app token, regenerating");
                self.refresh_token().await?;

                let token = self.token.read().await;
                let response = self.helix_client.req_get(request, &*token).await?;
                Ok(response.data)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn refresh_token(&self) -> Result<()> {
        let new_token = AppAccessToken::get_app_access_token(
            &self.helix_client,
            self.config.client_id.clone().into(),
            self.config.client_secret.clone().into(),
            Scope::all(),
        )
        .await?;
        *self.token.write().await = new_token;
        info!("Regenerated app token");
        Ok(())
    }

    pub async fn get_users(
        &self,
        ids: Vec<String>,
        names: Vec<String>,
        ignore_cache: bool,
    ) -> Result<HashMap<String, String>> {
        let mut users = HashMap::new();
        let mut ids_to_request = Vec::new();
        let mut names_to_request = Vec::new();

        if ignore_cache {
            ids_to_request.clone_from(&ids);
            names_to_request.clone_from(&names);
        } else {
            for id in ids {
                match self.users.get_login(&id) {
                    Some(Some(login)) => {
                        users.insert(id, login);
                    }
                    Some(None) => (),
                    None => ids_to_request.push(id),
                }
            }

            for name in names {
                match self.users.get_id(&name) {
                    Some(Some(id)) => {
                        users.insert(id, name);
                    }
                    Some(None) => (),
                    None => names_to_request.push(name),
                }
            }
        }

        let mut new_users = Vec::with_capacity(ids_to_request.len() + names_to_request.len());

        // There are no chunks if the vec is empty, so there is no empty request made
        for chunk in ids_to_request.chunks(100) {
            debug!("Requesting user info for ids {chunk:?}");

            new_users.extend(self.fetch_users(GetUsersRequest::ids(chunk)).await?);
        }

        for chunk in names_to_request.chunks(100) {
            debug!("Requesting user info for names {chunk:?}");

            new_users.extend(self.fetch_users(GetUsersRequest::logins(chunk)).await?);
        }

        for user in new_users {
            let id = user.id.to_string();
            let login = user.login.to_string();

            self.users.insert(id.clone(), login.clone());

            users.insert(id, login);
        }

        // Banned users which were not returned by the api
        for id in ids_to_request {
            if !users.contains_key(id.as_str()) {
                self.users.insert_optional(Some(id), None);
            }
        }
        for name in names_to_request {
            if !users.values().any(|login| login == name.as_str()) {
                self.users.insert_optional(None, Some(name));
            }
        }

        Ok(users)
    }

    pub async fn get_user_id_by_name(&self, name: &str) -> Result<String> {
        match self.users.get_id(name) {
            Some(Some(id)) => Ok(id),
            Some(None) => Err(Error::NotFound),
            None => {
                let users = self.fetch_users(GetUsersRequest::logins(vec![name])).await?;
                match users.into_iter().next() {
                    Some(user) => {
                        let user_id = user.id.to_string();
                        self.users.insert(user_id.clone(), user.login.to_string());
                        Ok(user_id)
                    }
                    None => {
                        self.users.insert_optional(None, Some(name.to_owned()));
                        Err(Error::NotFound)
                    }
                }
            }
        }
    }

    pub async fn optout_user(&self, user_id: &str) -> anyhow::Result<()> {
        delete_user_logs(&self.db, user_id)
            .await
            .context("Could not delete logs")?;

        self.optout_users.insert(user_id.to_owned());
        update_opt_out(&self.db, user_id, true)
            .await
            .context("Could not save opt-out state")?;
        info!("User {user_id} opted out");

        Ok(())
    }

    pub fn check_opted_out(&self, channel_id: &str, user_id: Option<&str>) -> Result<()> {
        if self.optout_users.contains(channel_id) {
            return Err(Error::ChannelOptedOut);
        }

        if let Some(user_id) = user_id {
            if self.optout_users.contains(user_id) {
                return Err(Error::UserOptedOut);
            }
        }

        Ok(())
    }

    pub async fn update_channels(&self, channels: &[String], action: ChannelAction) -> Result<()> {
        update_channels(&self.db, channels, action).await?;
        {
            let mut guard = self.channels.write().await;
            for channel in channels {
                match action {
                    ChannelAction::Join => {
                        guard.insert(channel.clone());
                    }
                    ChannelAction::Part => {
                        guard.remove(channel);
                    }
                }
            }
        }
        Ok(())
    }
}

fn is_unauthorized(err: &ClientRequestError<reqwest::Error>) -> bool {
    matches!(
        err,
        ClientRequestError::HelixRequestGetError(HelixRequestGetError::Error { status, .. })
            if *status == reqwest::StatusCode::UNAUTHORIZED
    )
}
