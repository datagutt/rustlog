use crate::{
    app::App,
    db::schema::{StructuredMessage, UnstructuredMessage},
    logs::extract::{extract_channel_and_user_from_raw, extract_raw_timestamp},
    ShutdownRx,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::Utc;
use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::{sync::mpsc::Receiver, sync::mpsc::Sender, time::sleep};
use tracing::{debug, error, info, log::warn, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    transport::tcp::{MakeConnection, TCPTransport, TCPTransportConnectError},
    ClientConfig, MetricsConfig, TwitchIRCClient,
};

const CHANNEL_REJOIN_INTERVAL_SECONDS: u64 = 3600;
const CHANENLS_REFETCH_RETRY_INTERVAL_SECONDS: u64 = 5;

const TWITCH_IRC_SERVER: &str = "irc.chat.twitch.tv";
const TWITCH_IRC_PORT: u16 = 6697;

/// Server/port used for every IRC connection, set once at bot startup from config.
/// twitch-irc bakes the endpoint into the transport type rather than the runtime
/// `ClientConfig`, and `MakeConnection::new_socket` takes no arguments, so the
/// only way to make it configurable is to stash the target here.
static IRC_CONNECTION_CONFIG: OnceLock<(String, u16)> = OnceLock::new();

/// TLS [`MakeConnection`] that dials the endpoint in [`IRC_CONNECTION_CONFIG`]
/// instead of the hardcoded Twitch one. Mirrors twitch-irc's own webpki-roots TLS
/// connector so it shares the same rustls/crypto provider already in the build.
struct ConfigurableTLS;

#[async_trait]
impl MakeConnection for ConfigurableTLS {
    type Socket = tokio_rustls::client::TlsStream<TcpStream>;

    async fn new_socket() -> Result<Self::Socket, TCPTransportConnectError> {
        use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
        use tokio_rustls::TlsConnector;

        let (server, port) = IRC_CONNECTION_CONFIG
            .get()
            .map(|(s, p)| (s.clone(), *p))
            .unwrap_or_else(|| (TWITCH_IRC_SERVER.to_owned(), TWITCH_IRC_PORT));

        let root_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let domain = ServerName::try_from(server.clone()).map_err(|_| {
            TCPTransportConnectError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid IRC server hostname: {server}"),
            ))
        })?;

        let stream = TcpStream::connect((server.as_str(), port)).await?;
        Ok(connector.connect(domain, stream).await?)
    }
}

type TwitchClient<C> = TwitchIRCClient<TCPTransport<ConfigurableTLS>, C>;

#[derive(Debug)]
pub enum BotMessage {
    JoinChannels(Vec<String>),
    PartChannels(Vec<String>),
}

lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTERS: IntCounterVec = register_int_counter_vec!(
        "rustlog_messages_received",
        "How many messages were written",
        &["channel_id"]
    )
    .unwrap();
}

const COMMAND_PREFIX: &str = "!rustlog ";

pub async fn run<C: LoginCredentials>(
    login_credentials: C,
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
    shutdown_rx: ShutdownRx,
    command_rx: Receiver<BotMessage>,
) {
    let bot = Bot::new(app, writer_tx);
    bot.run(login_credentials, shutdown_rx, command_rx).await;
}

#[derive(Clone)]
struct Bot {
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
}

impl Bot {
    pub fn new(app: App, writer_tx: Sender<StructuredMessage<'static>>) -> Bot {
        Self { app, writer_tx }
    }

    pub async fn run<C: LoginCredentials>(
        self,
        login_credentials: C,
        mut shutdown_rx: ShutdownRx,
        mut command_rx: Receiver<BotMessage>,
    ) {
        let server = self
            .app
            .config
            .irc_server
            .clone()
            .unwrap_or_else(|| TWITCH_IRC_SERVER.to_owned());
        let port = self.app.config.irc_port.unwrap_or(TWITCH_IRC_PORT);
        info!("Connecting to IRC at {server}:{port}");
        IRC_CONNECTION_CONFIG.set((server, port)).ok();

        let client_config = ClientConfig {
            login_credentials,
            max_channels_per_connection: 400,

            max_waiting_messages_per_connection: 5,
            time_per_message: Duration::from_millis(150),

            connection_rate_limiter: Arc::new(Semaphore::new(1)),
            new_connection_every: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(20),

            metrics_config: MetricsConfig::default(),
            tracing_identifier: None,
        };
        let (mut receiver, client) =
            TwitchIRCClient::<TCPTransport<ConfigurableTLS>, C>::new(client_config);

        let app = self.app.clone();
        let join_client = client.clone();
        tokio::spawn(async move {
            loop {
                let channel_ids = app.channels.read().await.clone();

                let interval = match app
                    .get_users(Vec::from_iter(channel_ids), vec![], true)
                    .await
                {
                    Ok(users) => {
                        info!("Joining {} channels", users.len());
                        for channel_login in users.into_values() {
                            debug!("Logging channel {channel_login}");
                            join_client
                                .join(channel_login)
                                .expect("Failed to join channel");
                        }
                        CHANNEL_REJOIN_INTERVAL_SECONDS
                    }
                    Err(err) => {
                        error!("Could not fetch users list: {err}");
                        CHANENLS_REFETCH_RETRY_INTERVAL_SECONDS
                    }
                };
                sleep(Duration::from_secs(interval)).await;
            }
        });

        let bot = self.clone();
        let msg_client = client.clone();
        tokio::spawn(async move {
            while let Some(msg) = command_rx.recv().await {
                match msg {
                    BotMessage::JoinChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Join,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                    BotMessage::PartChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Part,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    if let Err(e) = self.handle_message(msg, &client).await {
                        error!("Could not handle message: {e}");
                    }
                }
                _ = shutdown_rx.changed() => {
                    debug!("Shutting down bot task");
                    break;
                }
            }
        }
    }

    async fn handle_message<C: LoginCredentials>(
        &self,
        msg: ServerMessage,
        client: &TwitchClient<C>,
    ) -> anyhow::Result<()> {
        if let ServerMessage::Privmsg(privmsg) = &msg {
            trace!("Processing message {}", privmsg.message_text);
            if let Some(cmd) = privmsg.message_text.strip_prefix(COMMAND_PREFIX) {
                if let Err(err) = self
                    .handle_command(cmd, client, &privmsg.sender.id, &privmsg.sender.login)
                    .await
                {
                    warn!("Could not handle command {cmd}: {err:#}");
                }
            }
        }

        self.write_message(msg).await?;

        Ok(())
    }

    fn check_admin(&self, user_login: &str) -> anyhow::Result<()> {
        if self
            .app
            .config
            .admins
            .iter()
            .any(|login| login == user_login)
        {
            Ok(())
        } else {
            Err(anyhow!("User {user_login} is not an admin"))
        }
    }

    async fn write_message(&self, msg: ServerMessage) -> anyhow::Result<()> {
        // Ignore
        if matches!(msg, ServerMessage::RoomState(_)) {
            return Ok(());
        }

        let irc_message = IRCMessage::from(msg);

        if let Some((channel_id, maybe_user_id)) = extract_channel_and_user_from_raw(&irc_message) {
            if !channel_id.is_empty() {
                MESSAGES_RECEIVED_COUNTERS
                    .with_label_values(&[channel_id])
                    .inc();
            }

            let timestamp = extract_raw_timestamp(&irc_message)
                .unwrap_or_else(|| Utc::now().timestamp_millis().try_into().unwrap());
            let user_id = maybe_user_id.unwrap_or_default().to_owned();

            if self.app.optout_users.contains(&user_id) {
                return Ok(());
            }

            let raw_irc = irc_message.as_raw_irc();

            let unstructured = UnstructuredMessage {
                channel_id,
                user_id: &user_id,
                timestamp,
                raw: &raw_irc,
            };
            match StructuredMessage::from_unstructured(&unstructured) {
                Ok(msg) => {
                    let owned_msg = msg.into_owned();
                    self.app.firehose_tx.send(owned_msg.clone()).ok();
                    self.writer_tx.send(owned_msg).await?;
                }
                Err(err) => {
                    error!("Could not convert message {unstructured:?} to be logged: {err}");
                }
            }
        }

        Ok(())
    }

    async fn handle_command<C: LoginCredentials>(
        &self,
        cmd: &str,
        client: &TwitchClient<C>,
        sender_id: &str,
        sender_login: &str,
    ) -> anyhow::Result<()> {
        debug!("Processing command {cmd}");
        let mut split = cmd.split_whitespace();
        if let Some(action) = split.next() {
            let args: Vec<&str> = split.collect();

            match action {
                "join" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Join)
                        .await?
                }
                "leave" | "part" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Part)
                        .await?
                }
                "optout" => {
                    self.optout_user(&args, sender_login, sender_id).await?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    async fn optout_user(
        &self,
        args: &[&str],
        sender_login: &str,
        sender_id: &str,
    ) -> anyhow::Result<()> {
        let arg = args.first().context("No optout code provided")?;
        if self.app.optout_codes.remove(*arg).is_some() {
            self.app.optout_user(sender_id).await?;

            Ok(())
        } else if self.check_admin(sender_login).is_ok() {
            let user_id = self.app.get_user_id_by_name(arg).await?;

            self.app.optout_user(&user_id).await?;

            Ok(())
        } else {
            Err(anyhow!("Invalid optout code"))
        }
    }

    async fn update_channels<C: LoginCredentials>(
        &self,
        client: &TwitchClient<C>,
        channels: &[&str],
        action: ChannelAction,
    ) -> anyhow::Result<()> {
        if channels.is_empty() {
            return Err(anyhow!("no channels specified"));
        }

        let (ids, logins) = self
            .app
            .get_users(
                vec![],
                channels.iter().map(ToString::to_string).collect(),
                false,
            )
            .await?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        self.app.update_channels(&ids, action).await?;

        for login in logins {
            match action {
                ChannelAction::Join => {
                    info!("Joining channel {login}");
                    client.join(login)?;
                }
                ChannelAction::Part => {
                    info!("Parting channel {login}");
                    client.part(login);
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum ChannelAction {
    Join,
    Part,
}
