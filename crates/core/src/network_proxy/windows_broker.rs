use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
use socket2::{Domain, Protocol, Socket, Type};
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::{
    setsockopt, SOCKET_ERROR, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
};

use super::windows_tcp_attribution::{
    attribute_tcp_connection, current_user_sid, AttributedWindowsProcess,
};
use super::{
    handle_client, handle_socks5_client, network_proxy_policy_digest, NetworkProxyConfig,
    NetworkProxyState, NetworkProxyTransport, TrustedWindowsSandboxContext,
};
use crate::runtime_values::RuntimeString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsNetworkBrokerRoute {
    pub session_id: String,
    pub route_sid: String,
    pub route_lease_id: String,
    pub http_addr: SocketAddr,
    pub socks_addr: Option<SocketAddr>,
    pub broker_generation: String,
    pub policy_digest: String,
    pub identity_kind: WindowsBrokerIdentityKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsBrokerIdentityKind {
    Restricted,
    Capability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteIdentity {
    Restricted,
    Capability { owner_user_sid: String },
}

impl RouteIdentity {
    fn kind(&self) -> WindowsBrokerIdentityKind {
        match self {
            Self::Restricted => WindowsBrokerIdentityKind::Restricted,
            Self::Capability { .. } => WindowsBrokerIdentityKind::Capability,
        }
    }

    fn is_present(&self, sid: &str, process: &AttributedWindowsProcess) -> bool {
        match self {
            Self::Restricted => process.restricted_sids.iter().any(|entry| entry == sid),
            Self::Capability { .. } => process.capability_sids.iter().any(|entry| entry == sid),
        }
    }

    fn permits(&self, process: &AttributedWindowsProcess) -> bool {
        match self {
            Self::Capability { owner_user_sid } => {
                process.app_container_sid.is_some() && process.user_sid == *owner_user_sid
            }
            Self::Restricted => true,
        }
    }
}

impl WindowsNetworkBrokerRoute {
    pub fn trusted_context(&self) -> TrustedWindowsSandboxContext {
        TrustedWindowsSandboxContext {
            route_sid: RuntimeString::from_utf8(&self.route_sid),
            http_addr: self.http_addr,
            socks_addr: self.socks_addr,
            broker_generation: RuntimeString::from_utf8(&self.broker_generation),
            policy_digest: RuntimeString::from_utf8(&self.policy_digest),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsNetworkBrokerHealth {
    pub generation: String,
    pub http_addr: SocketAddr,
    pub socks_addr: SocketAddr,
    pub route_count: usize,
    pub http_listener_running: bool,
    pub socks_listener_running: bool,
}

struct BrokerRouteState {
    session_id: String,
    route_sid: String,
    route_lease_id: String,
    policy_digest: String,
    identity: RouteIdentity,
    socks_enabled: bool,
    proxy: Arc<NetworkProxyState>,
    cancel: CancellationToken,
    active_connections: AtomicUsize,
    drained: Notify,
}

#[derive(Default)]
struct BrokerRoutes {
    active: HashMap<String, Arc<BrokerRouteState>>,
    // A timed-out revoke retains its owner until that exact lease is drained.
    retiring: HashMap<String, Arc<BrokerRouteState>>,
    closed: bool,
}

type RouteRegistry = Arc<Mutex<BrokerRoutes>>;

struct PreparedRoute {
    session_id: String,
    config: NetworkProxyConfig,
    policy_digest: String,
    route_sid: String,
    identity: RouteIdentity,
}

impl PreparedRoute {
    fn new(
        session_id: String,
        config: NetworkProxyConfig,
        policy_digest: String,
        route_sid: String,
        identity: RouteIdentity,
    ) -> io::Result<Self> {
        if session_id.trim().is_empty() || policy_digest.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "broker route requires non-empty session id and policy digest",
            ));
        }
        if !config.enabled || config.transport != NetworkProxyTransport::WindowsBroker {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "broker route requires enabled transport=windows-broker",
            ));
        }
        config.validate()?;
        validate_broker_config(&config)?;
        if network_proxy_policy_digest(&config)? != policy_digest {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "broker route policy digest does not match the canonical proxy configuration",
            ));
        }
        Ok(Self {
            session_id,
            config,
            policy_digest,
            route_sid,
            identity,
        })
    }

    fn session_capability(
        session_id: String,
        config: NetworkProxyConfig,
        policy_digest: String,
    ) -> io::Result<Self> {
        let name = format!("klaus-network-route-{}", uuid::Uuid::new_v4());
        let sid = klaus_sandbox_win::capability::derive_route_capability_sid(&name)
            .map_err(io::Error::other)?;
        let owner_user_sid = current_user_sid()?;
        Self::new(
            session_id,
            config,
            policy_digest,
            sid,
            RouteIdentity::Capability { owner_user_sid },
        )
    }

    fn build(
        self,
        broker: &WindowsNetworkBroker,
    ) -> (WindowsNetworkBrokerRoute, Arc<BrokerRouteState>) {
        let Self {
            session_id,
            config,
            policy_digest,
            route_sid,
            identity,
        } = self;
        let route_lease_id = uuid::Uuid::new_v4().to_string();
        let identity_kind = identity.kind();
        let socks_addr = config.enable_socks5.then_some(broker.socks_addr);
        let context = TrustedWindowsSandboxContext {
            route_sid: RuntimeString::from_utf8(&route_sid),
            http_addr: broker.http_addr,
            socks_addr,
            broker_generation: RuntimeString::from_utf8(&broker.generation),
            policy_digest: RuntimeString::from_utf8(&policy_digest),
        };
        let state = Arc::new(NetworkProxyState {
            config: RwLock::new(config),
            http_addr: broker.http_addr,
            socks_addr,
            trusted_windows_context: Some(context),
            blocked: Mutex::new(VecDeque::new()),
            policy_decider: None,
            blocked_observer: None,
            config_reloader: None,
        });
        let state = Arc::new(BrokerRouteState {
            session_id: session_id.clone(),
            route_sid: route_sid.clone(),
            route_lease_id: route_lease_id.clone(),
            policy_digest: policy_digest.clone(),
            identity,
            socks_enabled: socks_addr.is_some(),
            proxy: state,
            cancel: CancellationToken::new(),
            active_connections: AtomicUsize::new(0),
            drained: Notify::new(),
        });
        let route = WindowsNetworkBrokerRoute {
            session_id,
            route_sid,
            route_lease_id,
            http_addr: broker.http_addr,
            socks_addr,
            broker_generation: broker.generation.clone(),
            policy_digest,
            identity_kind,
        };
        (route, state)
    }
}

struct BrokerListeners {
    runtime: tokio::runtime::Handle,
    http: TcpListener,
    socks: TcpListener,
}

pub struct WindowsNetworkBroker {
    generation: String,
    http_addr: SocketAddr,
    socks_addr: SocketAddr,
    routes: RouteRegistry,
    cancel: CancellationToken,
    http_task: Option<JoinHandle<()>>,
    socks_task: Option<JoinHandle<()>>,
    http_failure: Option<String>,
    socks_failure: Option<String>,
}

impl WindowsNetworkBroker {
    pub async fn start() -> io::Result<Self> {
        Self::start_sync()
    }

    pub async fn start_with_preferred_ports(preferred_ports: &[u16]) -> io::Result<Self> {
        Self::start_sync_with_preferred_ports(preferred_ports)
    }

    /// Bind and retain the listeners without blocking an async executor.
    pub fn start_sync() -> io::Result<Self> {
        Self::start_sync_with_preferred_ports(&[])
    }

    fn start_sync_with_preferred_ports(preferred_ports: &[u16]) -> io::Result<Self> {
        let (mut broker, listeners) = Self::bind_sync(preferred_ports)?;
        broker.activate_listeners(listeners);
        Ok(broker)
    }

    fn bind_sync(preferred_ports: &[u16]) -> io::Result<(Self, BrokerListeners)> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("Windows network broker requires a Tokio runtime"))?;
        let (http, socks) = bind_listener_pair(preferred_ports)?;
        let broker = Self {
            generation: uuid::Uuid::new_v4().to_string(),
            http_addr: http.local_addr()?,
            socks_addr: socks.local_addr()?,
            routes: Arc::new(Mutex::new(BrokerRoutes::default())),
            cancel: CancellationToken::new(),
            http_task: None,
            socks_task: None,
            http_failure: None,
            socks_failure: None,
        };
        Ok((
            broker,
            BrokerListeners {
                runtime,
                http,
                socks,
            },
        ))
    }

    fn activate_listeners(&mut self, listeners: BrokerListeners) {
        // The caller has completed every fallible initialization step. Until
        // this point, an error drops the bound sockets synchronously and no
        // accepted connection or blocking attribution task can outlive it.
        self.http_task = Some(listeners.runtime.spawn(run_listener(
            listeners.http,
            Arc::clone(&self.routes),
            BrokerProtocol::Http,
            self.cancel.clone(),
        )));
        self.socks_task = Some(listeners.runtime.spawn(run_listener(
            listeners.socks,
            Arc::clone(&self.routes),
            BrokerProtocol::Socks,
            self.cancel.clone(),
        )));
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn socks_addr(&self) -> SocketAddr {
        self.socks_addr
    }

    pub async fn register_route(
        &self,
        session_id: String,
        config: NetworkProxyConfig,
        policy_digest: String,
    ) -> io::Result<WindowsNetworkBrokerRoute> {
        self.insert_prepared_route(PreparedRoute::new(
            session_id,
            config,
            policy_digest,
            random_restricting_sid(),
            RouteIdentity::Restricted,
        )?)
    }

    /// Register a fresh session capability for the real owner, never a supplied
    /// user identity. This path does not replace or await another route.
    #[cfg(test)]
    pub(crate) fn register_session_capability_route(
        &self,
        session_id: String,
        config: NetworkProxyConfig,
        policy_digest: String,
    ) -> io::Result<WindowsNetworkBrokerRoute> {
        self.insert_prepared_route(PreparedRoute::session_capability(
            session_id,
            config,
            policy_digest,
        )?)
    }

    fn insert_prepared_route(
        &self,
        prepared: PreparedRoute,
    ) -> io::Result<WindowsNetworkBrokerRoute> {
        let mut registry = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.closed
            || self.cancel.is_cancelled()
            || self.http_task.as_ref().is_none_or(JoinHandle::is_finished)
            || self.socks_task.as_ref().is_none_or(JoinHandle::is_finished)
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Windows broker listener is no longer running",
            ));
        }
        // Lookup and insertion are one transaction. An old draining identity
        // cannot be reused while any accepted connection still owns its policy.
        if registry.active.contains_key(&prepared.route_sid)
            || registry
                .retiring
                .values()
                .any(|route| route.route_sid == prepared.route_sid)
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "sandbox identity already owns a broker route",
            ));
        }
        let (route, state) = prepared.build(self);
        registry.active.insert(route.route_sid.clone(), state);
        Ok(route)
    }

    pub async fn unregister_route(
        &self,
        session_id: &str,
        route_sid: &str,
        route_lease_id: &str,
        broker_generation: &str,
    ) -> io::Result<bool> {
        self.unregister_route_with_timeout(
            session_id,
            route_sid,
            route_lease_id,
            broker_generation,
            Duration::from_secs(30),
        )
        .await
    }

    async fn unregister_route_with_timeout(
        &self,
        session_id: &str,
        route_sid: &str,
        route_lease_id: &str,
        broker_generation: &str,
        timeout: Duration,
    ) -> io::Result<bool> {
        if broker_generation != self.generation {
            return Ok(false);
        }
        let route = {
            let mut registry = self
                .routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registry.active.get(route_sid).is_some_and(|route| {
                route.session_id == session_id && route.route_lease_id == route_lease_id
            }) {
                let route = registry
                    .active
                    .remove(route_sid)
                    .expect("checked route identity");
                // Admission uses this same mutex, so every prior admission has
                // incremented its count before this route becomes unavailable.
                route.cancel.cancel();
                registry
                    .retiring
                    .insert(route_lease_id.to_string(), Arc::clone(&route));
                Some(route)
            } else {
                registry
                    .retiring
                    .get(route_lease_id)
                    .filter(|route| route.session_id == session_id && route.route_sid == route_sid)
                    .cloned()
            }
        };
        let Some(route) = route else {
            return Ok(false);
        };
        drain_route(Arc::clone(&route), timeout).await?;
        let mut registry = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .retiring
            .get(route_lease_id)
            .is_some_and(|current| Arc::ptr_eq(current, &route))
        {
            registry.retiring.remove(route_lease_id);
        }
        Ok(true)
    }

    pub fn route_matches(&self, session_id: &str, route_sid: &str, policy_digest: &str) -> bool {
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .get(route_sid)
            .is_some_and(|route| {
                route.session_id == session_id && route.policy_digest == policy_digest
            })
    }

    pub fn health(&self) -> WindowsNetworkBrokerHealth {
        WindowsNetworkBrokerHealth {
            generation: self.generation.clone(),
            http_addr: self.http_addr,
            socks_addr: self.socks_addr,
            route_count: self
                .routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .len(),
            http_listener_running: self
                .http_task
                .as_ref()
                .is_some_and(|task| !task.is_finished()),
            socks_listener_running: self
                .socks_task
                .as_ref()
                .is_some_and(|task| !task.is_finished()),
        }
    }

    fn begin_shutdown(&self) -> Vec<Arc<BrokerRouteState>> {
        self.cancel.cancel();
        let mut registry = self
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.closed = true;
        let active = registry
            .active
            .drain()
            .map(|(_, route)| route)
            .collect::<Vec<_>>();
        for route in active {
            route.cancel.cancel();
            registry
                .retiring
                .insert(route.route_lease_id.clone(), route);
        }
        for route in registry.retiring.values() {
            route.cancel.cancel();
        }
        registry.retiring.values().cloned().collect()
    }

    /// Confirm listener, attribution, and accepted connection completion. A
    /// timeout keeps the tasks and retiring leases available to the next waiter.
    pub async fn shutdown_and_wait(&mut self, timeout: Duration) -> io::Result<()> {
        let routes = self.begin_shutdown();
        tokio::time::timeout(timeout, async {
            let (http, socks) = tokio::join!(
                join_listener(&mut self.http_task, &mut self.http_failure, "HTTP"),
                join_listener(&mut self.socks_task, &mut self.socks_failure, "SOCKS"),
            );
            http?;
            socks?;
            for route in routes {
                wait_for_route_connections_to_drain(route, || {}).await;
            }
            self.routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retiring
                .clear();
            Ok(())
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Windows network broker did not drain before the cleanup deadline",
            )
        })?
    }

    /// Compatibility cancellation only. Session owners use shutdown_and_wait;
    /// this method and Drop cannot certify that blocking attribution has ended.
    pub async fn shutdown(&self) {
        for route in self.begin_shutdown() {
            if drain_route(route, Duration::from_secs(30)).await.is_err() {
                break;
            }
        }
        if let Some(task) = &self.http_task {
            task.abort();
        }
        if let Some(task) = &self.socks_task {
            task.abort();
        }
    }
}

async fn join_listener(
    task: &mut Option<JoinHandle<()>>,
    failure: &mut Option<String>,
    name: &str,
) -> io::Result<()> {
    if let Some(handle) = task.as_mut() {
        let result = handle.await;
        *task = None;
        if let Err(error) = result {
            *failure = Some(format!("{name} broker drain failed: {error}"));
        }
    }
    match failure {
        Some(error) => Err(io::Error::other(error.clone())),
        None => Ok(()),
    }
}

impl Drop for WindowsNetworkBroker {
    fn drop(&mut self) {
        self.begin_shutdown();
        if let Some(task) = &self.http_task {
            task.abort();
        }
        if let Some(task) = &self.socks_task {
            task.abort();
        }
    }
}

/// The Session and its sandbox wrapper share this owner. Broker route policies
/// do not retain it, so their NetworkProxyState cannot form an Arc cycle.
pub(crate) struct SessionWindowsNetwork {
    broker: tokio::sync::Mutex<WindowsNetworkBroker>,
    cancel: CancellationToken,
}

impl SessionWindowsNetwork {
    pub(crate) fn start_proxy_sync(
        session_id: String,
        config: NetworkProxyConfig,
    ) -> io::Result<super::NetworkProxy> {
        let digest = network_proxy_policy_digest(&config)?;
        let prepared = PreparedRoute::session_capability(session_id, config.clone(), digest)?;
        let (mut broker, listeners) = WindowsNetworkBroker::bind_sync(&[])?;
        let (route, state) = prepared.build(&broker);
        let mut proxy =
            super::NetworkProxy::from_trusted_windows_context(config, route.trusted_context())?;
        // The session observes the same policy and blocked-request history
        // that admitted connections actually use. The owner remains outside
        // NetworkProxyState, so sharing this state cannot retain the broker.
        proxy.state = Arc::clone(&state.proxy);
        broker
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .insert(route.route_sid.clone(), state);
        broker.activate_listeners(listeners);
        let owner = Arc::new(Self {
            cancel: broker.cancel.clone(),
            broker: tokio::sync::Mutex::new(broker),
        });
        proxy.session_broker = Some(owner);
        Ok(proxy)
    }

    pub(crate) async fn shutdown_and_wait(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        // Signal immediately, including a deadline that has already expired.
        self.cancel.cancel();
        let mut broker = tokio::time::timeout_at(deadline, self.broker.lock())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Windows broker shutdown owner is busy",
                )
            })?;
        broker
            .shutdown_and_wait(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
    }
}

#[derive(Clone, Copy)]
enum BrokerProtocol {
    Http,
    Socks,
}

async fn run_listener(
    listener: TcpListener,
    routes: RouteRegistry,
    protocol: BrokerProtocol,
    cancel: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else { break; };
                let routes = Arc::clone(&routes);
                let cancel = cancel.clone();
                connections.spawn(async move {
                    let _ = dispatch_connection(socket, routes, protocol, cancel).await;
                });
            }
        }
    }
    // Stop accepting before waiting for attribution and route handlers. In
    // particular, do not abort a task while it owns a spawn_blocking query.
    drop(listener);
    cancel.cancel();
    while connections.join_next().await.is_some() {}
}

async fn dispatch_connection(
    socket: TcpStream,
    routes: RouteRegistry,
    protocol: BrokerProtocol,
    cancel: CancellationToken,
) -> io::Result<()> {
    let local_addr = socket.local_addr()?;
    let peer_addr = socket.peer_addr()?;
    let attributed =
        tokio::task::spawn_blocking(move || attribute_tcp_connection(local_addr, peer_addr))
            .await
            .map_err(|error| {
                io::Error::other(format!("join Windows attribution task: {error}"))
            })??;
    if cancel.is_cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "network broker closed during attribution",
        ));
    }
    let _attributed_process_id = attributed.process_id;
    let connection = admit_connection(&routes, &attributed)?;
    let route = &connection.route;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "network broker was closed")),
        result = async {
            match protocol {
                BrokerProtocol::Http => handle_client(Arc::clone(&route.proxy), socket).await,
                BrokerProtocol::Socks if route.socks_enabled => {
                    handle_socks5_client(Arc::clone(&route.proxy), socket).await
                }
                BrokerProtocol::Socks => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "network proxy route does not enable SOCKS5",
                )),
            }
        } => result,
        _ = route.cancel.cancelled() => Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "network proxy route was revoked",
        )),
    }
}

struct ActiveConnection {
    route: Arc<BrokerRouteState>,
}

impl ActiveConnection {
    fn new(route: Arc<BrokerRouteState>) -> Self {
        route.active_connections.fetch_add(1, Ordering::AcqRel);
        Self { route }
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        if self.route.active_connections.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.route.drained.notify_waiters();
        }
    }
}

async fn drain_route(route: Arc<BrokerRouteState>, timeout: Duration) -> io::Result<()> {
    route.cancel.cancel();
    tokio::time::timeout(timeout, wait_for_route_connections_to_drain(route, || {}))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Windows broker route did not drain before the cleanup deadline",
            )
        })
}

async fn wait_for_route_connections_to_drain(
    route: Arc<BrokerRouteState>,
    mut after_wait_registration: impl FnMut(),
) {
    loop {
        let notified = route.drained.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        after_wait_registration();
        if route.active_connections.load(Ordering::Acquire) == 0 {
            break;
        }
        notified.await;
    }
}

fn registered_route_for_identity(
    routes: &HashMap<String, Arc<BrokerRouteState>>,
    process: &AttributedWindowsProcess,
) -> io::Result<Arc<BrokerRouteState>> {
    let mut matching = routes
        .iter()
        .filter(|(sid, route)| {
            !route.cancel.is_cancelled() && route.identity.is_present(sid, process)
        })
        .map(|(_, route)| Arc::clone(route));
    let route = matching.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "proxy client token has no registered network proxy route identity",
        )
    })?;
    if matching.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "proxy client token has multiple registered network proxy route identities",
        ));
    }
    if !route.identity.permits(process) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "proxy capability requires its owner in an AppContainer",
        ));
    }
    Ok(route)
}

fn admit_connection(
    routes: &RouteRegistry,
    process: &AttributedWindowsProcess,
) -> io::Result<ActiveConnection> {
    let registry = routes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if registry.closed {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "Windows broker admission is closed",
        ));
    }
    let route = registered_route_for_identity(&registry.active, process)?;
    // Revoke takes this same mutex before it removes the route or samples the
    // counter. The returned guard covers every access to the selected policy.
    Ok(ActiveConnection::new(route))
}

fn validate_broker_config(config: &NetworkProxyConfig) -> io::Result<()> {
    if config.proxy_url.is_some()
        || config.socks_url.is_some()
        || config.dangerously_allow_non_loopback_proxy
        || config.allow_upstream_proxy
        || config.upstream_proxy.is_some()
        || config.upstream_http_proxy.is_some()
        || config.upstream_https_proxy.is_some()
        || config.upstream_all_proxy.is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows broker owns listener addresses and does not allow upstream proxy settings",
        ));
    }
    if config
        .managed_ca_bundle_path
        .as_ref()
        .is_some_and(|path| path.is_empty())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed CA bundle path cannot be empty",
        ));
    }
    Ok(())
}

fn random_restricting_sid() -> String {
    let a = rand::random::<u32>();
    let b = rand::random::<u32>();
    let c = rand::random::<u32>();
    let d = rand::random::<u32>();
    format!("S-1-5-21-{a}-{b}-{c}-{d}")
}

fn bind_listener_pair(preferred_ports: &[u16]) -> io::Result<(TcpListener, TcpListener)> {
    let preferred = if preferred_ports.len() == 2
        && preferred_ports[0] != 0
        && preferred_ports[1] != 0
        && preferred_ports[0] != preferred_ports[1]
    {
        Some((preferred_ports[0], preferred_ports[1]))
    } else {
        Some((3128, 8081))
    };
    if let Some((http_port, socks_port)) = preferred {
        if let Ok(pair) = bind_exact_listener_pair(http_port, socks_port) {
            return Ok(pair);
        }
    }
    for _ in 0..32 {
        let http = bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let socks = match bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0))) {
            Ok(listener) => listener,
            Err(_) => continue,
        };
        if http.local_addr()?.port() == socks.local_addr()?.port() {
            continue;
        }
        http.set_nonblocking(true)?;
        socks.set_nonblocking(true)?;
        return Ok((TcpListener::from_std(http)?, TcpListener::from_std(socks)?));
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        "failed to reserve an HTTP/SOCKS loopback listener pair after 32 attempts",
    ))
}

fn bind_exact_listener_pair(
    http_port: u16,
    socks_port: u16,
) -> io::Result<(TcpListener, TcpListener)> {
    let http = bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], http_port)))?;
    let socks = bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], socks_port)))?;
    http.set_nonblocking(true)?;
    socks.set_nonblocking(true)?;
    Ok((TcpListener::from_std(http)?, TcpListener::from_std(socks)?))
}

fn bind_exclusive_listener(address: SocketAddr) -> io::Result<std::net::TcpListener> {
    #[cfg(windows)]
    {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        let enabled = 1i32;
        let result = unsafe {
            setsockopt(
                socket.as_raw_socket() as usize,
                SOL_SOCKET,
                SO_EXCLUSIVEADDRUSE,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled) as i32,
            )
        };
        if result == SOCKET_ERROR {
            return Err(io::Error::last_os_error());
        }
        socket.bind(&address.into())?;
        socket.listen(128)?;
        Ok(socket.into())
    }
    #[cfg(not(windows))]
    {
        std::net::TcpListener::bind(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(session_id: &str, digest: &str) -> Arc<BrokerRouteState> {
        let config = NetworkProxyConfig {
            enabled: true,
            transport: NetworkProxyTransport::WindowsBroker,
            ..Default::default()
        };
        Arc::new(BrokerRouteState {
            session_id: session_id.to_string(),
            route_sid: "sid-fixture".into(),
            identity: RouteIdentity::Restricted,
            route_lease_id: uuid::Uuid::new_v4().to_string(),
            policy_digest: digest.to_string(),
            socks_enabled: false,
            proxy: Arc::new(NetworkProxyState {
                config: RwLock::new(config),
                http_addr: SocketAddr::from(([127, 0, 0, 1], 3128)),
                socks_addr: None,
                trusted_windows_context: None,
                blocked: Mutex::new(VecDeque::new()),
                policy_decider: None,
                blocked_observer: None,
                config_reloader: None,
            }),
            cancel: CancellationToken::new(),
            active_connections: AtomicUsize::new(0),
            drained: Notify::new(),
        })
    }

    fn process(
        restricted: &[&str],
        capabilities: &[&str],
        user: &str,
        package: Option<&str>,
    ) -> AttributedWindowsProcess {
        let restricted_sids = restricted
            .iter()
            .map(|sid| (*sid).to_string())
            .collect::<Vec<_>>();
        let app_container_sid = package.map(str::to_string);
        AttributedWindowsProcess {
            process_id: 1,
            restricted_sids,
            capability_sids: capabilities.iter().map(|sid| (*sid).to_string()).collect(),
            user_sid: user.into(),
            app_container_sid,
        }
    }

    #[test]
    fn route_selection_fails_closed_for_missing_or_ambiguous_sid() {
        let mut routes = HashMap::new();
        routes.insert("sid-a".to_string(), route("a", "digest-a"));
        routes.insert("sid-b".to_string(), route("b", "digest-b"));
        assert_eq!(
            registered_route_for_identity(&routes, &process(&[], &[], "user", None))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            registered_route_for_identity(
                &routes,
                &process(&["sid-a", "sid-b"], &[], "user", None)
            )
            .err()
            .unwrap()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            registered_route_for_identity(&routes, &process(&["sid-a"], &[], "user", None))
                .unwrap()
                .session_id,
            "a"
        );
    }

    #[test]
    fn capability_routes_require_the_owner_and_an_app_container_and_exactly_one_match() {
        let mut capability = route("capability-owner", "digest");
        Arc::get_mut(&mut capability).unwrap().identity = RouteIdentity::Capability {
            owner_user_sid: "owner".into(),
        };
        let restricted = route("restricted-owner", "digest");
        let mut routes = HashMap::from([
            ("capability".into(), capability),
            ("restricted".into(), restricted),
        ]);
        for denied in [
            process(&[], &["capability"], "owner", None),
            process(&[], &["capability"], "other-user", Some("execution")),
            process(&["capability"], &[], "owner", Some("execution")),
            process(&[], &[], "owner", Some("capability")),
            process(&["restricted"], &["capability"], "owner", Some("execution")),
        ] {
            assert_eq!(
                registered_route_for_identity(&routes, &denied)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
        }
        assert_eq!(
            registered_route_for_identity(
                &routes,
                &process(&[], &["capability"], "owner", Some("execution"))
            )
            .unwrap()
            .session_id,
            "capability-owner"
        );
        assert_eq!(
            registered_route_for_identity(
                &routes,
                &process(&["restricted"], &[], "owner", Some("execution"))
            )
            .unwrap()
            .session_id,
            "restricted-owner"
        );
        let second = Arc::clone(routes.get("capability").unwrap());
        routes.insert("second-capability".into(), second);
        assert_eq!(
            registered_route_for_identity(
                &routes,
                &process(
                    &[],
                    &["capability", "second-capability"],
                    "owner",
                    Some("execution")
                )
            )
            .err()
            .unwrap()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn broker_config_rejects_host_controlled_listener_and_upstream_fields() {
        let mut config = NetworkProxyConfig {
            enabled: true,
            transport: NetworkProxyTransport::WindowsBroker,
            ..Default::default()
        };
        assert!(validate_broker_config(&config).is_ok());
        config.proxy_url = Some(RuntimeString::from_utf8("http://127.0.0.1:9000"));
        assert_eq!(
            validate_broker_config(&config).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    fn broker_config() -> NetworkProxyConfig {
        NetworkProxyConfig {
            enabled: true,
            transport: NetworkProxyTransport::WindowsBroker,
            enable_socks5: true,
            allow_local_binding: Some(false),
            allow_upstream_proxy: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn session_capability_registration_is_fresh_and_bound_to_the_current_owner() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut broker = WindowsNetworkBroker::start_sync().unwrap();
        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let first = broker
            .register_session_capability_route(
                "same-session".into(),
                config.clone(),
                digest.clone(),
            )
            .unwrap();
        let second = broker
            .register_session_capability_route("same-session".into(), config, digest)
            .unwrap();
        let owner = current_user_sid().unwrap();
        let (bound, count) = {
            let registry = broker.routes.lock().unwrap();
            let bound = match &registry.active.get(&first.route_sid).unwrap().identity {
                RouteIdentity::Capability { owner_user_sid } => owner_user_sid == &owner,
                _ => false,
            };
            (bound, registry.active.len())
        };
        // Exercise the actual TCP attribution path with an ordinary host client.
        // A capability name in the registry must not grant that client a route.
        let mut client = TcpStream::connect(broker.http_addr()).await.unwrap();
        client.write_all(b"GET http://klaus.invalid/.well-known/klaus-sandbox-health HTTP/1.1\r\nHost: klaus.invalid\r\n\r\n").await.unwrap();
        let mut response = Vec::new();
        let read =
            tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut response)).await;
        drop(client);
        broker
            .shutdown_and_wait(Duration::from_secs(3))
            .await
            .unwrap();
        assert!(read.is_ok(), "ordinary host connection must be closed");
        assert!(
            response.is_empty(),
            "ordinary host must not reach the capability route"
        );
        assert!(bound);
        assert_eq!(first.identity_kind, WindowsBrokerIdentityKind::Capability);
        assert!(first.route_sid.starts_with("S-1-15-3-1024-"));
        assert_ne!(first.route_sid, second.route_sid);
        assert_ne!(first.route_lease_id, second.route_lease_id);
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn revoke_timeout_retains_the_exact_lease_and_rejects_new_admission() {
        let mut broker = WindowsNetworkBroker::start_sync().unwrap();
        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let registered = broker
            .register_route("session".into(), config, digest)
            .await
            .unwrap();
        let identity = process(&[&registered.route_sid], &[], "owner", None);
        let admitted = admit_connection(&broker.routes, &identity).unwrap();
        let result = broker
            .unregister_route_with_timeout(
                &registered.session_id,
                &registered.route_sid,
                &registered.route_lease_id,
                &registered.broker_generation,
                Duration::ZERO,
            )
            .await;
        let rejected = admit_connection(&broker.routes, &identity)
            .err()
            .map(|error| error.kind());
        let retained = broker
            .routes
            .lock()
            .unwrap()
            .retiring
            .contains_key(&registered.route_lease_id);
        drop(admitted);
        let retried = broker
            .unregister_route(
                &registered.session_id,
                &registered.route_sid,
                &registered.route_lease_id,
                &registered.broker_generation,
            )
            .await;
        broker
            .shutdown_and_wait(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(rejected, Some(io::ErrorKind::PermissionDenied));
        assert!(retained, "timeout must retain the owner needed by a retry");
        assert!(retried.unwrap());
    }

    #[tokio::test]
    async fn session_proxy_observes_requests_blocked_by_its_actual_route() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut config = broker_config();
        config.blocked_domains = Some(vec!["blocked.test".into()]);
        let proxy = super::super::NetworkProxy::start_session_owned(
            &RuntimeString::from("blocked-observation"),
            config,
        )
        .unwrap();
        let owner = proxy.session_broker_owner().unwrap();
        let route_state = {
            let broker = owner.broker.lock().await;
            let registry = broker.routes.lock().unwrap();
            Arc::clone(&registry.active.values().next().unwrap().proxy)
        };
        // Exercise the HTTP policy handler after admission. TCP attribution is
        // covered separately; this socket carries no request to an external host.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let exchange = async {
            let client = async {
                let mut stream = TcpStream::connect(address).await?;
                stream
                    .write_all(
                        b"GET http://blocked.test/path HTTP/1.1\r\nHost: blocked.test\r\n\r\n",
                    )
                    .await?;
                let mut response = String::new();
                stream.read_to_string(&mut response).await?;
                Ok::<_, io::Error>(response)
            };
            let server = async {
                let (stream, _) = listener.accept().await?;
                handle_client(route_state, stream).await
            };
            tokio::join!(client, server)
        };
        let exchange = tokio::time::timeout(Duration::from_secs(3), exchange).await;
        drop(listener);
        let blocked = proxy.blocked_snapshot().await;
        let cleanup = owner
            .shutdown_and_wait(tokio::time::Instant::now() + Duration::from_secs(3))
            .await;
        cleanup.expect("broker cleanup before assertions");
        let (response, handled) = exchange.expect("owned HTTP exchange completed");
        handled.expect("HTTP policy handler completed");
        assert!(response.unwrap().starts_with("HTTP/1.1 403"));
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].host, "blocked.test");
        assert_eq!(blocked[0].reason, "domain_denied");
    }

    #[tokio::test]
    async fn session_broker_shutdown_is_explicit_idempotent_and_has_no_owner_cycle() {
        let proxy = super::super::NetworkProxy::start_session_owned(
            &RuntimeString::from("owned-session"),
            broker_config(),
        )
        .unwrap();
        let owner = proxy.session_broker_owner().unwrap();
        let context = proxy.state.trusted_windows_context.clone().unwrap();
        let http = context.http_addr;
        let socks = context.socks_addr.unwrap();
        let weak = Arc::downgrade(&owner);
        let borrowed =
            super::super::NetworkProxy::from_trusted_windows_context(broker_config(), context)
                .unwrap();
        assert!(
            borrowed.session_broker_owner().is_none(),
            "external trusted contexts remain borrowed"
        );
        drop(borrowed);
        let cloned = proxy.clone();
        drop(proxy);
        let first = owner
            .shutdown_and_wait(tokio::time::Instant::now() + Duration::from_secs(3))
            .await;
        let second = owner
            .shutdown_and_wait(tokio::time::Instant::now() + Duration::from_secs(3))
            .await;
        let ports_free =
            std::net::TcpListener::bind(http).is_ok() && std::net::TcpListener::bind(socks).is_ok();
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "sandbox wrapper retains the shared owner"
        );
        drop(cloned);
        assert!(
            weak.upgrade().is_none(),
            "route policies must not retain their broker"
        );
        first.unwrap();
        second.unwrap();
        assert!(ports_free);
    }

    #[tokio::test]
    async fn route_leases_are_unique_and_require_exact_unregister_identity() {
        let broker = WindowsNetworkBroker::start().await.unwrap();
        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let first = broker
            .register_route("session-a".to_string(), config.clone(), digest.clone())
            .await
            .unwrap();
        let second = broker
            .register_route("session-b".to_string(), config, digest.clone())
            .await
            .unwrap();

        assert_ne!(first.route_sid, second.route_sid);
        assert_ne!(first.route_lease_id, second.route_lease_id);
        assert_eq!(broker.health().route_count, 2);
        assert!(broker.route_matches("session-a", &first.route_sid, &digest));
        assert!(!broker
            .unregister_route(
                "session-a",
                &first.route_sid,
                "stale-lease",
                &first.broker_generation,
            )
            .await
            .unwrap());
        assert_eq!(broker.health().route_count, 2);
        assert!(broker
            .unregister_route(
                "session-a",
                &first.route_sid,
                &first.route_lease_id,
                &first.broker_generation,
            )
            .await
            .unwrap());
        assert_eq!(broker.health().route_count, 1);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn independent_routes_for_the_same_session_keep_their_exact_leases() {
        let broker = WindowsNetworkBroker::start().await.unwrap();
        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let old = broker
            .register_route("session-a".to_string(), config.clone(), digest.clone())
            .await
            .unwrap();
        let replacement = broker
            .register_route("session-a".to_string(), config, digest.clone())
            .await
            .unwrap();

        assert_ne!(old.route_sid, replacement.route_sid);
        assert_eq!(broker.health().route_count, 2);
        assert!(broker.route_matches("session-a", &old.route_sid, &digest));
        assert!(broker.route_matches("session-a", &replacement.route_sid, &digest));
        assert!(broker
            .unregister_route(
                "session-a",
                &old.route_sid,
                &old.route_lease_id,
                &old.broker_generation,
            )
            .await
            .unwrap());
        assert!(!broker.route_matches("session-a", &old.route_sid, &digest));
        assert!(broker.route_matches("session-a", &replacement.route_sid, &digest));
        assert_eq!(broker.health().route_count, 1);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn registration_rejects_a_noncanonical_policy_digest() {
        let broker = WindowsNetworkBroker::start().await.unwrap();
        let error = broker
            .register_route(
                "session-a".to_string(),
                broker_config(),
                "forged".to_string(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(broker.health().route_count, 0);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn revoking_a_route_cancels_and_waits_for_active_connections() {
        let route = route("session-a", "digest-a");
        let active = ActiveConnection::new(Arc::clone(&route));
        let draining = tokio::spawn(drain_route(Arc::clone(&route), Duration::from_secs(1)));

        tokio::task::yield_now().await;
        assert!(route.cancel.is_cancelled());
        assert!(!draining.is_finished());

        drop(active);
        tokio::time::timeout(Duration::from_secs(1), draining)
            .await
            .expect("route drain timed out")
            .expect("route drain task failed")
            .expect("route drain failed");
        assert_eq!(route.active_connections.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn route_drain_does_not_lose_the_last_connection_notification() {
        let route = route("session-a", "digest-a");
        let active = ActiveConnection::new(Arc::clone(&route));
        let mut active = Some(active);

        tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_route_connections_to_drain(Arc::clone(&route), || {
                drop(active.take());
            }),
        )
        .await
        .expect("route drain lost the last-connection notification");
        assert_eq!(route.active_connections.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn unactivated_broker_releases_both_ports_before_the_runtime_yields() {
        let (broker, listeners) = WindowsNetworkBroker::bind_sync(&[]).unwrap();
        let http = broker.http_addr();
        let socks = broker.socks_addr();
        assert!(!broker.health().http_listener_running);
        assert!(!broker.health().socks_listener_running);
        drop(listeners);
        drop(broker);
        // A fallible constructor can return synchronously: it must not depend
        // on the runtime polling an aborted listener before its ports are free.
        let http = std::net::TcpListener::bind(http).expect("HTTP socket released synchronously");
        let socks =
            std::net::TcpListener::bind(socks).expect("SOCKS socket released synchronously");
        drop((http, socks));
    }

    #[tokio::test]
    async fn diagnostic_broker_shutdown_waits_for_connections_and_releases_both_ports() {
        let mut broker = WindowsNetworkBroker::start().await.unwrap();
        let http = broker.http_addr();
        let socks = broker.socks_addr();
        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let registered = broker
            .register_route("private-diagnostic".into(), config, digest)
            .await
            .unwrap();
        let route = broker
            .routes
            .lock()
            .unwrap()
            .active
            .get(&registered.route_sid)
            .unwrap()
            .clone();
        let active = ActiveConnection::new(Arc::clone(&route));
        let closing = tokio::spawn(async move {
            broker
                .shutdown_and_wait(Duration::from_secs(2))
                .await
                .unwrap();
            assert_eq!(broker.health().route_count, 0);
            assert!(!broker.health().http_listener_running);
            assert!(!broker.health().socks_listener_running);
        });
        route.cancel.cancelled().await;
        assert!(
            !closing.is_finished(),
            "shutdown must retain the connection owner"
        );
        drop(active);
        closing.await.unwrap();
        assert!(std::net::TcpListener::bind(http).is_ok());
        assert!(std::net::TcpListener::bind(socks).is_ok());
    }

    #[tokio::test]
    async fn diagnostic_broker_shutdown_reports_an_unconfirmed_connection_drain() {
        let mut broker = WindowsNetworkBroker::start().await.unwrap();
        let route = route("private-diagnostic", "digest");
        let active = ActiveConnection::new(Arc::clone(&route));
        broker
            .routes
            .lock()
            .unwrap()
            .active
            .insert("sid-fixture".into(), route);
        let error = broker
            .shutdown_and_wait(Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(broker.routes.lock().unwrap().retiring.len(), 1);
        drop(active);
        broker
            .shutdown_and_wait(Duration::from_secs(2))
            .await
            .unwrap();
        assert!(broker.routes.lock().unwrap().retiring.is_empty());
    }

    #[tokio::test]
    async fn listener_failure_prevents_new_route_registration() {
        let broker = WindowsNetworkBroker::start().await.unwrap();
        broker.http_task.as_ref().unwrap().abort();
        tokio::task::yield_now().await;

        let config = broker_config();
        let digest = network_proxy_policy_digest(&config).unwrap();
        let error = broker
            .register_route("session-a".to_string(), config, digest)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(broker.health().route_count, 0);
        broker.shutdown().await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn broker_listener_ports_cannot_be_rebound_by_another_process_socket() {
        let broker = WindowsNetworkBroker::start().await.unwrap();
        assert!(std::net::TcpListener::bind(broker.http_addr()).is_err());
        assert!(std::net::TcpListener::bind(broker.socks_addr()).is_err());
        broker.shutdown().await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn occupied_preferred_port_falls_back_as_an_atomic_pair() {
        let occupied =
            bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0))).expect("occupied port");
        let occupied_port = occupied.local_addr().unwrap().port();
        let available =
            bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0))).expect("available port");
        let available_port = available.local_addr().unwrap().port();
        drop(available);

        let (http, socks) =
            bind_listener_pair(&[occupied_port, available_port]).expect("fallback pair");
        let selected = [
            http.local_addr().unwrap().port(),
            socks.local_addr().unwrap().port(),
        ];
        assert!(!selected.contains(&occupied_port));
        assert_ne!(selected[0], selected[1]);
        assert!(std::net::TcpListener::bind(occupied.local_addr().unwrap()).is_err());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn released_listener_pair_can_be_reused_exactly() {
        // The preferred pair is two ports this case just proved free, not the built-in
        // defaults. Asking for the defaults measured two things at once — that 3128/8081 are
        // available on this host and that a released pair can be retaken — and it failed on
        // the first while claiming to test the second.
        let http_probe = bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("an ephemeral http port");
        let socks_probe = bind_exclusive_listener(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("an ephemeral socks port");
        let preferred = [
            http_probe.local_addr().unwrap().port(),
            socks_probe.local_addr().unwrap().port(),
        ];
        drop(http_probe);
        drop(socks_probe);

        let (first_http, first_socks) =
            bind_listener_pair(&preferred).expect("first listener pair");
        assert_eq!(first_http.local_addr().unwrap().port(), preferred[0]);
        assert_eq!(first_socks.local_addr().unwrap().port(), preferred[1]);
        drop(first_http);
        drop(first_socks);

        let (second_http, second_socks) =
            bind_listener_pair(&preferred).expect("reused listener pair");
        assert_eq!(second_http.local_addr().unwrap().port(), preferred[0]);
        assert_eq!(second_socks.local_addr().unwrap().port(), preferred[1]);
    }
}
