//! Managed network proxy runtime primitives.
//!
//! The command runtime consumes the small, portable sandbox context here. The
//! local proxy implementation lives in the same module so tests can exercise the
//! whole env + sandbox + policy loop without depending on host UI.

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::tools::builtins::host_policy::{
    evaluate_host_policy, HostPolicyDecision, NetworkDomainPolicy,
};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use url::Url;

#[cfg(windows)]
mod windows_broker;
#[cfg(windows)]
mod windows_tcp_attribution;
#[cfg(windows)]
pub(crate) use windows_broker::SessionWindowsNetwork;
#[cfg(windows)]
pub use windows_broker::{
    WindowsBrokerIdentityKind, WindowsNetworkBroker, WindowsNetworkBrokerHealth,
    WindowsNetworkBrokerRoute,
};

/// Serde is retained only for the engine-owned configuration file boundary.
/// Every projection is strict Unicode-scalar UTF-8; runtime/wire paths use the
/// `RuntimeEncode`/`RuntimeDecode` implementations below instead.
mod scalar_runtime_serde {
    use crate::runtime_values::RuntimeString;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    fn scalar<E: serde::ser::Error>(value: &RuntimeString) -> Result<String, E> {
        value.try_to_utf8().map_err(E::custom)
    }

    pub mod option {
        use super::*;

        pub fn serialize<S>(value: &Option<RuntimeString>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .as_ref()
                .map(scalar::<S::Error>)
                .transpose()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<RuntimeString>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<String>::deserialize(deserializer).map(|value| value.map(RuntimeString::from))
        }
    }

    pub mod vec {
        use super::*;

        pub fn serialize<S>(value: &[RuntimeString], serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .iter()
                .map(scalar::<S::Error>)
                .collect::<Result<Vec<_>, _>>()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<RuntimeString>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Vec::<String>::deserialize(deserializer)
                .map(|values| values.into_iter().map(RuntimeString::from).collect())
        }
    }

    pub mod option_vec {
        use super::*;

        pub fn serialize<S>(
            value: &Option<Vec<RuntimeString>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .as_ref()
                .map(|values| {
                    values
                        .iter()
                        .map(scalar::<S::Error>)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<RuntimeString>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<Vec<String>>::deserialize(deserializer).map(|values| {
                values.map(|values| values.into_iter().map(RuntimeString::from).collect())
            })
        }
    }
}

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

mod upstream;

use upstream::{
    connect_target, connect_upstream_proxy, discover_upstream_proxy_from_env,
    write_upstream_proxy_authorization,
};

const PROXY_URL_ENV_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "WS_PROXY",
    "WSS_PROXY",
    "ws_proxy",
    "wss_proxy",
    "YARN_HTTP_PROXY",
    "YARN_HTTPS_PROXY",
    "NPM_CONFIG_HTTP_PROXY",
    "NPM_CONFIG_HTTPS_PROXY",
    "NPM_CONFIG_PROXY",
    "npm_config_http_proxy",
    "npm_config_https_proxy",
    "npm_config_proxy",
    "BUNDLE_HTTP_PROXY",
    "BUNDLE_HTTPS_PROXY",
    "PIP_PROXY",
    "DOCKER_HTTP_PROXY",
    "DOCKER_HTTPS_PROXY",
];
const ALL_PROXY_ENV_KEYS: &[&str] = &["ALL_PROXY", "all_proxy", "FTP_PROXY", "ftp_proxy"];
const NO_PROXY_ENV_KEYS: &[&str] = &[
    "NO_PROXY",
    "no_proxy",
    "NPM_CONFIG_NOPROXY",
    "npm_config_noproxy",
    "YARN_NO_PROXY",
    "BUNDLE_NO_PROXY",
];
const PROXY_ACTIVE_ENV_KEY: &str = "KLAUS_NETWORK_PROXY_ACTIVE";
const ALLOW_LOCAL_BINDING_ENV_KEY: &str = "KLAUS_NETWORK_ALLOW_LOCAL_BINDING";
const ELECTRON_GET_USE_PROXY_ENV_KEY: &str = "ELECTRON_GET_USE_PROXY";
const NODE_USE_ENV_PROXY_ENV_KEY: &str = "NODE_USE_ENV_PROXY";
#[cfg(any(target_os = "macos", test))]
const GIT_SSH_COMMAND_ENV_KEY: &str = "GIT_SSH_COMMAND";
#[cfg(any(target_os = "macos", test))]
const KLAUS_PROXY_GIT_SSH_COMMAND_MARKER: &str = "KLAUS_PROXY_GIT_SSH_COMMAND=1 ";

const DEFAULT_NO_PROXY_VALUE: &str = concat!(
    "localhost,127.0.0.1,::1,",
    "10.0.0.0/8,",
    "172.16.0.0/12,",
    "192.168.0.0/16"
);
const MAX_HEADER_BYTES: usize = 64 * 1024;

fn node_utf8_projection(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is always valid UTF-8")
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    values
        .iter()
        .cloned()
        .reduce(|joined, value| joined.concat(&separator).concat(&value))
        .unwrap_or_default()
}

fn sandbox_kind_enforces_managed_network(kind: SandboxKind) -> bool {
    matches!(
        kind,
        SandboxKind::Seatbelt
            | SandboxKind::Bubblewrap
            | SandboxKind::NativeHelper
            | SandboxKind::WindowsRestrictedToken
            | SandboxKind::WindowsMxc
    )
}

fn sandbox_kind_is_native_windows(kind: SandboxKind) -> bool {
    kind == SandboxKind::WindowsRestrictedToken
}

pub(crate) fn windows_sandbox_supports_network_transport(transport: NetworkProxyTransport) -> bool {
    transport == NetworkProxyTransport::WindowsBroker
}

pub(crate) fn windows_sandbox_supports_network_transport_for_kind(
    kind: SandboxKind,
    transport: NetworkProxyTransport,
) -> bool {
    match kind {
        SandboxKind::WindowsMxc => transport == NetworkProxyTransport::Local,
        SandboxKind::WindowsRestrictedToken => {
            windows_sandbox_supports_network_transport(transport)
        }
        _ => false,
    }
}

/// Portable managed-network facts needed by a sandbox backend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedNetworkSandboxContext {
    /// Loopback proxy ports that sandboxed commands may connect to.
    pub loopback_ports: Vec<u16>,
    /// Whether the command may bind local sockets and exchange loopback traffic.
    pub allow_local_binding: bool,
    /// Unix socket paths the sandboxed command may use for local IPC.
    pub allow_unix_sockets: Vec<RuntimeString>,
    /// Whether all Unix sockets are allowed.
    pub allow_all_unix_sockets: bool,
    /// Managed CA bundle exposed when HTTPS interception/upstream trust is active.
    pub managed_ca_bundle_path: Option<RuntimeString>,
    /// Windows route SID: a Session capability or an external restricting SID.
    pub network_proxy_restricting_sid: Option<RuntimeString>,
    /// Windows broker generation that owns the route and listener pair.
    pub windows_broker_generation: Option<RuntimeString>,
}

/// Trusted Windows broker route data.
///
/// This type deliberately has no serde or runtime codec implementation. It is
/// constructed by the engine's Session broker or the stdio transport's private
/// initialization path. Renderer input cannot supply this context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedWindowsSandboxContext {
    pub route_sid: RuntimeString,
    pub http_addr: SocketAddr,
    pub socks_addr: Option<SocketAddr>,
    pub broker_generation: RuntimeString,
    pub policy_digest: RuntimeString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkProxyTransport {
    #[default]
    Local,
    WindowsBroker,
}

/// Opt-in managed-network proxy settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkProxyConfig {
    pub enabled: bool,
    pub transport: NetworkProxyTransport,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub proxy_url: Option<RuntimeString>,
    pub enable_socks5: bool,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub socks_url: Option<RuntimeString>,
    #[serde(with = "scalar_runtime_serde::option_vec")]
    pub allowed_domains: Option<Vec<RuntimeString>>,
    #[serde(with = "scalar_runtime_serde::option_vec")]
    pub blocked_domains: Option<Vec<RuntimeString>>,
    /// Omission is resolved for the selected sandbox; explicit false remains a denial.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_local_binding: Option<bool>,
    pub dangerously_allow_non_loopback_proxy: bool,
    pub allow_upstream_proxy: bool,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub upstream_proxy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub upstream_http_proxy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub upstream_https_proxy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub upstream_all_proxy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option_vec"
    )]
    pub allow_unix_sockets: Option<Vec<RuntimeString>>,
    pub allow_all_unix_sockets: bool,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_serde::option"
    )]
    pub managed_ca_bundle_path: Option<RuntimeString>,
    pub credential_broker: CredentialBrokerConfig,
    pub mode: NetworkMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CredentialBrokerConfig {
    pub enabled: bool,
    #[serde(with = "scalar_runtime_serde::vec")]
    pub brokered_env_keys: Vec<RuntimeString>,
}

#[cfg(any(windows, test))]
impl NetworkProxyConfig {
    /// Captures fixed listener roles for native service provisioning without
    /// starting listeners. Ephemeral ports use the shared setup path instead.
    pub fn windows_sandbox_proxy_configuration(
        &self,
    ) -> io::Result<(
        klaus_sandbox_win::service::ProvisioningSettings,
        klaus_sandbox_win::service::ProxyListeners,
    )> {
        use klaus_sandbox_win::service::{ProvisioningSettings, ProxyListeners};
        if !self.enabled {
            return Ok((ProvisioningSettings::default(), ProxyListeners::default()));
        }
        let http = resolve_proxy_bind_addr(self.proxy_url.as_ref(), 3128, self)?;
        if http.port() == 0 {
            return Err(io::Error::other(
                "network.proxy_url must use a fixed non-zero port for managed proxy provisioning",
            ));
        }
        let mut ports = vec![http.port()];
        if self.enable_socks5 {
            let socks = resolve_proxy_bind_addr(self.socks_url.as_ref(), 8081, self)?;
            if socks.port() == 0 {
                return Err(io::Error::other(
                    "network.socks_url must use a fixed non-zero port for managed proxy provisioning",
                ));
            }
            ports.push(socks.port());
        }
        ports.sort_unstable();
        ports.dedup();
        let socks_port = self.enable_socks5.then(|| {
            ports
                .iter()
                .copied()
                .find(|port| *port != http.port())
                .unwrap_or(http.port())
        });
        Ok((
            ProvisioningSettings {
                proxy_ports: ports,
                allow_local_binding: self.allow_local_binding(),
            },
            ProxyListeners {
                http_ports: vec![http.port()],
                socks_ports: socks_port.into_iter().collect(),
            },
        ))
    }
}

impl Default for NetworkProxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            transport: NetworkProxyTransport::Local,
            proxy_url: None,
            enable_socks5: true,
            socks_url: None,
            allowed_domains: None,
            blocked_domains: None,
            allow_local_binding: None,
            dangerously_allow_non_loopback_proxy: false,
            allow_upstream_proxy: false,
            upstream_proxy: None,
            upstream_http_proxy: None,
            upstream_https_proxy: None,
            upstream_all_proxy: None,
            allow_unix_sockets: None,
            allow_all_unix_sockets: false,
            managed_ca_bundle_path: None,
            credential_broker: CredentialBrokerConfig::default(),
            mode: NetworkMode::default(),
        }
    }
}

impl RuntimeEncode for CredentialBrokerConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("enabled", &self.enabled)?;
        object.field("brokeredEnvKeys", &self.brokered_env_keys)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CredentialBrokerConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                enabled: object.defaulted("enabled", &[], || false)?,
                brokered_env_keys: object.defaulted("brokeredEnvKeys", &[], Vec::new)?,
            })
        })
    }
}

impl RuntimeEncode for NetworkProxyConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("enabled", &self.enabled)?;
        object.field("transport", &self.transport)?;
        if let Some(value) = &self.proxy_url {
            object.field("proxyUrl", value)?;
        }
        object.field("enableSocks5", &self.enable_socks5)?;
        if let Some(value) = &self.socks_url {
            object.field("socksUrl", value)?;
        }
        object.field("allowedDomains", &self.allowed_domains)?;
        object.field("blockedDomains", &self.blocked_domains)?;
        if let Some(value) = self.allow_local_binding {
            object.field("allowLocalBinding", &value)?;
        }
        object.field(
            "dangerouslyAllowNonLoopbackProxy",
            &self.dangerously_allow_non_loopback_proxy,
        )?;
        object.field("allowUpstreamProxy", &self.allow_upstream_proxy)?;
        if let Some(value) = &self.upstream_proxy {
            object.field("upstreamProxy", value)?;
        }
        if let Some(value) = &self.upstream_http_proxy {
            object.field("upstreamHttpProxy", value)?;
        }
        if let Some(value) = &self.upstream_https_proxy {
            object.field("upstreamHttpsProxy", value)?;
        }
        if let Some(value) = &self.upstream_all_proxy {
            object.field("upstreamAllProxy", value)?;
        }
        if let Some(value) = &self.allow_unix_sockets {
            object.field("allowUnixSockets", value)?;
        }
        object.field("allowAllUnixSockets", &self.allow_all_unix_sockets)?;
        if let Some(value) = &self.managed_ca_bundle_path {
            object.field("managedCaBundlePath", value)?;
        }
        object.field("credentialBroker", &self.credential_broker)?;
        object.field("mode", &self.mode)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for NetworkProxyConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let defaults = Self::default();
            Ok(Self {
                enabled: object.defaulted("enabled", &[], || defaults.enabled)?,
                transport: object.defaulted("transport", &[], || defaults.transport)?,
                proxy_url: object.optional("proxyUrl", &[])?,
                enable_socks5: object.defaulted("enableSocks5", &[], || defaults.enable_socks5)?,
                socks_url: object.optional("socksUrl", &[])?,
                allowed_domains: object.optional("allowedDomains", &[])?,
                blocked_domains: object.optional("blockedDomains", &[])?,
                allow_local_binding: object.optional("allowLocalBinding", &[])?,
                dangerously_allow_non_loopback_proxy: object.defaulted(
                    "dangerouslyAllowNonLoopbackProxy",
                    &[],
                    || defaults.dangerously_allow_non_loopback_proxy,
                )?,
                allow_upstream_proxy: object
                    .defaulted("allowUpstreamProxy", &[], || defaults.allow_upstream_proxy)?,
                upstream_proxy: object.optional("upstreamProxy", &[])?,
                upstream_http_proxy: object.optional("upstreamHttpProxy", &[])?,
                upstream_https_proxy: object.optional("upstreamHttpsProxy", &[])?,
                upstream_all_proxy: object.optional("upstreamAllProxy", &[])?,
                allow_unix_sockets: object.optional("allowUnixSockets", &[])?,
                allow_all_unix_sockets: object.defaulted("allowAllUnixSockets", &[], || {
                    defaults.allow_all_unix_sockets
                })?,
                managed_ca_bundle_path: object.optional("managedCaBundlePath", &[])?,
                credential_broker: object
                    .defaulted("credentialBroker", &[], || defaults.credential_broker)?,
                mode: object.defaulted("mode", &[], || defaults.mode)?,
            })
        })
    }
}

impl NetworkProxyConfig {
    /// Ordinary proxy execution defaults to client-only loopback access.
    pub fn allow_local_binding(&self) -> bool {
        self.allow_local_binding.unwrap_or(false)
    }

    /// Resolve omission for the executor without weakening an explicit denial.
    pub fn resolve_for_sandbox(&self, kind: SandboxKind) -> io::Result<Self> {
        let allow_local_binding = self
            .allow_local_binding
            .unwrap_or(self.enabled && kind == SandboxKind::WindowsMxc);
        if self.enabled && kind == SandboxKind::WindowsMxc && !allow_local_binding {
            return Err(io::Error::new(io::ErrorKind::Unsupported,
                "MXC cannot enforce allow_local_binding=false: native host-loopback access is bidirectional; use true or a different sandbox backend"));
        }
        Ok(Self {
            allow_local_binding: Some(allow_local_binding),
            ..self.clone()
        })
    }

    fn domain_policy(&self) -> NetworkDomainPolicy {
        NetworkDomainPolicy {
            allowed_domains: self.allowed_domains.clone(),
            blocked_domains: self.blocked_domains.clone(),
        }
    }

    fn with_env_upstream_proxy(mut self) -> Self {
        if self.allow_upstream_proxy {
            if self.upstream_http_proxy.is_none() {
                self.upstream_http_proxy =
                    discover_upstream_proxy_from_env(&["HTTP_PROXY", "http_proxy"])
                        .map(RuntimeString::from);
            }
            if self.upstream_https_proxy.is_none() {
                self.upstream_https_proxy =
                    discover_upstream_proxy_from_env(&["HTTPS_PROXY", "https_proxy"])
                        .map(RuntimeString::from);
            }
            if self.upstream_all_proxy.is_none() {
                self.upstream_all_proxy =
                    discover_upstream_proxy_from_env(&["ALL_PROXY", "all_proxy"])
                        .map(RuntimeString::from);
            }
        }
        self
    }

    fn upstream_proxy_url_for(&self, is_secure: bool) -> Option<&RuntimeString> {
        if !self.allow_upstream_proxy {
            return None;
        }
        self.upstream_proxy.as_ref().or_else(|| {
            if is_secure {
                self.upstream_https_proxy
                    .as_ref()
                    .or(self.upstream_http_proxy.as_ref())
                    .or(self.upstream_all_proxy.as_ref())
            } else {
                self.upstream_http_proxy
                    .as_ref()
                    .or(self.upstream_all_proxy.as_ref())
            }
        })
    }

    fn unix_socket_paths(&self) -> &[RuntimeString] {
        self.allow_unix_sockets.as_deref().unwrap_or(&[])
    }

    fn unix_socket_proxying_enabled(&self) -> bool {
        self.allow_all_unix_sockets || !self.unix_socket_paths().is_empty()
    }

    fn validate(&self) -> io::Result<()> {
        if self.transport == NetworkProxyTransport::WindowsBroker && self.allow_upstream_proxy {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker networking does not allow an upstream proxy",
            ));
        }
        for (index, socket_path) in self.unix_socket_paths().iter().enumerate() {
            if !is_absolute_unix_path(socket_path) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("network allow_unix_sockets[{index}] must be an absolute path"),
                ));
            }
        }
        Ok(())
    }
}

impl RuntimeEncode for NetworkProxyTransport {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Local => "local",
            Self::WindowsBroker => "windows-broker",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for NetworkProxyTransport {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("local", &[]),
                RuntimeEnumVariant::new("windows-broker", &[]),
            ],
            &[Self::Local, Self::WindowsBroker],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    Limited,
    #[default]
    Full,
}

impl RuntimeEncode for NetworkMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Limited => "limited",
            Self::Full => "full",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for NetworkMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("limited", &[]),
                RuntimeEnumVariant::new("full", &[]),
            ],
            &[Self::Limited, Self::Full],
        )
    }
}

impl NetworkMode {
    fn allows_http_method(self, method: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Limited => matches!(
                method.to_ascii_uppercase().as_str(),
                "GET" | "HEAD" | "OPTIONS"
            ),
        }
    }
}

fn resolve_proxy_bind_addr(
    configured_url: Option<&RuntimeString>,
    default_port: u16,
    config: &NetworkProxyConfig,
) -> io::Result<SocketAddr> {
    let requested = match configured_url {
        Some(value) => parse_proxy_bind_addr(value, default_port)?,
        None => SocketAddr::from(([127, 0, 0, 1], 0)),
    };
    Ok(clamp_proxy_bind_addr(requested, config))
}

fn parse_proxy_bind_addr(value: &RuntimeString, default_port: u16) -> io::Result<SocketAddr> {
    let value = node_utf8_projection(value);
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "network proxy bind address is empty",
        ));
    }

    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, default_port));
    }

    let candidate = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    if let Ok(url) = Url::parse(&candidate) {
        if let Some(host) = url.host_str() {
            let host = host.trim_matches(|c| c == '[' || c == ']');
            let port = url.port().unwrap_or(default_port);
            let ip = if host.eq_ignore_ascii_case("localhost") {
                IpAddr::from([127, 0, 0, 1])
            } else {
                host.parse::<IpAddr>()
                    .unwrap_or_else(|_| IpAddr::from([127, 0, 0, 1]))
            };
            return Ok(SocketAddr::new(ip, port));
        }
    }

    let (host, port) = trimmed.rsplit_once(':').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid network proxy bind address: {trimmed}"),
        )
    })?;
    let port = port.parse::<u16>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid network proxy port {port:?}: {err}"),
        )
    })?;
    let ip = if host.eq_ignore_ascii_case("localhost") {
        IpAddr::from([127, 0, 0, 1])
    } else {
        host.parse::<IpAddr>()
            .unwrap_or_else(|_| IpAddr::from([127, 0, 0, 1]))
    };
    Ok(SocketAddr::new(ip, port))
}

fn clamp_proxy_bind_addr(addr: SocketAddr, config: &NetworkProxyConfig) -> SocketAddr {
    if addr.ip().is_loopback() {
        return addr;
    }
    if config.unix_socket_proxying_enabled() || !config.dangerously_allow_non_loopback_proxy {
        return SocketAddr::from(([127, 0, 0, 1], addr.port()));
    }
    addr
}

fn validate_trusted_windows_context(
    context: &TrustedWindowsSandboxContext,
    socks_enabled: bool,
) -> io::Result<()> {
    validate_trusted_windows_addr("trusted Windows broker HTTP address", context.http_addr)?;
    match (socks_enabled, context.socks_addr) {
        (true, Some(addr)) => {
            validate_trusted_windows_addr("trusted Windows broker SOCKS address", addr)?
        }
        (true, None) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trusted Windows broker context is missing the SOCKS address",
            ))
        }
        (false, Some(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trusted Windows broker context supplied SOCKS while SOCKS is disabled",
            ))
        }
        (false, None) => {}
    }
    if context.route_sid.is_empty()
        || context.broker_generation.is_empty()
        || context.policy_digest.is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "trusted Windows broker context contains an empty identity field",
        ));
    }
    Ok(())
}

fn validate_trusted_windows_addr(name: &str, addr: SocketAddr) -> io::Result<()> {
    if addr.ip() != IpAddr::from([127, 0, 0, 1]) || addr.port() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a non-zero 127.0.0.1 address"),
        ));
    }
    Ok(())
}

fn sanitize_managed_proxy_env(env: &mut RuntimeEnv) {
    let keys = env
        .entries()
        .map(|(key, _)| key)
        .filter(|key| {
            key.try_to_utf8()
                .ok()
                .is_some_and(|key| is_reserved_managed_proxy_env_key(&key))
        })
        .collect::<Vec<_>>();
    for key in keys {
        env.remove(&key);
    }
}

fn is_reserved_managed_proxy_env_key(key: &str) -> bool {
    let is_listed_proxy_key = PROXY_URL_ENV_KEYS
        .iter()
        .chain(ALL_PROXY_ENV_KEYS)
        .chain(NO_PROXY_ENV_KEYS)
        .any(|candidate| candidate.eq_ignore_ascii_case(key));
    if is_listed_proxy_key {
        return true;
    }
    let upper = key.to_ascii_uppercase();
    upper == NODE_USE_ENV_PROXY_ENV_KEY
        || upper == ELECTRON_GET_USE_PROXY_ENV_KEY
        || upper == PROXY_ACTIVE_ENV_KEY
        || upper == ALLOW_LOCAL_BINDING_ENV_KEY
        || upper == "KLAUS_CREDENTIAL_BROKER_ACTIVE"
        || upper == "KLAUS_BROKERED_CREDENTIAL_KEYS"
        || (upper.starts_with("KLAUS_") && upper.contains("SANDBOX"))
}

fn is_absolute_unix_path(path: &RuntimeString) -> bool {
    path.starts_with("/")
}

#[cfg(any(target_os = "macos", test))]
fn git_ssh_proxy_command(socks_addr: SocketAddr) -> String {
    format!(
        "{KLAUS_PROXY_GIT_SSH_COMMAND_MARKER}ssh -o ProxyCommand='nc -X 5 -x {socks_addr} %h %p'"
    )
}

/// Environment and sandbox context prepared for one child process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedManagedNetwork {
    pub env: RuntimeEnv,
    pub sandbox_context: ManagedNetworkSandboxContext,
}

/// One denied proxy request retained for audit/debug surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedRequest {
    pub host: RuntimeString,
    pub port: u16,
    pub protocol: RuntimeString,
    pub reason: RuntimeString,
    pub source: RuntimeString,
    pub timestamp_ms: i64,
}

impl RuntimeEncode for BlockedRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("host", &self.host)?;
        object.field("port", &self.port)?;
        object.field("protocol", &self.protocol)?;
        object.field("reason", &self.reason)?;
        object.field("source", &self.source)?;
        object.field("timestampMs", &self.timestamp_ms)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BlockedRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                host: object.required("host", &[])?,
                port: object.required("port", &[])?,
                protocol: object.required("protocol", &[])?,
                reason: object.required("reason", &[])?,
                source: object.required("source", &[])?,
                timestamp_ms: object.required("timestampMs", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NetworkProtocol {
    Http,
    HttpsConnect,
    Socks5Tcp,
    UnixSocket,
}

impl RuntimeEncode for NetworkProtocol {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Http => "http",
            Self::HttpsConnect => "httpsConnect",
            Self::Socks5Tcp => "socks5Tcp",
            Self::UnixSocket => "unixSocket",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for NetworkProtocol {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("http", &[]),
                RuntimeEnumVariant::new("httpsConnect", &[]),
                RuntimeEnumVariant::new("socks5Tcp", &[]),
                RuntimeEnumVariant::new("unixSocket", &[]),
            ],
            &[
                Self::Http,
                Self::HttpsConnect,
                Self::Socks5Tcp,
                Self::UnixSocket,
            ],
        )
    }
}

impl NetworkProtocol {
    fn from_label(label: &str) -> Self {
        match label {
            "https_connect" => Self::HttpsConnect,
            "socks5_tcp" => Self::Socks5Tcp,
            "unix_socket" => Self::UnixSocket,
            _ => Self::Http,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicyRequest {
    pub host: String,
    pub port: u16,
    pub protocol: NetworkProtocol,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicyDecision {
    pub decision: NetworkDecision,
    pub source: String,
    pub reason: Option<String>,
    pub request: NetworkPolicyRequest,
}

impl NetworkPolicyDecision {
    pub fn allow(source: impl Into<String>, request: NetworkPolicyRequest) -> Self {
        Self {
            decision: NetworkDecision::Allow,
            source: source.into(),
            reason: None,
            request,
        }
    }

    pub fn deny(
        source: impl Into<String>,
        reason: impl Into<String>,
        request: NetworkPolicyRequest,
    ) -> Self {
        Self {
            decision: NetworkDecision::Deny,
            source: source.into(),
            reason: Some(reason.into()),
            request,
        }
    }
}

#[async_trait]
pub trait NetworkPolicyDecider: Send + Sync + 'static {
    async fn decide(&self, request: NetworkPolicyRequest) -> NetworkPolicyDecision;
}

#[async_trait]
pub trait BlockedRequestObserver: Send + Sync + 'static {
    async fn on_blocked_request(&self, request: BlockedRequest);
}

#[async_trait]
pub trait ConfigReloader: Send + Sync + 'static {
    async fn maybe_reload(&self) -> Option<NetworkProxyConfig>;
}

#[derive(Clone)]
pub struct NetworkProxy {
    state: Arc<NetworkProxyState>,
    // The broker owns route NetworkProxyState values, never these wrappers.
    // Keeping ownership here avoids a broker -> route -> broker reference cycle.
    #[cfg(windows)]
    session_broker: Option<Arc<SessionWindowsNetwork>>,
}

struct NetworkProxyState {
    config: RwLock<NetworkProxyConfig>,
    http_addr: SocketAddr,
    socks_addr: Option<SocketAddr>,
    trusted_windows_context: Option<TrustedWindowsSandboxContext>,
    blocked: Mutex<VecDeque<BlockedRequest>>,
    policy_decider: Option<Arc<dyn NetworkPolicyDecider>>,
    blocked_observer: Option<Arc<dyn BlockedRequestObserver>>,
    config_reloader: Option<Arc<dyn ConfigReloader>>,
}

struct BoundProxyListeners {
    http_listener: TcpListener,
    http_addr: SocketAddr,
    socks_listener: Option<TcpListener>,
    socks_addr: Option<SocketAddr>,
}

struct NetworkProxyRuntimeHooks {
    policy_decider: Option<Arc<dyn NetworkPolicyDecider>>,
    blocked_observer: Option<Arc<dyn BlockedRequestObserver>>,
    config_reloader: Option<Arc<dyn ConfigReloader>>,
}

pub struct NetworkProxyBuilder {
    config: NetworkProxyConfig,
    policy_decider: Option<Arc<dyn NetworkPolicyDecider>>,
    blocked_observer: Option<Arc<dyn BlockedRequestObserver>>,
    config_reloader: Option<Arc<dyn ConfigReloader>>,
}

impl NetworkProxyBuilder {
    fn new(config: NetworkProxyConfig) -> Self {
        Self {
            config,
            policy_decider: None,
            blocked_observer: None,
            config_reloader: None,
        }
    }

    pub fn policy_decider_arc(mut self, decider: Arc<dyn NetworkPolicyDecider>) -> Self {
        self.policy_decider = Some(decider);
        self
    }

    pub fn blocked_request_observer_arc(
        mut self,
        observer: Arc<dyn BlockedRequestObserver>,
    ) -> Self {
        self.blocked_observer = Some(observer);
        self
    }

    pub fn config_reloader_arc(mut self, reloader: Arc<dyn ConfigReloader>) -> Self {
        self.config_reloader = Some(reloader);
        self
    }

    pub async fn build(self) -> io::Result<NetworkProxy> {
        let config = self.config.with_env_upstream_proxy();
        config.validate()?;
        if config.transport == NetworkProxyTransport::WindowsBroker {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker networking requires a trusted Host route context",
            ));
        }
        let http_bind_addr = resolve_proxy_bind_addr(config.proxy_url.as_ref(), 3128, &config)?;
        let socks_bind_addr = resolve_proxy_bind_addr(config.socks_url.as_ref(), 8081, &config)?;
        let http_listener = TcpListener::bind(http_bind_addr).await?;
        let http_addr = http_listener.local_addr()?;
        let (socks_listener, socks_addr) = if config.enable_socks5 {
            let listener = TcpListener::bind(socks_bind_addr).await?;
            let addr = listener.local_addr()?;
            (Some(listener), Some(addr))
        } else {
            (None, None)
        };
        NetworkProxy::from_listeners(
            config,
            BoundProxyListeners {
                http_listener,
                http_addr,
                socks_listener,
                socks_addr,
            },
            NetworkProxyRuntimeHooks {
                policy_decider: self.policy_decider,
                blocked_observer: self.blocked_observer,
                config_reloader: self.config_reloader,
            },
        )
    }
}

/// Sandbox wrapper that applies managed-network env and loopback-only policy at
/// the final spawn boundary.
pub struct ManagedNetworkSandboxBackend {
    inner: Arc<dyn SandboxBackend>,
    proxy: NetworkProxy,
}

/// Keeps the Host-owned Windows broker listener pair committed to the
/// Firewall/WFP setup marker while networking is disabled. No route is
/// registered and no proxy variables are injected.
pub struct WindowsNetworkGuardSandboxBackend {
    inner: Arc<dyn SandboxBackend>,
    ports: Vec<u16>,
}

impl WindowsNetworkGuardSandboxBackend {
    pub fn try_new(
        inner: Arc<dyn SandboxBackend>,
        context: TrustedWindowsSandboxContext,
    ) -> io::Result<Self> {
        if !cfg!(windows) || !sandbox_kind_is_native_windows(inner.kind()) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Windows broker guard requires the native Windows sandbox",
            ));
        }
        if !context.route_sid.is_empty() || !context.policy_digest.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker guard must not contain a route SID or policy digest",
            ));
        }
        if context.broker_generation.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker guard is missing its generation",
            ));
        }
        validate_trusted_windows_addr("trusted Windows guard HTTP address", context.http_addr)?;
        let socks = context.socks_addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker guard is missing the SOCKS address",
            )
        })?;
        validate_trusted_windows_addr("trusted Windows guard SOCKS address", socks)?;
        if context.http_addr.port() == socks.port() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker guard listener ports must be distinct",
            ));
        }
        Ok(Self {
            inner,
            ports: vec![context.http_addr.port(), socks.port()],
        })
    }
}

impl SandboxBackend for WindowsNetworkGuardSandboxBackend {
    fn enforces_planning_read_only(&self) -> bool {
        self.inner.enforces_planning_read_only()
    }

    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }

    fn is_available(&self) -> bool {
        self.inner.is_available()
    }

    fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let mut guarded_policy = policy.clone();
        guarded_policy.network_allowed = false;
        guarded_policy.windows_network_proxy_restricting_sid = None;
        guarded_policy.windows_network_allow_local_binding = None;
        guarded_policy.windows_network_proxy_guard_ports = Some(self.ports.clone());
        if policy.planning_read_only || self.inner.enforces_planning_read_only() {
            guarded_policy.enforce_planning_read_only();
        } else if policy.runtime_network_ceiling == Some(false) {
            guarded_policy.enforce_no_network();
        }
        self.inner.transform(req, &guarded_policy)
    }
}

impl ManagedNetworkSandboxBackend {
    pub fn new(inner: Arc<dyn SandboxBackend>, proxy: NetworkProxy) -> Self {
        Self { inner, proxy }
    }

    pub fn try_new(inner: Arc<dyn SandboxBackend>, proxy: NetworkProxy) -> io::Result<Self> {
        Self::validate_kind(inner.kind())?;
        if inner.kind() == SandboxKind::WindowsMxc {
            let config = proxy.state.config_snapshot();
            if config.transport != NetworkProxyTransport::Local {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "native MXC managed networking requires local dedicated proxy listeners",
                ));
            }
            let ports = proxy.loopback_ports();
            if ports.is_empty() || ports.contains(&0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "MXC managed networking requires nonzero dedicated proxy ports",
                ));
            }
            if !config.allow_local_binding() {
                return Err(io::Error::new(io::ErrorKind::Unsupported,
                    "MXC cannot enforce managed networking with allow_local_binding=false: native host-loopback access is bidirectional"));
            }
        }
        Ok(Self { inner, proxy })
    }

    pub(crate) fn validate_kind(kind: SandboxKind) -> io::Result<()> {
        if !sandbox_kind_enforces_managed_network(kind) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("{:?} sandbox does not enforce managed networking", kind),
            ));
        }
        Ok(())
    }
}

impl SandboxBackend for ManagedNetworkSandboxBackend {
    fn enforces_planning_read_only(&self) -> bool {
        self.inner.enforces_planning_read_only()
    }

    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }

    fn is_available(&self) -> bool {
        self.inner.is_available()
    }

    fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        if policy.planning_read_only || self.inner.enforces_planning_read_only() {
            let mut restricted = policy.clone();
            restricted.enforce_planning_read_only();
            return self.inner.transform(req, &restricted);
        }
        if policy.runtime_network_ceiling == Some(false) {
            let mut restricted = policy.clone();
            restricted.enforce_no_network();
            return self.inner.transform(req, &restricted);
        }
        let prepared = self.proxy.prepare_env(req.env.clone().unwrap_or_default());
        let mut managed_req = req.clone();
        managed_req.env = Some(prepared.env);
        let mut managed_policy = policy.clone();
        managed_policy.network_allowed = false;
        managed_policy.allow_loopback_ports = Some(prepared.sandbox_context.loopback_ports);
        managed_policy.allow_unix_sockets = Some(prepared.sandbox_context.allow_unix_sockets);
        managed_policy.allow_all_unix_sockets =
            Some(prepared.sandbox_context.allow_all_unix_sockets);
        managed_policy.windows_network_proxy_restricting_sid =
            if self.inner.kind() == SandboxKind::WindowsMxc {
                None
            } else {
                prepared.sandbox_context.network_proxy_restricting_sid
            };
        managed_policy.windows_network_allow_local_binding =
            Some(prepared.sandbox_context.allow_local_binding);
        managed_policy.windows_network_proxy_guard_ports = None;
        if let Some(ca) = prepared.sandbox_context.managed_ca_bundle_path {
            if let Some(readable) = managed_policy.readable_roots.as_mut() {
                readable.push(ca);
            }
        }
        self.inner.transform(&managed_req, &managed_policy)
    }
}

impl NetworkProxy {
    #[cfg(windows)]
    pub(crate) fn start_session_owned(
        session_id: &RuntimeString,
        config: NetworkProxyConfig,
    ) -> io::Result<Self> {
        let session_id = session_id.try_to_utf8().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid session identity: {error}"),
            )
        })?;
        SessionWindowsNetwork::start_proxy_sync(session_id.to_string(), config)
    }

    #[cfg(windows)]
    pub(crate) fn session_broker_owner(&self) -> Option<Arc<SessionWindowsNetwork>> {
        self.session_broker.clone()
    }

    pub fn builder(config: NetworkProxyConfig) -> NetworkProxyBuilder {
        NetworkProxyBuilder::new(config)
    }

    pub fn start_sync(config: NetworkProxyConfig) -> io::Result<Self> {
        let config = config.with_env_upstream_proxy();
        config.validate()?;
        if config.transport == NetworkProxyTransport::WindowsBroker {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows broker networking requires a trusted Host route context",
            ));
        }
        let http_bind_addr = resolve_proxy_bind_addr(config.proxy_url.as_ref(), 3128, &config)?;
        let socks_bind_addr = resolve_proxy_bind_addr(config.socks_url.as_ref(), 8081, &config)?;
        let std_http_listener = std::net::TcpListener::bind(http_bind_addr)?;
        std_http_listener.set_nonblocking(true)?;
        let http_addr = std_http_listener.local_addr()?;
        let http_listener = TcpListener::from_std(std_http_listener)?;
        let (socks_listener, socks_addr) = if config.enable_socks5 {
            let listener = std::net::TcpListener::bind(socks_bind_addr)?;
            listener.set_nonblocking(true)?;
            let addr = listener.local_addr()?;
            (Some(TcpListener::from_std(listener)?), Some(addr))
        } else {
            (None, None)
        };
        Self::from_listeners(
            config,
            BoundProxyListeners {
                http_listener,
                http_addr,
                socks_listener,
                socks_addr,
            },
            NetworkProxyRuntimeHooks {
                policy_decider: None,
                blocked_observer: None,
                config_reloader: None,
            },
        )
    }

    pub async fn start(config: NetworkProxyConfig) -> io::Result<Self> {
        Self::builder(config).build().await
    }

    pub fn from_trusted_windows_context(
        config: NetworkProxyConfig,
        context: TrustedWindowsSandboxContext,
    ) -> io::Result<Self> {
        let config = config.with_env_upstream_proxy();
        config.validate()?;
        if config.transport != NetworkProxyTransport::WindowsBroker {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "trusted Windows broker context requires transport=windows-broker",
            ));
        }
        validate_trusted_windows_context(&context, config.enable_socks5)?;
        let expected_digest = network_proxy_policy_digest(&config)?;
        let supplied_digest = context.policy_digest.try_to_utf8().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("trusted Windows broker policy digest is invalid: {error}"),
            )
        })?;
        if supplied_digest != expected_digest {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "trusted Windows broker policy digest does not match the resolved proxy policy",
            ));
        }
        Ok(Self {
            state: Arc::new(NetworkProxyState {
                config: RwLock::new(config),
                http_addr: context.http_addr,
                socks_addr: context.socks_addr,
                trusted_windows_context: Some(context),
                blocked: Mutex::new(VecDeque::new()),
                policy_decider: None,
                blocked_observer: None,
                config_reloader: None,
            }),
            #[cfg(windows)]
            session_broker: None,
        })
    }

    fn from_listeners(
        config: NetworkProxyConfig,
        listeners: BoundProxyListeners,
        hooks: NetworkProxyRuntimeHooks,
    ) -> io::Result<Self> {
        let BoundProxyListeners {
            http_listener,
            http_addr,
            socks_listener,
            socks_addr,
        } = listeners;
        let NetworkProxyRuntimeHooks {
            policy_decider,
            blocked_observer,
            config_reloader,
        } = hooks;
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("managed network proxy requires a Tokio runtime"))?;
        let state = Arc::new(NetworkProxyState {
            config: RwLock::new(config),
            http_addr,
            socks_addr,
            trusted_windows_context: None,
            blocked: Mutex::new(VecDeque::new()),
            policy_decider,
            blocked_observer,
            config_reloader,
        });
        let accept_state = Arc::clone(&state);
        handle.spawn(async move {
            loop {
                let Ok((socket, _client_addr)) = http_listener.accept().await else {
                    break;
                };
                let connection_state = Arc::clone(&accept_state);
                tokio::spawn(async move {
                    let _ = handle_client(connection_state, socket).await;
                });
            }
        });
        if let Some(socks_listener) = socks_listener {
            let accept_state = Arc::clone(&state);
            handle.spawn(async move {
                loop {
                    let Ok((socket, _client_addr)) = socks_listener.accept().await else {
                        break;
                    };
                    let connection_state = Arc::clone(&accept_state);
                    tokio::spawn(async move {
                        let _ = handle_socks5_client(connection_state, socket).await;
                    });
                }
            });
        }
        Ok(Self {
            state,
            #[cfg(windows)]
            session_broker: None,
        })
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.state.http_addr
    }

    pub fn socks_addr(&self) -> Option<SocketAddr> {
        self.state.socks_addr
    }

    pub fn network_proxy_restricting_sid(&self) -> Option<&RuntimeString> {
        self.state
            .trusted_windows_context
            .as_ref()
            .map(|context| &context.route_sid)
    }

    pub fn prepare_env(&self, mut env: RuntimeEnv) -> PreparedManagedNetwork {
        let config = self.state.config_snapshot();
        sanitize_managed_proxy_env(&mut env);
        let proxy_url = format!("http://{}", self.http_addr());
        let all_proxy_url = self
            .socks_addr()
            .map(|addr| format!("socks5h://{addr}"))
            .unwrap_or_else(|| proxy_url.clone());
        for key in PROXY_URL_ENV_KEYS {
            env.insert((*key).into(), proxy_url.clone().into());
        }
        for key in ALL_PROXY_ENV_KEYS {
            env.insert((*key).into(), all_proxy_url.clone().into());
        }
        if config.allow_local_binding() {
            for key in NO_PROXY_ENV_KEYS {
                env.insert((*key).into(), DEFAULT_NO_PROXY_VALUE.into());
            }
        }
        env.insert(PROXY_ACTIVE_ENV_KEY.into(), "1".into());
        env.insert(
            ALLOW_LOCAL_BINDING_ENV_KEY.into(),
            if config.allow_local_binding() {
                "1".into()
            } else {
                "0".into()
            },
        );
        env.insert(NODE_USE_ENV_PROXY_ENV_KEY.into(), "1".into());
        env.insert(ELECTRON_GET_USE_PROXY_ENV_KEY.into(), "1".into());
        if let Some(ca_bundle) = config.managed_ca_bundle_path.as_ref() {
            for key in [
                "SSL_CERT_FILE",
                "NODE_EXTRA_CA_CERTS",
                "REQUESTS_CA_BUNDLE",
                "CURL_CA_BUNDLE",
                "GIT_SSL_CAINFO",
                "BUNDLE_SSL_CA_CERT",
                "AWS_CA_BUNDLE",
            ] {
                env.insert(key.into(), ca_bundle.clone());
            }
        }
        #[cfg(any(target_os = "macos", test))]
        if let Some(socks_addr) = self.socks_addr() {
            let should_set_git_ssh = env
                .get(&RuntimeString::from(GIT_SSH_COMMAND_ENV_KEY))
                .map(|value| value.starts_with(KLAUS_PROXY_GIT_SSH_COMMAND_MARKER))
                .unwrap_or(true);
            if should_set_git_ssh {
                env.insert(
                    GIT_SSH_COMMAND_ENV_KEY.into(),
                    git_ssh_proxy_command(socks_addr).into(),
                );
            }
        }
        if config.credential_broker.enabled {
            env.insert("KLAUS_CREDENTIAL_BROKER_ACTIVE".into(), "1".into());
            env.insert(
                "KLAUS_BROKERED_CREDENTIAL_KEYS".into(),
                join_runtime_strings(&config.credential_broker.brokered_env_keys, ","),
            );
        }

        PreparedManagedNetwork {
            env,
            sandbox_context: ManagedNetworkSandboxContext {
                loopback_ports: self.loopback_ports(),
                allow_local_binding: config.allow_local_binding(),
                allow_unix_sockets: config.allow_unix_sockets.unwrap_or_default(),
                allow_all_unix_sockets: config.allow_all_unix_sockets,
                managed_ca_bundle_path: config.managed_ca_bundle_path,
                network_proxy_restricting_sid: self
                    .state
                    .trusted_windows_context
                    .as_ref()
                    .map(|context| context.route_sid.clone()),
                windows_broker_generation: self
                    .state
                    .trusted_windows_context
                    .as_ref()
                    .map(|context| context.broker_generation.clone()),
            },
        }
    }

    fn loopback_ports(&self) -> Vec<u16> {
        let mut ports = vec![self.http_addr().port()];
        if let Some(socks_addr) = self.socks_addr() {
            ports.push(socks_addr.port());
        }
        ports.sort_unstable();
        ports.dedup();
        ports
    }

    pub async fn blocked_snapshot(&self) -> Vec<BlockedRequest> {
        self.state
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }
}

pub fn network_proxy_policy_digest(config: &NetworkProxyConfig) -> io::Result<String> {
    let value = serde_json::to_value(config).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serialize managed network policy: {error}"),
        )
    })?;
    let canonical = canonical_json_value(&value);
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn canonical_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => {
            serde_json::to_string(value).expect("serialize JSON string")
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json_value)
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("serialize JSON object key"),
                        canonical_json_value(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

async fn handle_socks5_client(
    state: Arc<NetworkProxyState>,
    mut client: TcpStream,
) -> io::Result<()> {
    let mut header = [0u8; 2];
    client.read_exact(&mut header).await?;
    if header[0] != 0x05 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported SOCKS version",
        ));
    }
    let mut methods = vec![0u8; header[1] as usize];
    client.read_exact(&mut methods).await?;
    client.write_all(&[0x05, 0x00]).await?;

    let mut req = [0u8; 4];
    client.read_exact(&mut req).await?;
    if req[0] != 0x05 || req[1] != 0x01 {
        return write_socks5_failure(&mut client, 0x07).await;
    }
    let host = match req[3] {
        0x01 => {
            let mut ip = [0u8; 4];
            client.read_exact(&mut ip).await?;
            IpAddr::from(ip).to_string()
        }
        0x03 => {
            let mut len = [0u8; 1];
            client.read_exact(&mut len).await?;
            let mut host = vec![0u8; len[0] as usize];
            client.read_exact(&mut host).await?;
            String::from_utf8_lossy(&host).into_owned()
        }
        0x04 => {
            let mut ip = [0u8; 16];
            client.read_exact(&mut ip).await?;
            IpAddr::from(ip).to_string()
        }
        _ => return write_socks5_failure(&mut client, 0x08).await,
    };
    let mut port_bytes = [0u8; 2];
    client.read_exact(&mut port_bytes).await?;
    let port = u16::from_be_bytes(port_bytes);

    state.maybe_reload_config().await;
    if state.config_snapshot().mode == NetworkMode::Limited {
        state
            .record_blocked(blocked(
                &host,
                port,
                "socks5_tcp",
                "socks_denied_in_limited_mode",
            ))
            .await;
        return write_socks5_failure(&mut client, 0x02).await;
    }

    let target = match evaluate_request(&state, &host, port, "socks5_tcp").await {
        Ok(target) => target,
        Err(blocked) => {
            state.record_blocked(blocked).await;
            return write_socks5_failure(&mut client, 0x02).await;
        }
    };

    let config = state.config_snapshot();
    let mut upstream = match connect_evaluated_target(&config, &target).await {
        Ok(stream) => stream,
        Err(_) => return write_socks5_failure(&mut client, 0x05).await,
    };
    client
        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

async fn write_socks5_failure(client: &mut TcpStream, code: u8) -> io::Result<()> {
    client
        .write_all(&[0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
}

async fn handle_client(state: Arc<NetworkProxyState>, mut client: TcpStream) -> io::Result<()> {
    let header = read_header(&mut client).await?;
    let Some(header_end) = find_header_end(&header) else {
        return write_forbidden(&mut client, "malformed_request").await;
    };
    let head = &header[..header_end - 4];
    let body_prefix = &header[header_end..];
    let head_text = String::from_utf8_lossy(head);
    let mut lines = head_text.split("\r\n");
    let Some(request_line) = lines.next() else {
        return write_forbidden(&mut client, "malformed_request").await;
    };
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let target = request_parts.next().unwrap_or_default();
    let version = request_parts.next().unwrap_or("HTTP/1.1");

    if method.eq_ignore_ascii_case("CONNECT") {
        return handle_connect(state, client, target).await;
    }

    state.maybe_reload_config().await;
    let config = state.config_snapshot();
    if !config.mode.allows_http_method(method) {
        let blocked = blocked(target, 0, "http", "method_denied");
        state.record_blocked(blocked).await;
        return write_forbidden(&mut client, "method_denied").await;
    }

    let rest = head_text
        .split_once("\r\n")
        .map(|(_, rest)| rest)
        .unwrap_or_default();
    if let Some(socket_path) = header_value(rest, "x-unix-socket") {
        return handle_unix_socket_request(
            state,
            client,
            UnixSocketRequest {
                method,
                target,
                version,
                headers: rest,
                body_prefix,
                socket_path,
            },
        )
        .await;
    }

    let Ok(url) = Url::parse(target) else {
        return write_forbidden(&mut client, "absolute_url_required").await;
    };
    let Some(host) = url.host_str().map(str::to_string) else {
        return write_forbidden(&mut client, "missing_host").await;
    };
    if state.trusted_windows_context.is_some()
        && method.eq_ignore_ascii_case("GET")
        && url.scheme() == "http"
        && host.eq_ignore_ascii_case("klaus.invalid")
        && url.path() == "/.well-known/klaus-sandbox-health"
    {
        client
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            .await?;
        return Ok(());
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let evaluated = match evaluate_request(&state, &host, port, "http").await {
        Ok(target) => target,
        Err(blocked) => {
            state.record_blocked(blocked).await;
            return write_forbidden(&mut client, "network_policy_denied").await;
        }
    };

    let config = state.config_snapshot();
    let use_upstream_proxy = config.upstream_proxy_url_for(/*is_secure*/ false).is_some();
    let mut upstream = if use_upstream_proxy {
        connect_upstream_proxy(&config, /*is_secure*/ false).await?
    } else {
        connect_direct_target(&evaluated).await?
    };
    let target_line = if use_upstream_proxy {
        target.to_string()
    } else {
        path_and_query(&url)
    };
    let rewritten = format!("{method} {target_line} {version}\r\n");
    upstream.write_all(rewritten.as_bytes()).await?;
    if use_upstream_proxy {
        write_upstream_proxy_authorization(&config, &mut upstream, /*is_secure*/ false).await?;
    }
    upstream.write_all(rest.as_bytes()).await?;
    upstream.write_all(b"\r\n\r\n").await?;
    upstream.write_all(body_prefix).await?;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

struct UnixSocketRequest<'a> {
    method: &'a str,
    target: &'a str,
    version: &'a str,
    headers: &'a str,
    body_prefix: &'a [u8],
    socket_path: String,
}

async fn handle_unix_socket_request(
    state: Arc<NetworkProxyState>,
    mut client: TcpStream,
    request: UnixSocketRequest<'_>,
) -> io::Result<()> {
    let config = state.config_snapshot();
    if !is_unix_socket_allowed(&config, &request.socket_path) {
        state
            .record_blocked(blocked(
                &request.socket_path,
                0,
                "unix_socket",
                "unix_socket_denied",
            ))
            .await;
        return write_forbidden(&mut client, "unix_socket_denied").await;
    }

    #[cfg(not(unix))]
    {
        let UnixSocketRequest {
            method,
            target,
            version,
            headers,
            body_prefix,
            socket_path,
        } = request;
        let _ = (method, target, version, headers, body_prefix);
        state
            .record_blocked(blocked(
                &socket_path,
                0,
                "unix_socket",
                "unix_socket_unsupported",
            ))
            .await;
        write_forbidden(&mut client, "unix_socket_unsupported").await
    }

    #[cfg(unix)]
    {
        let UnixSocketRequest {
            method,
            target,
            version,
            headers,
            body_prefix,
            socket_path,
        } = request;
        let mut upstream = tokio::net::UnixStream::connect(&socket_path).await?;
        let target_line = unix_socket_target_line(target);
        let sanitized_headers = remove_header(headers, "x-unix-socket");
        let rewritten = format!("{method} {target_line} {version}\r\n");
        upstream.write_all(rewritten.as_bytes()).await?;
        upstream.write_all(sanitized_headers.as_bytes()).await?;
        upstream.write_all(b"\r\n\r\n").await?;
        upstream.write_all(body_prefix).await?;
        let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
        Ok(())
    }
}

async fn handle_connect(
    state: Arc<NetworkProxyState>,
    mut client: TcpStream,
    target: &str,
) -> io::Result<()> {
    state.maybe_reload_config().await;
    if state.config_snapshot().mode == NetworkMode::Limited {
        state
            .record_blocked(blocked(
                target,
                443,
                "https_connect",
                "connect_denied_in_limited_mode",
            ))
            .await;
        return write_forbidden(&mut client, "connect_denied_in_limited_mode").await;
    }
    let Some((host, port)) = parse_authority(target, 443) else {
        return write_forbidden(&mut client, "malformed_connect_target").await;
    };
    let target = match evaluate_request(&state, &host, port, "https_connect").await {
        Ok(target) => target,
        Err(blocked) => {
            state.record_blocked(blocked).await;
            return write_forbidden(&mut client, "network_policy_denied").await;
        }
    };
    let config = state.config_snapshot();
    let mut upstream = connect_evaluated_target(&config, &target).await?;
    client
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

fn header_value(headers: &str, name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

/// Both helpers serve only the unix-socket proxy branch below (unix domain
/// sockets exist only on unix).
#[cfg(unix)]
fn remove_header(headers: &str, name: &str) -> String {
    headers
        .lines()
        .filter(|line| {
            line.split_once(':')
                .map(|(key, _)| !key.trim().eq_ignore_ascii_case(name))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>()
        .join("\r\n")
}

#[cfg(unix)]
fn unix_socket_target_line(target: &str) -> String {
    Url::parse(target)
        .map(|url| path_and_query(&url))
        .unwrap_or_else(|_| {
            if target.starts_with('/') {
                target.to_string()
            } else {
                "/".to_string()
            }
        })
}

fn is_unix_socket_allowed(config: &NetworkProxyConfig, socket_path: &str) -> bool {
    if !socket_path.starts_with('/') {
        return false;
    }
    if config.allow_all_unix_sockets {
        return true;
    }
    let requested = normalize_unix_socket_path(socket_path);
    config.unix_socket_paths().iter().any(|allowed| {
        is_absolute_unix_path(allowed)
            && normalize_unix_socket_path(&node_utf8_projection(allowed)) == requested
    })
}

fn normalize_unix_socket_path(path: &str) -> String {
    crate::fs::node_path::canonicalize_host(path)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

async fn read_header(client: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        let n = client.read(&mut chunk).await?;
        if n == 0 {
            return Ok(buf);
        }
        buf.extend_from_slice(&chunk[..n]);
        if find_header_end(&buf).is_some() {
            return Ok(buf);
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy request headers exceeded limit",
            ));
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn path_and_query(url: &Url) -> String {
    let mut path = url.path().to_string();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    path
}

fn parse_authority(authority: &str, default_port: u16) -> Option<(String, u16)> {
    let authority = authority.trim();
    if authority.is_empty() {
        return None;
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, rest) = rest.split_once(']')?;
        let port = rest
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Some((host.to_string(), port));
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if let Ok(port) = port.parse::<u16>() {
            return Some((host.to_string(), port));
        }
    }
    Some((authority.to_string(), default_port))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EvaluatedNetworkTarget {
    host: String,
    port: u16,
    addresses: Vec<SocketAddr>,
}

async fn evaluate_request(
    state: &NetworkProxyState,
    host: &str,
    port: u16,
    protocol: &str,
) -> Result<EvaluatedNetworkTarget, BlockedRequest> {
    state.maybe_reload_config().await;
    let config = state.config_snapshot();
    if restricted_destination_port(port) {
        return Err(blocked(host, port, protocol, "restricted_destination_port"));
    }
    let policy = config.domain_policy();
    if evaluate_host_policy(host, Some(&policy)) == HostPolicyDecision::Deny {
        let request = NetworkPolicyRequest {
            host: host.to_ascii_lowercase(),
            port,
            protocol: NetworkProtocol::from_label(protocol),
            reason: "domain_denied".to_string(),
        };
        if let Some(decider) = &state.policy_decider {
            let decision = decider.decide(request.clone()).await;
            if decision.decision != NetworkDecision::Allow {
                return Err(blocked(
                    host,
                    port,
                    protocol,
                    decision.reason.as_deref().unwrap_or("domain_denied"),
                ));
            }
        } else {
            return Err(blocked(host, port, protocol, "domain_denied"));
        }
    }

    let uses_upstream_proxy = config.upstream_proxy_url_for(protocol != "http").is_some();
    if uses_upstream_proxy {
        if !config.allow_local_binding() && is_private_or_loopback_host(host) {
            return Err(blocked(host, port, protocol, "local_network_denied"));
        }
        return Ok(EvaluatedNetworkTarget {
            host: normalized_target_host(host),
            port,
            addresses: Vec::new(),
        });
    }

    let addresses = match resolve_target_addresses(host, port).await {
        Ok(addresses) => addresses,
        Err(reason) => return Err(blocked(host, port, protocol, reason)),
    };
    if !config.allow_local_binding() && addresses.iter().any(|addr| is_non_public_ip(addr.ip())) {
        return Err(blocked(host, port, protocol, "local_network_denied"));
    }
    Ok(EvaluatedNetworkTarget {
        host: normalized_target_host(host),
        port,
        addresses,
    })
}

async fn resolve_target_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, &'static str> {
    let normalized = normalized_target_host(host);
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let resolved = timeout(
        Duration::from_secs(2),
        tokio::net::lookup_host((normalized.as_str(), port)),
    )
    .await
    .map_err(|_| "dns_resolution_timed_out")?
    .map_err(|_| "dns_resolution_failed")?;
    let mut addresses = resolved.collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err("dns_resolution_empty");
    }
    Ok(addresses)
}

fn normalized_target_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn restricted_destination_port(port: u16) -> bool {
    port == 0 || port == 53 || (137..=139).contains(&port) || port == 445 || port == 853
}

fn is_private_or_loopback_host(host: &str) -> bool {
    let host = normalized_target_host(host);
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(is_non_public_ip)
}

fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
                || ipv4_in_cidr(ip, [0, 0, 0, 0], 8)
                || ipv4_in_cidr(ip, [100, 64, 0, 0], 10)
                || ipv4_in_cidr(ip, [192, 0, 0, 0], 24)
                || ipv4_in_cidr(ip, [192, 0, 2, 0], 24)
                || ipv4_in_cidr(ip, [198, 18, 0, 0], 15)
                || ipv4_in_cidr(ip, [198, 51, 100, 0], 24)
                || ipv4_in_cidr(ip, [203, 0, 113, 0], 24)
                || ipv4_in_cidr(ip, [240, 0, 0, 0], 4)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_non_public_ip(IpAddr::V4(mapped));
            }
            let first_segment = ip.segments()[0];
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (first_segment & 0xfe00) == 0xfc00
                || (first_segment & 0xffc0) == 0xfe80
        }
    }
}

fn ipv4_in_cidr(ip: std::net::Ipv4Addr, base: [u8; 4], prefix: u8) -> bool {
    let ip = u32::from(ip);
    let base = u32::from(std::net::Ipv4Addr::from(base));
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    (ip & mask) == (base & mask)
}

async fn connect_evaluated_target(
    config: &NetworkProxyConfig,
    target: &EvaluatedNetworkTarget,
) -> io::Result<TcpStream> {
    if config.upstream_proxy_url_for(/*is_secure*/ true).is_some() {
        connect_target(config, &target.host, target.port).await
    } else {
        connect_direct_target(target).await
    }
}

async fn connect_direct_target(target: &EvaluatedNetworkTarget) -> io::Result<TcpStream> {
    let mut last_error = None;
    for address in &target.addresses {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                let peer = stream.peer_addr()?;
                if target.addresses.contains(&peer) {
                    return Ok(stream);
                }
                last_error = Some(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "connected peer did not match the evaluated DNS result",
                ));
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "evaluated target has no connectable address",
        )
    }))
}

fn blocked(host: &str, port: u16, protocol: &str, reason: &str) -> BlockedRequest {
    BlockedRequest {
        host: host.to_ascii_lowercase().into(),
        port,
        protocol: protocol.into(),
        reason: reason.into(),
        source: "proxy_policy".into(),
        timestamp_ms: Utc::now().timestamp_millis(),
    }
}

impl NetworkProxyState {
    fn config_snapshot(&self) -> NetworkProxyConfig {
        self.config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn maybe_reload_config(&self) {
        let Some(reloader) = &self.config_reloader else {
            return;
        };
        if let Some(config) = reloader.maybe_reload().await {
            *self
                .config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                config.with_env_upstream_proxy();
        }
    }

    async fn record_blocked(&self, request: BlockedRequest) {
        let request = {
            let mut blocked = self
                .blocked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            blocked.push_back(request);
            while blocked.len() > 200 {
                blocked.pop_front();
            }
            blocked.back().cloned()
        };
        if let (Some(observer), Some(request)) = (&self.blocked_observer, request) {
            observer.on_blocked_request(request).await;
        }
    }
}

async fn write_forbidden(client: &mut TcpStream, reason: &str) -> io::Result<()> {
    let body = format!("Network proxy denied request: {reason}\n");
    let response = format!(
        "HTTP/1.1 403 Forbidden\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    client.write_all(response.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    use crate::runtime_values::RuntimePath;
    use crate::types::sandbox::{ExecRequest, SandboxKind, SandboxPolicy, SpawnSpec};
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn runtime_env(entries: &[(&str, &str)]) -> RuntimeEnv {
        let mut env = RuntimeEnv::new();
        for (key, value) in entries {
            env.insert(RuntimeString::from(*key), RuntimeString::from(*value));
        }
        env
    }

    fn assert_env_value(env: &RuntimeEnv, key: &str, expected: &str) {
        assert_eq!(
            env.get(&RuntimeString::from(key)),
            Some(&RuntimeString::from(expected))
        );
    }

    #[test]
    fn network_proxy_runtime_codecs_preserve_utf16_and_declaration_order() {
        let exact = RuntimeString::from_utf16(vec![u16::from(b'x'), 0xd800, u16::from(b'y')]);
        let config = NetworkProxyConfig {
            enabled: true,
            transport: NetworkProxyTransport::Local,
            proxy_url: Some(exact.clone()),
            enable_socks5: false,
            socks_url: Some("socks5://127.0.0.1:1080".into()),
            allowed_domains: Some(vec![exact.clone()]),
            blocked_domains: None,
            allow_local_binding: Some(true),
            dangerously_allow_non_loopback_proxy: true,
            allow_upstream_proxy: true,
            upstream_proxy: Some("http://127.0.0.1:3128".into()),
            upstream_http_proxy: Some("http://127.0.0.1:3129".into()),
            upstream_https_proxy: Some("http://127.0.0.1:3130".into()),
            upstream_all_proxy: Some("http://127.0.0.1:3131".into()),
            allow_unix_sockets: Some(vec![exact.clone()]),
            allow_all_unix_sockets: true,
            managed_ca_bundle_path: Some(exact.clone()),
            credential_broker: CredentialBrokerConfig {
                enabled: true,
                brokered_env_keys: vec![exact.clone()],
            },
            mode: NetworkMode::Limited,
        };

        let encoded = runtime_codec::encode_value(&config).unwrap();
        let RuntimeJson::Object(object) = &encoded else {
            panic!("network proxy config must encode as an object");
        };
        let keys = object
            .entries()
            .map(|(key, _)| key.try_to_utf8().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            [
                "enabled",
                "transport",
                "proxyUrl",
                "enableSocks5",
                "socksUrl",
                "allowedDomains",
                "blockedDomains",
                "allowLocalBinding",
                "dangerouslyAllowNonLoopbackProxy",
                "allowUpstreamProxy",
                "upstreamProxy",
                "upstreamHttpProxy",
                "upstreamHttpsProxy",
                "upstreamAllProxy",
                "allowUnixSockets",
                "allowAllUnixSockets",
                "managedCaBundlePath",
                "credentialBroker",
                "mode",
            ]
        );
        assert_eq!(
            runtime_codec::decode_value::<NetworkProxyConfig>(&encoded).unwrap(),
            config
        );

        let blocked = BlockedRequest {
            host: exact.clone(),
            port: 443,
            protocol: "https_connect".into(),
            reason: exact.clone(),
            source: "proxy_policy".into(),
            timestamp_ms: 1,
        };
        let encoded = runtime_codec::encode_value(&blocked).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<BlockedRequest>(&encoded).unwrap(),
            blocked
        );

        let serde_error = serde_json::to_string(&config).unwrap_err();
        assert!(serde_error
            .to_string()
            .contains("unpaired UTF-16 surrogate"));
    }

    struct CaptureSandbox {
        captured: Arc<Mutex<Option<(ExecRequest, SandboxPolicy)>>>,
    }

    impl SandboxBackend for CaptureSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some((req.clone(), policy.clone()));
            SpawnSpec {
                file: req.argv[0].clone(),
                args: req.argv[1..].to_vec(),
                cwd: req.cwd.clone(),
                env: req.env.clone().unwrap_or_default(),
            }
        }
    }

    async fn proxy_request(proxy_addr: SocketAddr, request: String) -> String {
        let mut stream = TcpStream::connect(proxy_addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn socks5_connect_and_read(proxy_addr: SocketAddr, host: &str, port: u16) -> Vec<u8> {
        let mut stream = TcpStream::connect(proxy_addr).await.unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut greeting = [0u8; 2];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [0x05, 0x00]);

        let host_bytes = host.as_bytes();
        let mut request = vec![0x05, 0x01, 0x00, 0x03, host_bytes.len() as u8];
        request.extend_from_slice(host_bytes);
        request.extend_from_slice(&port.to_be_bytes());
        stream.write_all(&request).await.unwrap();

        let mut response = [0u8; 10];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(response[0], 0x05);
        assert_eq!(response[1], 0x00, "SOCKS connect failed: {response:?}");

        let request = format!("GET / HTTP/1.1\r\nhost: {host}:{port}\r\nconnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        buf
    }

    async fn spawn_upstream(body: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0; 4096];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        addr
    }

    async fn spawn_recording_upstream_proxy(
        response_body: &'static str,
    ) -> (SocketAddr, Arc<Mutex<Option<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first_line = Arc::new(Mutex::new(None));
        let first_line_for_task = Arc::clone(&first_line);
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0; 4096];
            let Ok(n) = socket.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            *first_line_for_task.lock().unwrap() = request.lines().next().map(str::to_string);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        (addr, first_line)
    }

    async fn spawn_recording_connect_proxy() -> (SocketAddr, Arc<Mutex<Option<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first_line = Arc::new(Mutex::new(None));
        let first_line_for_task = Arc::clone(&first_line);
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0; 4096];
            let Ok(n) = socket.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            *first_line_for_task.lock().unwrap() = request.lines().next().map(str::to_string);
            let _ = socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await;
        });
        (addr, first_line)
    }

    #[cfg(unix)]
    async fn spawn_unix_http_server(response_body: &'static str) -> String {
        use tokio::net::UnixListener;

        let path = std::env::temp_dir().join(format!(
            "klaus-network-proxy-{}-{}.sock",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let cleanup_path = path.clone();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                let _ = std::fs::remove_file(&cleanup_path);
                return;
            };
            let mut buf = vec![0; 4096];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = std::fs::remove_file(&cleanup_path);
        });
        path_string
    }

    struct AllowPolicyDecider;

    #[async_trait]
    impl NetworkPolicyDecider for AllowPolicyDecider {
        async fn decide(&self, request: NetworkPolicyRequest) -> NetworkPolicyDecision {
            NetworkPolicyDecision::allow("test_decider", request)
        }
    }

    struct OneShotReload {
        next: Mutex<Option<NetworkProxyConfig>>,
    }

    #[async_trait]
    impl ConfigReloader for OneShotReload {
        async fn maybe_reload(&self) -> Option<NetworkProxyConfig> {
            self.next.lock().unwrap().take()
        }
    }

    struct RecordingObserver {
        seen: Arc<Mutex<Vec<BlockedRequest>>>,
    }

    #[async_trait]
    impl BlockedRequestObserver for RecordingObserver {
        async fn on_blocked_request(&self, request: BlockedRequest) {
            self.seen.lock().unwrap().push(request);
        }
    }

    #[tokio::test]
    async fn prepare_env_sets_proxy_vars_and_loopback_sandbox_context() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let prepared = proxy.prepare_env(RuntimeEnv::new());
        let proxy_url = format!("http://{}", proxy.http_addr());
        let socks_url = format!("socks5h://{}", proxy.socks_addr().unwrap());

        assert_env_value(&prepared.env, "KLAUS_NETWORK_PROXY_ACTIVE", "1");
        assert_env_value(&prepared.env, "HTTP_PROXY", &proxy_url);
        assert_env_value(&prepared.env, "HTTPS_PROXY", &proxy_url);
        assert_env_value(&prepared.env, "ALL_PROXY", &socks_url);
        let mut ports = prepared.sandbox_context.loopback_ports.clone();
        ports.sort_unstable();
        assert_eq!(
            ports,
            vec![proxy.http_addr().port(), proxy.socks_addr().unwrap().port()]
        );
        assert!(!prepared.sandbox_context.allow_local_binding);
    }

    #[tokio::test]
    async fn prepare_env_removes_case_insensitive_proxy_and_private_sandbox_overrides() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let env = runtime_env(&[
            ("http_proxy", "http://attacker.invalid:1"),
            ("Https_Proxy", "http://attacker.invalid:2"),
            ("all_proxy", "socks5://attacker.invalid:3"),
            ("No_Proxy", "*"),
            ("kLaUs_SaNdBoX_RoUtE_SiD", "forged"),
            ("KLAUS_NETWORK_PROXY_ACTIVE", "forged"),
        ]);

        let prepared = proxy.prepare_env(env);
        let entries = prepared
            .env
            .entries()
            .map(|(key, value)| {
                (
                    key.try_to_utf8().unwrap(),
                    value.try_to_utf8().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        assert_env_value(
            &prepared.env,
            "http_proxy",
            &format!("http://{}", proxy.http_addr()),
        );
        assert!(!entries.iter().any(|(key, _)| key == "Https_Proxy"));
        assert_env_value(
            &prepared.env,
            "all_proxy",
            &format!("socks5h://{}", proxy.socks_addr().unwrap()),
        );
        assert!(!entries.iter().any(|(key, _)| key == "No_Proxy"));
        assert!(!entries
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("klaus_sandbox_route_sid")));
        assert_env_value(&prepared.env, "KLAUS_NETWORK_PROXY_ACTIVE", "1");
        assert!(!entries.iter().any(|(key, _)| key == "NO_PROXY"));
        assert!(!entries
            .iter()
            .any(|(_, value)| value.contains("attacker.invalid") || value == "forged"));
    }

    #[test]
    fn trusted_windows_context_requires_the_exact_canonical_policy_digest() {
        let config = NetworkProxyConfig {
            enabled: true,
            transport: NetworkProxyTransport::WindowsBroker,
            enable_socks5: true,
            allow_local_binding: Some(false),
            allow_upstream_proxy: false,
            ..Default::default()
        };
        let digest = network_proxy_policy_digest(&config).unwrap();
        assert_eq!(digest, network_proxy_policy_digest(&config).unwrap());
        let context = TrustedWindowsSandboxContext {
            route_sid: RuntimeString::from("S-1-5-21-1-2-3-4"),
            http_addr: "127.0.0.1:3128".parse().unwrap(),
            socks_addr: Some("127.0.0.1:8081".parse().unwrap()),
            broker_generation: RuntimeString::from("generation"),
            policy_digest: RuntimeString::from("forged"),
        };
        assert_eq!(
            NetworkProxy::from_trusted_windows_context(config.clone(), context.clone())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let proxy = NetworkProxy::from_trusted_windows_context(
            config.clone(),
            TrustedWindowsSandboxContext {
                policy_digest: RuntimeString::from(digest),
                ..context
            },
        )
        .unwrap();
        let prepared = proxy.prepare_env(RuntimeEnv::new());
        assert_eq!(
            prepared
                .sandbox_context
                .network_proxy_restricting_sid
                .as_ref(),
            Some(&RuntimeString::from("S-1-5-21-1-2-3-4"))
        );

        let mut changed = config;
        changed.blocked_domains = Some(vec![RuntimeString::from("blocked.example")]);
        assert_ne!(
            network_proxy_policy_digest(&changed).unwrap(),
            network_proxy_policy_digest(
                &proxy
                    .state
                    .config
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn prepare_env_threads_mitm_credential_and_unix_socket_context() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_unix_sockets: Some(vec!["/tmp/docker.sock".into()]),
            managed_ca_bundle_path: Some("/tmp/klaus-ca.pem".into()),
            credential_broker: CredentialBrokerConfig {
                enabled: true,
                brokered_env_keys: vec!["GITHUB_TOKEN".into()],
            },
            ..Default::default()
        })
        .await
        .unwrap();

        let prepared = proxy.prepare_env(RuntimeEnv::new());

        assert_env_value(&prepared.env, "SSL_CERT_FILE", "/tmp/klaus-ca.pem");
        assert_env_value(&prepared.env, "NODE_EXTRA_CA_CERTS", "/tmp/klaus-ca.pem");
        assert_env_value(&prepared.env, "KLAUS_CREDENTIAL_BROKER_ACTIVE", "1");
        assert_env_value(
            &prepared.env,
            "KLAUS_BROKERED_CREDENTIAL_KEYS",
            "GITHUB_TOKEN",
        );
        assert_eq!(
            prepared.sandbox_context.allow_unix_sockets,
            vec![RuntimeString::from("/tmp/docker.sock")]
        );
        assert_eq!(
            prepared.sandbox_context.managed_ca_bundle_path.as_ref(),
            Some(&RuntimeString::from("/tmp/klaus-ca.pem"))
        );
        assert_env_value(&prepared.env, "GIT_SSL_CAINFO", "/tmp/klaus-ca.pem");
        assert_env_value(&prepared.env, "BUNDLE_SSL_CA_CERT", "/tmp/klaus-ca.pem");
        assert_env_value(&prepared.env, "AWS_CA_BUNDLE", "/tmp/klaus-ca.pem");
        assert_env_value(
            &prepared.env,
            "DOCKER_HTTP_PROXY",
            &format!("http://{}", proxy.http_addr()),
        );
        assert_env_value(
            &prepared.env,
            "npm_config_proxy",
            &format!("http://{}", proxy.http_addr()),
        );
        assert_env_value(&prepared.env, "ELECTRON_GET_USE_PROXY", "1");
        #[cfg(any(target_os = "macos", test))]
        {
            let command = prepared
                .env
                .get(&RuntimeString::from("GIT_SSH_COMMAND"))
                .unwrap();
            assert!(command.starts_with("KLAUS_PROXY_GIT_SSH_COMMAND=1 ssh -o ProxyCommand="));
            assert!(command.contains(&proxy.socks_addr().unwrap().to_string()));
        }
    }

    #[tokio::test]
    async fn prepare_env_preserves_existing_git_ssh_wrappers() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let env = runtime_env(&[("GIT_SSH_COMMAND", "ssh -o IdentityAgent=/tmp/agent.sock")]);

        let prepared = proxy.prepare_env(env);

        assert_env_value(
            &prepared.env,
            "GIT_SSH_COMMAND",
            "ssh -o IdentityAgent=/tmp/agent.sock",
        );
    }

    #[tokio::test]
    async fn bind_urls_are_clamped_to_loopback_by_default() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            proxy_url: Some("http://0.0.0.0:0".into()),
            socks_url: Some("http://0.0.0.0:0".into()),
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(proxy.http_addr().ip().is_loopback());
        assert!(proxy.socks_addr().unwrap().ip().is_loopback());
    }

    #[tokio::test]
    async fn unix_socket_proxying_forces_loopback_even_when_non_loopback_is_allowed() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            proxy_url: Some("http://0.0.0.0:0".into()),
            socks_url: Some("http://0.0.0.0:0".into()),
            dangerously_allow_non_loopback_proxy: true,
            allow_unix_sockets: Some(vec!["/tmp/docker.sock".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(proxy.http_addr().ip().is_loopback());
        assert!(proxy.socks_addr().unwrap().ip().is_loopback());
    }

    #[tokio::test]
    async fn proxy_denies_blocked_domain_and_records_it() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            blocked_domains: Some(vec!["blocked.test".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "GET http://blocked.test/path HTTP/1.1\r\nhost: blocked.test\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403"));
        let blocked = proxy.blocked_snapshot().await;
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].host, "blocked.test");
        assert_eq!(blocked[0].reason, "domain_denied");
    }

    #[tokio::test]
    async fn blocked_request_observer_receives_denied_requests() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let proxy = NetworkProxy::builder(NetworkProxyConfig {
            enabled: true,
            blocked_domains: Some(vec!["blocked.test".into()]),
            ..Default::default()
        })
        .blocked_request_observer_arc(Arc::new(RecordingObserver {
            seen: Arc::clone(&seen),
        }))
        .build()
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "GET http://blocked.test/path HTTP/1.1\r\nhost: blocked.test\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403"));
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].host, "blocked.test");
        assert_eq!(seen[0].reason, "domain_denied");
    }

    #[tokio::test]
    async fn policy_decider_can_allow_a_static_allowlist_miss() {
        let upstream = spawn_upstream("decider-ok").await;
        let proxy = NetworkProxy::builder(NetworkProxyConfig {
            enabled: true,
            allowed_domains: Some(vec!["docs.example".into()]),
            allow_local_binding: Some(true),
            ..Default::default()
        })
        .policy_decider_arc(Arc::new(AllowPolicyDecider))
        .build()
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            format!(
                "GET http://127.0.0.1:{}/hello HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n",
                upstream.port()
            ),
        )
        .await;

        assert!(response.contains("decider-ok"), "{response}");
        assert!(proxy.blocked_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn config_reloader_hot_updates_domain_policy_before_requests() {
        let proxy = NetworkProxy::builder(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .config_reloader_arc(Arc::new(OneShotReload {
            next: Mutex::new(Some(NetworkProxyConfig {
                enabled: true,
                blocked_domains: Some(vec!["blocked.test".into()]),
                ..Default::default()
            })),
        }))
        .build()
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "GET http://blocked.test/path HTTP/1.1\r\nhost: blocked.test\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403"));
        let blocked = proxy.blocked_snapshot().await;
        assert_eq!(blocked[0].reason, "domain_denied");
    }

    #[tokio::test]
    async fn http_forwarding_can_route_through_configured_upstream_proxy() {
        let (upstream_proxy, first_line) = spawn_recording_upstream_proxy("upstream-ok").await;
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_upstream_proxy: true,
            upstream_proxy: Some(format!("http://{upstream_proxy}").into()),
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "GET http://example.test/resource HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.contains("upstream-ok"), "{response}");
        assert_eq!(
            first_line.lock().unwrap().as_deref(),
            Some("GET http://example.test/resource HTTP/1.1")
        );
    }

    #[tokio::test]
    async fn upstream_proxy_selection_is_protocol_specific() {
        let (http_proxy, http_first_line) = spawn_recording_upstream_proxy("http-upstream").await;
        let (https_proxy, https_first_line) = spawn_recording_connect_proxy().await;
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_upstream_proxy: true,
            upstream_http_proxy: Some(format!("http://{http_proxy}").into()),
            upstream_https_proxy: Some(format!("http://{https_proxy}").into()),
            ..Default::default()
        })
        .await
        .unwrap();

        let http_response = proxy_request(
            proxy.http_addr(),
            "GET http://example.test/resource HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n".to_string(),
        )
        .await;
        let connect_response = proxy_request(
            proxy.http_addr(),
            "CONNECT secure.example:443 HTTP/1.1\r\nhost: secure.example:443\r\n\r\n".to_string(),
        )
        .await;

        assert!(http_response.contains("http-upstream"), "{http_response}");
        assert!(
            connect_response.starts_with("HTTP/1.1 200"),
            "{connect_response}"
        );
        assert_eq!(
            http_first_line.lock().unwrap().as_deref(),
            Some("GET http://example.test/resource HTTP/1.1")
        );
        assert_eq!(
            https_first_line.lock().unwrap().as_deref(),
            Some("CONNECT secure.example:443 HTTP/1.1")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn http_proxy_routes_allowlisted_unix_socket_requests() {
        let socket_path = spawn_unix_http_server("unix-ok").await;
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_unix_sockets: Some(vec![socket_path.clone().into()]),
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            format!(
                "GET /_ping HTTP/1.1\r\nhost: docker.local\r\nx-unix-socket: {socket_path}\r\nconnection: close\r\n\r\n"
            ),
        )
        .await;

        assert!(response.contains("unix-ok"), "{response}");
        assert!(proxy.blocked_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn limited_network_mode_rejects_mutating_http_methods() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            mode: NetworkMode::Limited,
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "POST http://example.test/resource HTTP/1.1\r\nhost: example.test\r\ncontent-length: 0\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403"));
        let blocked = proxy.blocked_snapshot().await;
        assert_eq!(blocked[0].reason, "method_denied");
    }

    #[tokio::test]
    async fn proxy_denies_private_targets_by_default() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            "GET http://127.0.0.1:9/ HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n".to_string(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 403"));
        let blocked = proxy.blocked_snapshot().await;
        assert_eq!(blocked[0].host, "127.0.0.1");
        assert_eq!(blocked[0].reason, "local_network_denied");
    }

    #[test]
    fn classifies_non_public_ip_ranges() {
        for address in [
            "100.64.0.1",
            "192.0.2.10",
            "198.51.100.10",
            "203.0.113.10",
            "fc00::1",
            "fdff:ffff::1",
            "fe80::1",
            "febf:ffff::1",
        ] {
            assert!(is_non_public_ip(address.parse().unwrap()), "{address}");
        }
        for address in [
            "8.8.8.8",
            "2001:4860:4860::8888",
            "fbff:ffff::1",
            "fe00::1",
            "fe7f:ffff::1",
            "fec0::1",
        ] {
            assert!(!is_non_public_ip(address.parse().unwrap()), "{address}");
        }
    }

    #[tokio::test]
    async fn proxy_forwards_allowed_http_requests() {
        let upstream = spawn_upstream("proxied-ok").await;
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_local_binding: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();

        let response = proxy_request(
            proxy.http_addr(),
            format!(
                "GET http://{upstream}/hello HTTP/1.1\r\nhost: {upstream}\r\nconnection: close\r\n\r\n"
            ),
        )
        .await;

        assert!(response.contains("proxied-ok"), "{response}");
        assert!(proxy.blocked_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn socks5_proxy_forwards_allowed_tcp_connects() {
        let upstream = spawn_upstream("socks-ok").await;
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            allow_local_binding: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();

        let bytes =
            socks5_connect_and_read(proxy.socks_addr().unwrap(), "127.0.0.1", upstream.port())
                .await;

        let response = String::from_utf8_lossy(&bytes);
        assert!(response.contains("socks-ok"), "{response}");
    }

    #[tokio::test]
    async fn socks5_proxy_denies_blocked_domains_and_records_them() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            blocked_domains: Some(vec!["blocked.test".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

        let mut stream = TcpStream::connect(proxy.socks_addr().unwrap())
            .await
            .unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut greeting = [0u8; 2];
        stream.read_exact(&mut greeting).await.unwrap();
        let host = b"blocked.test";
        let mut request = vec![0x05, 0x01, 0x00, 0x03, host.len() as u8];
        request.extend_from_slice(host);
        request.extend_from_slice(&443u16.to_be_bytes());
        stream.write_all(&request).await.unwrap();
        let mut response = [0u8; 10];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(response[1], 0x02);
        let blocked = proxy.blocked_snapshot().await;
        assert_eq!(blocked[0].host, "blocked.test");
        assert_eq!(blocked[0].protocol, "socks5_tcp");
    }

    #[tokio::test]
    async fn sandbox_wrapper_injects_proxy_env_and_loopback_policy() {
        let proxy = NetworkProxy::start_sync(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .unwrap();
        let captured = Arc::new(Mutex::new(None));
        let sandbox = ManagedNetworkSandboxBackend::new(
            Arc::new(CaptureSandbox {
                captured: Arc::clone(&captured),
            }),
            proxy.clone(),
        );
        let env = runtime_env(&[("K", "V")]);

        let spec = sandbox.transform(
            &ExecRequest {
                argv: vec!["sh".into(), "-lc".into(), "true".into()],
                cwd: "/tmp".into(),
                env: Some(env),
                timeout_ms: None,
            },
            &SandboxPolicy {
                writable_roots: vec!["/tmp".into()],
                network_allowed: true,
                ..Default::default()
            },
        );

        assert_env_value(&spec.env, "K", "V");
        assert_env_value(&spec.env, "KLAUS_NETWORK_PROXY_ACTIVE", "1");
        let (_req, policy) = captured.lock().unwrap().clone().unwrap();
        assert!(!policy.network_allowed);
        assert_eq!(policy.allow_loopback_ports, Some(proxy.loopback_ports()));
    }

    #[tokio::test]
    async fn managed_proxy_preserves_workspace_writes_without_network_exemptions() {
        let proxy = NetworkProxy::start_sync(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .unwrap();
        let captured = Arc::new(Mutex::new(None));
        let sandbox = ManagedNetworkSandboxBackend::new(
            Arc::new(CaptureSandbox {
                captured: captured.clone(),
            }),
            proxy,
        );
        sandbox.transform(
            &ExecRequest {
                argv: vec!["sh".into(), "-lc".into(), "true".into()],
                cwd: "/tmp".into(),
                env: None,
                timeout_ms: None,
            },
            &SandboxPolicy {
                writable_roots: vec!["/workspace".into()],
                network_allowed: true,
                runtime_network_ceiling: Some(false),
                allow_loopback_ports: Some(vec![1234]),
                allow_all_unix_sockets: Some(true),
                ..Default::default()
            },
        );
        let (request, policy) = captured.lock().unwrap().clone().unwrap();
        assert!(!policy.network_allowed);
        assert!(policy.allow_loopback_ports.is_none());
        assert!(policy.allow_all_unix_sockets.is_none());
        assert_eq!(
            policy.writable_roots,
            vec![RuntimeString::from("/workspace")]
        );
        assert!(!policy.planning_read_only);
        assert!(request.env.is_none());
    }

    #[tokio::test]
    async fn managed_network_try_new_rejects_backends_that_cannot_enforce_it() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let captured = Arc::new(Mutex::new(None));

        let err = match ManagedNetworkSandboxBackend::try_new(
            Arc::new(CaptureSandbox {
                captured: Arc::clone(&captured),
            }),
            proxy,
        ) {
            Ok(_) => panic!("process backend must not be accepted for managed networking"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("does not enforce managed networking"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn shipped_linux_executor_is_admitted_for_managed_proxy_routing() {
        let proxy = NetworkProxy::start(NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let sandbox = ManagedNetworkSandboxBackend::try_new(
            Arc::new(crate::sandbox::LinuxSandbox::new()),
            proxy,
        )
        .expect("the namespace executor enforces dedicated proxy routes");
        assert_eq!(sandbox.kind(), SandboxKind::Bubblewrap);
    }

    #[test]
    fn binding_omission_is_resolved_for_the_selected_executor() {
        let config = NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        };
        assert_eq!(config.allow_local_binding, None);
        assert!(!config.allow_local_binding());
        let native = config.resolve_for_sandbox(SandboxKind::WindowsMxc).unwrap();
        assert_eq!(native.allow_local_binding, Some(true));
        let token = config
            .resolve_for_sandbox(SandboxKind::WindowsRestrictedToken)
            .unwrap();
        assert_eq!(token.allow_local_binding, Some(false));
        let denied = NetworkProxyConfig {
            allow_local_binding: Some(false),
            ..config
        };
        assert!(denied
            .resolve_for_sandbox(SandboxKind::WindowsMxc)
            .unwrap_err()
            .to_string()
            .contains("MXC cannot enforce allow_local_binding=false"));
        let omitted = runtime_codec::encode_value(&NetworkProxyConfig::default()).unwrap();
        let roundtrip: NetworkProxyConfig = runtime_codec::decode_value(&omitted).unwrap();
        assert_eq!(roundtrip.allow_local_binding, None);
    }

    #[test]
    fn mxc_managed_network_uses_local_listeners_and_no_token_broker() {
        assert!(sandbox_kind_enforces_managed_network(
            SandboxKind::WindowsMxc
        ));
        assert!(!sandbox_kind_is_native_windows(SandboxKind::WindowsMxc));
        assert!(windows_sandbox_supports_network_transport_for_kind(
            SandboxKind::WindowsMxc,
            NetworkProxyTransport::Local
        ));
        assert!(!windows_sandbox_supports_network_transport_for_kind(
            SandboxKind::WindowsMxc,
            NetworkProxyTransport::WindowsBroker
        ));
    }

    #[tokio::test]
    async fn mxc_context_contains_actual_ephemeral_ports_and_preserves_denial_ceiling() {
        struct MxcCapture {
            captured: Arc<Mutex<Option<SandboxPolicy>>>,
        }
        impl SandboxBackend for MxcCapture {
            fn kind(&self) -> SandboxKind {
                SandboxKind::WindowsMxc
            }
            fn is_available(&self) -> bool {
                true
            }
            fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
                *self.captured.lock().unwrap() = Some(policy.clone());
                SpawnSpec {
                    file: "fixture".into(),
                    args: Vec::new(),
                    cwd: req.cwd.clone(),
                    env: req.env.clone().unwrap_or_default(),
                }
            }
        }
        let config = NetworkProxyConfig {
            enabled: true,
            proxy_url: Some("http://127.0.0.1:0".into()),
            socks_url: Some("socks5://127.0.0.1:0".into()),
            ..Default::default()
        }
        .resolve_for_sandbox(SandboxKind::WindowsMxc)
        .unwrap();
        let proxy = NetworkProxy::start(config).await.unwrap();
        let expected = proxy.loopback_ports();
        assert!(!expected.contains(&0));
        let captured = Arc::new(Mutex::new(None));
        let backend = ManagedNetworkSandboxBackend::try_new(
            Arc::new(MxcCapture {
                captured: captured.clone(),
            }),
            proxy,
        )
        .unwrap();
        let req = ExecRequest {
            argv: vec!["fixture".into()],
            cwd: "/workspace".into(),
            env: None,
            timeout_ms: None,
        };
        backend.transform(&req, &SandboxPolicy::default());
        let policy = captured.lock().unwrap().clone().unwrap();
        assert_eq!(policy.allow_loopback_ports, Some(expected));
        assert_eq!(policy.windows_network_allow_local_binding, Some(true));
        assert!(policy.windows_network_proxy_restricting_sid.is_none());
        backend.transform(
            &req,
            &SandboxPolicy {
                runtime_network_ceiling: Some(false),
                ..Default::default()
            },
        );
        let policy = captured.lock().unwrap().clone().unwrap();
        assert!(policy.allow_loopback_ports.is_none());
        assert!(!policy.network_allowed);
    }

    #[test]
    fn managed_network_classifies_the_dedicated_windows_backend() {
        assert!(sandbox_kind_enforces_managed_network(
            SandboxKind::WindowsRestrictedToken
        ));
        assert!(sandbox_kind_is_native_windows(
            SandboxKind::WindowsRestrictedToken
        ));
        assert!(!sandbox_kind_is_native_windows(SandboxKind::NativeHelper));
        assert!(windows_sandbox_supports_network_transport(
            NetworkProxyTransport::WindowsBroker
        ));
        assert!(!windows_sandbox_supports_network_transport(
            NetworkProxyTransport::Local
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_guard_context_retains_ports_without_exposing_a_route() {
        struct GuardCaptureSandbox {
            captured: Arc<Mutex<Option<SandboxPolicy>>>,
        }

        impl SandboxBackend for GuardCaptureSandbox {
            fn kind(&self) -> SandboxKind {
                SandboxKind::WindowsRestrictedToken
            }

            fn is_available(&self) -> bool {
                true
            }

            fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
                *self.captured.lock().unwrap() = Some(policy.clone());
                SpawnSpec {
                    file: req.argv[0].clone(),
                    args: req.argv[1..].to_vec(),
                    cwd: req.cwd.clone(),
                    env: req.env.clone().unwrap_or_default(),
                }
            }
        }

        let captured = Arc::new(Mutex::new(None));
        let guard = WindowsNetworkGuardSandboxBackend::try_new(
            Arc::new(GuardCaptureSandbox {
                captured: Arc::clone(&captured),
            }),
            TrustedWindowsSandboxContext {
                route_sid: RuntimeString::default(),
                http_addr: SocketAddr::from(([127, 0, 0, 1], 3128)),
                socks_addr: Some(SocketAddr::from(([127, 0, 0, 1], 8081))),
                broker_generation: RuntimeString::from("generation-1"),
                policy_digest: RuntimeString::default(),
            },
        )
        .unwrap();

        let request = ExecRequest {
            argv: vec![RuntimeString::from("cmd.exe")],
            cwd: RuntimePath::from(r"C:\workspace"),
            env: Some(runtime_env(&[("HTTP_PROXY", "http://attacker.invalid:1")])),
            timeout_ms: None,
        };
        guard.transform(&request, &SandboxPolicy::default());

        let policy = captured.lock().unwrap().clone().unwrap();
        assert!(!policy.network_allowed);
        assert_eq!(
            policy.windows_network_proxy_guard_ports,
            Some(vec![3128, 8081])
        );
        assert!(policy.windows_network_proxy_restricting_sid.is_none());
        assert!(policy.windows_network_allow_local_binding.is_none());
    }

    #[cfg(windows)]
    #[test]
    fn windows_guard_context_rejects_route_credentials_and_bad_addresses() {
        struct NativeSandbox;

        impl SandboxBackend for NativeSandbox {
            fn kind(&self) -> SandboxKind {
                SandboxKind::WindowsRestrictedToken
            }

            fn is_available(&self) -> bool {
                true
            }

            fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
                SpawnSpec {
                    file: req.argv[0].clone(),
                    args: req.argv[1..].to_vec(),
                    cwd: req.cwd.clone(),
                    env: req.env.clone().unwrap_or_default(),
                }
            }
        }

        let inner: Arc<dyn SandboxBackend> = Arc::new(NativeSandbox);
        let routed = TrustedWindowsSandboxContext {
            route_sid: RuntimeString::from("S-1-5-21-1-2-3-4"),
            http_addr: SocketAddr::from(([127, 0, 0, 1], 3128)),
            socks_addr: Some(SocketAddr::from(([127, 0, 0, 1], 8081))),
            broker_generation: RuntimeString::from("generation-1"),
            policy_digest: RuntimeString::from("digest"),
        };
        assert!(WindowsNetworkGuardSandboxBackend::try_new(Arc::clone(&inner), routed).is_err());

        let non_loopback = TrustedWindowsSandboxContext {
            route_sid: RuntimeString::default(),
            http_addr: SocketAddr::from(([0, 0, 0, 0], 3128)),
            socks_addr: Some(SocketAddr::from(([127, 0, 0, 1], 8081))),
            broker_generation: RuntimeString::from("generation-1"),
            policy_digest: RuntimeString::default(),
        };
        assert!(WindowsNetworkGuardSandboxBackend::try_new(inner, non_loopback).is_err());
    }
}

#[cfg(test)]
mod native_service_projection_tests {
    use super::*;

    #[test]
    fn configured_service_proxy_preserves_roles_and_explicit_disable() {
        let mut config = NetworkProxyConfig {
            enabled: true,
            proxy_url: Some("http://127.0.0.1:48081".into()),
            socks_url: Some("socks5h://127.0.0.1:3128".into()),
            allow_local_binding: Some(true),
            ..Default::default()
        };
        let (settings, listeners) = config.windows_sandbox_proxy_configuration().unwrap();
        assert_eq!(settings.proxy_ports, vec![3128, 48081]);
        assert!(settings.allow_local_binding);
        assert_eq!(listeners.http_ports, vec![48081]);
        assert_eq!(listeners.socks_ports, vec![3128]);
        config.enabled = false;
        let (settings, listeners) = config.windows_sandbox_proxy_configuration().unwrap();
        assert!(settings.proxy_ports.is_empty());
        assert!(!settings.allow_local_binding);
        assert!(listeners.http_ports.is_empty());
        assert!(listeners.socks_ports.is_empty());
    }

    #[test]
    fn ephemeral_ports_cannot_be_provisioned_before_a_listener_is_bound() {
        let mut config = NetworkProxyConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(config
            .windows_sandbox_proxy_configuration()
            .unwrap_err()
            .to_string()
            .contains("network.proxy_url"));
        config.proxy_url = Some("http://127.0.0.1:3128".into());
        config.socks_url = Some("socks5://127.0.0.1:0".into());
        assert!(config
            .windows_sandbox_proxy_configuration()
            .unwrap_err()
            .to_string()
            .contains("network.socks_url"));
        config.enable_socks5 = false;
        let (settings, listeners) = config.windows_sandbox_proxy_configuration().unwrap();
        assert_eq!(settings.proxy_ports, vec![3128]);
        assert!(listeners.socks_ports.is_empty());
    }
}
