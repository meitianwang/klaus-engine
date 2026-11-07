// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to reuse captured native requests and existing setup environment rules.

use std::collections::HashMap;

use crate::boundary::NetworkBoundary;
use crate::protocol::SandboxRequest;
use crate::service::{ProvisioningSettings, ProxyListeners};

/// Uses the same environment-derived firewall settings as shared elevated setup.
pub fn provisioning_configuration(
    request: &SandboxRequest,
) -> (ProvisioningSettings, ProxyListeners) {
    let env = request
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let online = matches!(request.network, NetworkBoundary::Allowed);
    let identity = if online {
        super::setup::SandboxNetworkIdentity::Online
    } else {
        super::setup::SandboxNetworkIdentity::Offline
    };
    let settings = super::setup::offline_proxy_settings_from_env(&env, identity);
    let mut listeners = ProxyListeners::default();
    if !online {
        for value in super::setup::PROXY_ENV_KEYS
            .iter()
            .filter_map(|key| env.get(*key))
        {
            let Some((scheme, _)) = value.trim().split_once("://") else {
                continue;
            };
            let Some(port) = super::setup::loopback_proxy_port_from_url(value) else {
                continue;
            };
            match scheme.to_ascii_lowercase().as_str() {
                "http" | "https" => listeners.http_ports.push(port),
                "socks4" | "socks4a" | "socks5" | "socks5h" => listeners.socks_ports.push(port),
                _ => {}
            }
        }
        listeners.http_ports.sort_unstable();
        listeners.http_ports.dedup();
        listeners.socks_ports.sort_unstable();
        listeners.socks_ports.dedup();
    }
    (
        ProvisioningSettings {
            proxy_ports: settings.proxy_ports,
            allow_local_binding: settings.allow_local_binding,
        },
        listeners,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbsPath, FilesystemBoundary};
    use std::collections::BTreeMap;

    #[test]
    fn environment_settings_reuse_native_proxy_ports_and_protocol_roles() {
        let mut request = SandboxRequest {
            command: Vec::new(),
            cwd: AbsPath::new("C:\\workspace").unwrap(),
            env: BTreeMap::from([
                ("HTTP_PROXY".into(), "http://localhost:3128".into()),
                ("HTTPS_PROXY".into(), "http://127.0.0.1:3128".into()),
                ("ALL_PROXY".into(), "socks5h://[::1]:8081".into()),
                (
                    "KLAUS_WINDOWS_SANDBOX_PROXY_PORTS".into(),
                    "3128,10000,0".into(),
                ),
                ("KLAUS_NETWORK_ALLOW_LOCAL_BINDING".into(), "1".into()),
                ("WS_PROXY".into(), "http://remote.invalid:99".into()),
            ]),
            filesystem: FilesystemBoundary::default(),
            network: NetworkBoundary::Denied,
            network_route_capability_sid: None,
            container_name: String::new(),
            terminal: None,
        };
        let (settings, listeners) = provisioning_configuration(&request);
        assert_eq!(settings.proxy_ports, vec![3128, 8081, 10000]);
        assert!(settings.allow_local_binding);
        assert_eq!(listeners.http_ports, vec![3128]);
        assert_eq!(listeners.socks_ports, vec![8081]);
        request.network = NetworkBoundary::Allowed;
        assert_eq!(
            provisioning_configuration(&request),
            (ProvisioningSettings::default(), ProxyListeners::default())
        );
    }
}
