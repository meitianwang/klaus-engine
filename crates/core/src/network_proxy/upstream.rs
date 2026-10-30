use std::io;

use base64::Engine as _;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use url::Url;

use super::{read_header, NetworkProxyConfig};

pub(super) fn discover_upstream_proxy_from_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            if !value.trim().is_empty() {
                return Some(value);
            }
        }
    }
    None
}

pub(super) async fn connect_target(
    config: &NetworkProxyConfig,
    host: &str,
    port: u16,
) -> io::Result<TcpStream> {
    if config.upstream_proxy_url_for(/*is_secure*/ true).is_some() {
        connect_upstream_tunnel(config, host, port).await
    } else {
        TcpStream::connect((host, port)).await
    }
}

pub(super) async fn connect_upstream_proxy(
    config: &NetworkProxyConfig,
    is_secure: bool,
) -> io::Result<TcpStream> {
    let url = parse_upstream_proxy_url(config, is_secure)?;
    let host = url.host_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "upstream proxy is missing a host",
        )
    })?;
    let port = url.port_or_known_default().unwrap_or(80);
    TcpStream::connect((host, port)).await
}

async fn connect_upstream_tunnel(
    config: &NetworkProxyConfig,
    host: &str,
    port: u16,
) -> io::Result<TcpStream> {
    let mut stream = connect_upstream_proxy(config, /*is_secure*/ true).await?;
    let authority = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let request = format!("CONNECT {authority} HTTP/1.1\r\nhost: {authority}\r\n");
    stream.write_all(request.as_bytes()).await?;
    write_upstream_proxy_authorization(config, &mut stream, /*is_secure*/ true).await?;
    stream.write_all(b"\r\n").await?;
    let header = read_header(&mut stream).await?;
    let text = String::from_utf8_lossy(&header);
    let status_ok = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .is_some_and(|status| (200..300).contains(&status));
    if status_ok {
        Ok(stream)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "upstream proxy CONNECT was rejected",
        ))
    }
}

pub(super) async fn write_upstream_proxy_authorization(
    config: &NetworkProxyConfig,
    stream: &mut TcpStream,
    is_secure: bool,
) -> io::Result<()> {
    let url = parse_upstream_proxy_url(config, is_secure)?;
    if !url.username().is_empty() {
        let password = url.password().unwrap_or_default();
        let credentials = format!("{}:{password}", url.username());
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
        stream
            .write_all(format!("proxy-authorization: Basic {encoded}\r\n").as_bytes())
            .await?;
    }
    Ok(())
}

fn parse_upstream_proxy_url(config: &NetworkProxyConfig, is_secure: bool) -> io::Result<Url> {
    let raw = config.upstream_proxy_url_for(is_secure).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "upstream proxy is not configured")
    })?;
    let raw =
        String::from_utf8(raw.to_node_utf8()).expect("Node UTF-8 projection is always valid UTF-8");
    let url = Url::parse(&raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid upstream proxy URL: {err}"),
        )
    })?;
    if url.scheme() != "http" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only http upstream proxies are supported",
        ));
    }
    Ok(url)
}
