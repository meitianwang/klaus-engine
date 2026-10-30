use std::ffi::c_void;
use std::io;
use std::mem::{offset_of, size_of};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};

use winapi::um::winnt::{SE_GROUP_ENABLED, SE_GROUP_USE_FOR_DENY_ONLY};
use windows_sys::Win32::Foundation::{
    GetLastError, LocalFree, ERROR_INSUFFICIENT_BUFFER, HANDLE, HLOCAL, NO_ERROR,
};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetExtendedTcpTable, MIB_TCPROW_OWNER_PID, MIB_TCPTABLE_OWNER_PID,
    TCP_TABLE_OWNER_PID_CONNECTIONS,
};
use windows_sys::Win32::Networking::WinSock::AF_INET;
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{
    GetTokenInformation, TokenAppContainerSid, TokenCapabilities, TokenRestrictedSids, TokenUser,
    PSID, SID_AND_ATTRIBUTES, TOKEN_APPCONTAINER_INFORMATION, TOKEN_GROUPS,
    TOKEN_INFORMATION_CLASS, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AttributedWindowsProcess {
    pub process_id: u32,
    /// Legacy restricted-token source; never used as capability evidence.
    pub restricted_sids: Vec<String>,
    /// Only enabled, non-deny-only entries read from TokenCapabilities.
    pub capability_sids: Vec<String>,
    pub user_sid: String,
    pub app_container_sid: Option<String>,
}

/// Resolve the client process for an accepted IPv4 loopback connection.
///
/// The TCP tuple is queried both before and after token inspection. A changed
/// owner or vanished row is rejected so PID reuse and short-lived connection
/// races cannot route a request with stale token data.
pub(super) fn attribute_tcp_connection(
    accepted_local_addr: SocketAddr,
    accepted_peer_addr: SocketAddr,
) -> io::Result<AttributedWindowsProcess> {
    let (SocketAddr::V4(accepted_local_addr), SocketAddr::V4(accepted_peer_addr)) =
        (accepted_local_addr, accepted_peer_addr)
    else {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Windows proxy connection attribution supports IPv4 only",
        ));
    };
    if !accepted_local_addr.ip().is_loopback() || !accepted_peer_addr.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Windows proxy attribution requires a loopback connection",
        ));
    }

    let process_id = owning_process_id(accepted_local_addr, accepted_peer_addr)?;
    let attributed = token_identity_for_process(process_id)?;
    let verified_process_id = owning_process_id(accepted_local_addr, accepted_peer_addr)?;
    if verified_process_id != process_id {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Windows proxy connection owner changed during token attribution",
        ));
    }
    Ok(attributed)
}

fn owning_process_id(
    accepted_local_addr: SocketAddrV4,
    accepted_peer_addr: SocketAddrV4,
) -> io::Result<u32> {
    let mut byte_len = 0_u32;
    let result = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut byte_len,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_CONNECTIONS,
            0,
        )
    };
    if result != ERROR_INSUFFICIENT_BUFFER {
        return Err(win32_error("query IPv4 TCP owner table size", result));
    }

    let buffer = loop {
        let mut buffer = aligned_buffer(byte_len as usize)?;
        let result = unsafe {
            GetExtendedTcpTable(
                buffer.as_mut_ptr().cast::<c_void>(),
                &mut byte_len,
                0,
                AF_INET as u32,
                TCP_TABLE_OWNER_PID_CONNECTIONS,
                0,
            )
        };
        match result {
            NO_ERROR => break buffer,
            ERROR_INSUFFICIENT_BUFFER => continue,
            _ => return Err(win32_error("read IPv4 TCP owner table", result)),
        }
    };

    let rows = parse_tcp_owner_rows(&buffer, byte_len as usize)?;
    unique_client_process_id(rows, accepted_local_addr, accepted_peer_addr)
}

fn parse_tcp_owner_rows(buffer: &[usize], byte_len: usize) -> io::Result<&[MIB_TCPROW_OWNER_PID]> {
    let rows_offset = offset_of!(MIB_TCPTABLE_OWNER_PID, table);
    if byte_len > size_of_val(buffer) || byte_len < rows_offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid IPv4 TCP owner table length",
        ));
    }
    let row_count = unsafe { std::ptr::read_unaligned(buffer.as_ptr().cast::<u32>()) } as usize;
    let rows_byte_len = row_count
        .checked_mul(size_of::<MIB_TCPROW_OWNER_PID>())
        .and_then(|len| rows_offset.checked_add(len))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "IPv4 TCP owner table length overflow",
            )
        })?;
    if rows_byte_len > byte_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated IPv4 TCP owner table",
        ));
    }
    Ok(unsafe {
        let rows_ptr = buffer
            .as_ptr()
            .cast::<u8>()
            .add(rows_offset)
            .cast::<MIB_TCPROW_OWNER_PID>();
        std::slice::from_raw_parts(rows_ptr, row_count)
    })
}

fn unique_client_process_id(
    rows: &[MIB_TCPROW_OWNER_PID],
    accepted_local_addr: SocketAddrV4,
    accepted_peer_addr: SocketAddrV4,
) -> io::Result<u32> {
    let mut matching = rows
        .iter()
        .filter(|row| client_row_matches(row, accepted_local_addr, accepted_peer_addr))
        .map(|row| row.dwOwningPid);
    let process_id = matching.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "accepted connection is absent from the IPv4 TCP owner table",
        )
    })?;
    if matching.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "accepted connection has multiple IPv4 TCP owner rows",
        ));
    }
    Ok(process_id)
}

fn client_row_matches(
    row: &MIB_TCPROW_OWNER_PID,
    accepted_local_addr: SocketAddrV4,
    accepted_peer_addr: SocketAddrV4,
) -> bool {
    ipv4_addr_matches(row.dwLocalAddr, *accepted_peer_addr.ip())
        && tcp_port(row.dwLocalPort) == accepted_peer_addr.port()
        && ipv4_addr_matches(row.dwRemoteAddr, *accepted_local_addr.ip())
        && tcp_port(row.dwRemotePort) == accepted_local_addr.port()
}

fn ipv4_addr_matches(table_addr: u32, socket_addr: Ipv4Addr) -> bool {
    table_addr.to_ne_bytes() == socket_addr.octets()
}

fn tcp_port(table_port: u32) -> u16 {
    u16::from_be(table_port as u16)
}

fn token_identity_for_process(process_id: u32) -> io::Result<AttributedWindowsProcess> {
    let process_handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
    let process = owned_handle(process_handle, "open proxy client process")?;

    let token = open_process_token(process.as_raw_handle() as HANDLE)?;
    let user_sid = token_user_sid(&token)?;

    let (buffer, byte_len) = token_information(&token, TokenRestrictedSids)?;
    let restricted_sids = parse_token_groups(&buffer, byte_len)?
        .iter()
        .map(|entry| sid_to_string(entry.Sid))
        .collect::<io::Result<Vec<_>>>()?;

    // Do not merge capabilities with restricted or package SIDs: a capability
    // route must have been observed in this information class, with grant flags.
    let (buffer, byte_len) = token_information(&token, TokenCapabilities)?;
    let capability_sids = enabled_capability_sids(parse_token_groups(&buffer, byte_len)?)?;

    let (buffer, byte_len) = token_information(&token, TokenAppContainerSid)?;
    if byte_len < size_of::<TOKEN_APPCONTAINER_INFORMATION>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid AppContainer SID buffer length",
        ));
    }
    let info = unsafe {
        std::ptr::read_unaligned(buffer.as_ptr().cast::<TOKEN_APPCONTAINER_INFORMATION>())
    };
    let app_container_sid = if info.TokenAppContainer.is_null() {
        None
    } else {
        Some(sid_to_string(info.TokenAppContainer)?)
    };
    Ok(assemble_attribution(
        process_id,
        user_sid,
        restricted_sids,
        capability_sids,
        app_container_sid,
    ))
}

/// The primary-token owner used when registering a session's network route.
/// Querying it neither impersonates the caller nor changes any token privileges.
pub(super) fn current_user_sid() -> io::Result<String> {
    let token = open_process_token(unsafe { GetCurrentProcess() })?;
    token_user_sid(&token)
}

fn open_process_token(process: HANDLE) -> io::Result<OwnedHandle> {
    let mut token_handle = std::ptr::null_mut();
    let opened = unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token_handle) };
    if opened == 0 {
        return Err(last_error("open proxy client process token"));
    }
    owned_handle(token_handle, "open proxy client process token")
}

fn token_information(
    token: &OwnedHandle,
    information_class: TOKEN_INFORMATION_CLASS,
) -> io::Result<(Vec<usize>, usize)> {
    let mut byte_len = 0_u32;
    let queried = unsafe {
        GetTokenInformation(
            token.as_raw_handle() as HANDLE,
            information_class,
            std::ptr::null_mut(),
            0,
            &mut byte_len,
        )
    };
    if queried != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "token information unexpectedly fit an empty buffer",
        ));
    }
    if unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER {
        return Err(last_error("query proxy client token information size"));
    }

    let mut buffer = aligned_buffer(byte_len as usize)?;
    let queried = unsafe {
        GetTokenInformation(
            token.as_raw_handle() as HANDLE,
            information_class,
            buffer.as_mut_ptr().cast::<c_void>(),
            byte_len,
            &mut byte_len,
        )
    };
    if queried == 0 {
        return Err(last_error("read proxy client token information"));
    }
    if byte_len == 0 || byte_len as usize > size_of_val(&buffer[..]) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid proxy client token information length",
        ));
    }
    Ok((buffer, byte_len as usize))
}

fn token_user_sid(token: &OwnedHandle) -> io::Result<String> {
    let (buffer, byte_len) = token_information(token, TokenUser)?;
    if byte_len < size_of::<TOKEN_USER>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid token user buffer length",
        ));
    }
    let info = unsafe { std::ptr::read_unaligned(buffer.as_ptr().cast::<TOKEN_USER>()) };
    sid_to_string(info.User.Sid)
}

fn enabled_capability_sids(entries: &[SID_AND_ATTRIBUTES]) -> io::Result<Vec<String>> {
    entries
        .iter()
        .filter(|entry| {
            entry.Attributes & SE_GROUP_ENABLED != 0
                && entry.Attributes & SE_GROUP_USE_FOR_DENY_ONLY == 0
        })
        .map(|entry| sid_to_string(entry.Sid))
        .collect()
}

fn assemble_attribution(
    process_id: u32,
    user_sid: String,
    mut restricted_sids: Vec<String>,
    mut capability_sids: Vec<String>,
    app_container_sid: Option<String>,
) -> AttributedWindowsProcess {
    restricted_sids.sort();
    restricted_sids.dedup();
    capability_sids.sort();
    capability_sids.dedup();
    AttributedWindowsProcess {
        process_id,
        restricted_sids,
        capability_sids,
        user_sid,
        app_container_sid,
    }
}

fn parse_token_groups(buffer: &[usize], byte_len: usize) -> io::Result<&[SID_AND_ATTRIBUTES]> {
    let groups_offset = offset_of!(TOKEN_GROUPS, Groups);
    if byte_len > size_of_val(buffer) || byte_len < groups_offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid token SID group buffer length",
        ));
    }
    let group_count = unsafe { std::ptr::read_unaligned(buffer.as_ptr().cast::<u32>()) } as usize;
    let groups_byte_len = group_count
        .checked_mul(size_of::<SID_AND_ATTRIBUTES>())
        .and_then(|len| groups_offset.checked_add(len))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "token SID group buffer length overflow",
            )
        })?;
    if groups_byte_len > byte_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated token SID group buffer",
        ));
    }
    Ok(unsafe {
        let groups_ptr = buffer
            .as_ptr()
            .cast::<u8>()
            .add(groups_offset)
            .cast::<SID_AND_ATTRIBUTES>();
        std::slice::from_raw_parts(groups_ptr, group_count)
    })
}

pub(super) fn sid_to_string(sid: PSID) -> io::Result<String> {
    if sid.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "token SID is null",
        ));
    }
    let mut string_sid = std::ptr::null_mut();
    if unsafe { ConvertSidToStringSidW(sid, &mut string_sid) } == 0 {
        return Err(last_error("convert proxy client token SID to string"));
    }
    let value = unsafe {
        let mut len = 0;
        while *string_sid.add(len) != 0 {
            len += 1;
        }
        String::from_utf16_lossy(std::slice::from_raw_parts(string_sid, len))
    };
    unsafe {
        LocalFree(string_sid as HLOCAL);
    }
    Ok(value)
}

fn aligned_buffer(byte_len: usize) -> io::Result<Vec<usize>> {
    if byte_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows API returned an empty buffer length",
        ));
    }
    Ok(vec![0; byte_len.div_ceil(size_of::<usize>())])
}

fn owned_handle(handle: HANDLE, operation: &str) -> io::Result<OwnedHandle> {
    if handle.is_null() {
        return Err(last_error(operation));
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) })
}

fn win32_error(operation: &str, error_code: u32) -> io::Error {
    let error = io::Error::from_raw_os_error(error_code as i32);
    io::Error::new(error.kind(), format!("{operation}: {error}"))
}

fn last_error(operation: &str) -> io::Error {
    let error = io::Error::last_os_error();
    io::Error::new(error.kind(), format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;

    const ROUTE: &str = "S-1-15-3-1024-0-1-2-3-4-5-6-7";
    const PACKAGE: &str = "S-1-15-2-0-1-2-3-4-5-6";

    struct Sid(PSID);

    impl Sid {
        fn parse(text: &str) -> Self {
            let text: Vec<u16> = text.encode_utf16().chain([0]).collect();
            let mut raw = std::ptr::null_mut();
            assert_ne!(
                unsafe { ConvertStringSidToSidW(text.as_ptr(), &mut raw) },
                0
            );
            Self(raw)
        }
    }

    impl Drop for Sid {
        fn drop(&mut self) {
            unsafe { LocalFree(self.0 as HLOCAL) };
        }
    }

    #[test]
    fn capability_attribution_requires_enabled_and_rejects_deny_only() {
        let sid = Sid::parse(ROUTE);
        for attributes in [
            0,
            SE_GROUP_USE_FOR_DENY_ONLY,
            SE_GROUP_ENABLED | SE_GROUP_USE_FOR_DENY_ONLY,
        ] {
            let entries = [SID_AND_ATTRIBUTES {
                Sid: sid.0,
                Attributes: attributes,
            }];
            assert!(enabled_capability_sids(&entries)
                .expect("query attributes")
                .is_empty());
        }
        let entries = [SID_AND_ATTRIBUTES {
            Sid: sid.0,
            Attributes: SE_GROUP_ENABLED,
        }];
        assert_eq!(
            enabled_capability_sids(&entries).expect("enabled capability"),
            vec![ROUTE]
        );
    }

    #[test]
    fn restricted_or_package_sources_do_not_become_capability_evidence() {
        let attributed = assemble_attribution(
            42,
            "S-1-5-21-1-2-3-1001".into(),
            vec![ROUTE.into(), ROUTE.into()],
            vec![],
            Some(PACKAGE.into()),
        );
        assert!(attributed.capability_sids.is_empty());
        assert_eq!(attributed.restricted_sids, vec![ROUTE]);
        assert_eq!(attributed.app_container_sid.as_deref(), Some(PACKAGE));
        let attributed = assemble_attribution(42, "owner".into(), vec![], vec![ROUTE.into()], None);
        assert_eq!(attributed.capability_sids, vec![ROUTE]);
        assert!(attributed.restricted_sids.is_empty());
        assert!(attributed.app_container_sid.is_none());
    }

    #[test]
    fn malformed_token_group_lengths_fail_instead_of_exposing_entries() {
        assert!(parse_token_groups(&[], 0).is_err());
        let mut buffer = aligned_buffer(size_of::<TOKEN_GROUPS>()).expect("buffer");
        unsafe { std::ptr::write_unaligned(buffer.as_mut_ptr().cast::<u32>(), u32::MAX) };
        assert!(parse_token_groups(&buffer, size_of::<TOKEN_GROUPS>()).is_err());
        assert!(parse_token_groups(&buffer, size_of_val(&buffer[..]) + 1).is_err());
    }
}
