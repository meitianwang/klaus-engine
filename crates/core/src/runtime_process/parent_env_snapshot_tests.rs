// Gated with their only users: the re-exec, symbol-table and subprocess cases below are
// unix-only, so on Windows these are imports with nothing to name.
#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::process::{Command, Stdio};

use quote::ToTokens;
#[cfg(unix)]
use serde_json::json;
use sha2::{Digest, Sha256};
use syn::visit::{self, Visit};
use syn::{Expr, Lit, Meta};

use super::super::RuntimeEnv;
use super::*;
#[cfg(unix)]
use crate::runtime_values::NodeBufferUtf8;
use crate::runtime_values::RuntimeString;

fn logical_entries(environment: &RuntimeEnv) -> Vec<(Vec<u16>, Vec<u16>)> {
    environment
        .entries()
        .map(|(key, value)| (key.as_utf16().to_vec(), value.as_utf16().to_vec()))
        .collect()
}

fn scalar_entries(environment: &RuntimeEnv) -> Vec<(String, String)> {
    environment
        .entries()
        .map(|(key, value)| {
            (
                key.try_to_utf8().expect("scalar test key"),
                value.try_to_utf8().expect("scalar test value"),
            )
        })
        .collect()
}

#[test]
fn unix_raw_environ_uses_names_only_then_getter_and_object_entries_semantics() {
    let raw = [
        b"B=raw-b".to_vec(),
        b"NO_EQUALS".to_vec(),
        b"EQ=raw=enumeration".to_vec(),
        b"A=raw-a".to_vec(),
        b"A=raw-duplicate".to_vec(),
        b"MISS=raw-miss".to_vec(),
        b"01=near-leading-zero".to_vec(),
        b"2=index-two".to_vec(),
        b"10=index-ten".to_vec(),
        b"4294967294=max-index".to_vec(),
        b"4294967295=near-max".to_vec(),
    ];
    let mut getter_calls = Vec::new();
    let snapshot = ParentEnvSnapshot::from_unix_raw_entries(raw, |key| {
        getter_calls.push(key.to_vec());
        match key {
            b"B" => Some(b"getter-b".to_vec()),
            b"EQ" => Some(b"getter=a=b".to_vec()),
            b"A" => Some(b"getter-first".to_vec()),
            b"MISS" => None,
            b"01" => Some(b"getter-leading".to_vec()),
            b"4294967295" => Some(b"getter-near-max".to_vec()),
            other => panic!("indexed or malformed key reached getter: {other:?}"),
        }
    });

    assert_eq!(
        getter_calls,
        [
            b"B".as_slice(),
            b"EQ".as_slice(),
            b"A".as_slice(),
            b"MISS".as_slice(),
            b"01".as_slice(),
            b"4294967295".as_slice(),
        ],
    );
    assert_eq!(
        scalar_entries(&snapshot),
        [
            ("B".to_owned(), "getter-b".to_owned()),
            ("EQ".to_owned(), "getter=a=b".to_owned()),
            ("A".to_owned(), "getter-first".to_owned()),
            ("01".to_owned(), "getter-leading".to_owned()),
            ("4294967295".to_owned(), "getter-near-max".to_owned()),
        ],
    );
}

#[test]
fn unix_invalid_names_collide_after_v8_ingress_and_getter_projection() {
    let raw = [
        b"\xff=ignored-invalid".to_vec(),
        b"\xef\xbf\xbd=ignored-valid-replacement".to_vec(),
        b"\xe2\x82=ignored-incomplete".to_vec(),
        b"\xc0\xaf=ignored-overlong".to_vec(),
        b"\xef\xbf\xbd\xef\xbf\xbd=ignored-two-replacements".to_vec(),
    ];
    let mut getter_calls = Vec::new();
    let snapshot = ParentEnvSnapshot::from_unix_raw_entries(raw, |key| {
        getter_calls.push(key.to_vec());
        match key {
            b"\xef\xbf\xbd" => Some(b"value-\xff".to_vec()),
            b"\xef\xbf\xbd\xef\xbf\xbd" => Some(b"two".to_vec()),
            other => panic!("unexpected projected key: {other:?}"),
        }
    });

    assert_eq!(
        getter_calls,
        [
            b"\xef\xbf\xbd".as_slice(),
            b"\xef\xbf\xbd\xef\xbf\xbd".as_slice(),
        ],
        "duplicate logical keys are enumerated once before property Get",
    );
    assert_eq!(
        logical_entries(&snapshot),
        [
            (
                vec![0xfffd],
                RuntimeString::from_utf8("value-�").as_utf16().to_vec(),
            ),
            (vec![0xfffd, 0xfffd], "two".encode_utf16().collect()),
        ],
    );
}

#[test]
fn windows_libuv_entries_drop_hidden_names_and_delegate_case_aliases_to_getter() {
    let converted_entries = [
        b"=C:=C:\\workspace".to_vec(),
        b"Path=raw-one".to_vec(),
        b"PATH=raw-two".to_vec(),
        b"PaTh=raw-three".to_vec(),
        b"NO_EQUALS".to_vec(),
        b"2=index".to_vec(),
    ];
    let mut getter_calls = Vec::new();
    let snapshot = ParentEnvSnapshot::from_windows_libuv_utf8_entries(converted_entries, |key| {
        getter_calls.push(key.to_vec());
        match key {
            b"Path" | b"PATH" | b"PaTh" => Some(b"host-case-value".to_vec()),
            other => panic!("hidden/index key reached Windows getter: {other:?}"),
        }
    });

    assert_eq!(
        getter_calls,
        [b"Path".as_slice(), b"PATH".as_slice(), b"PaTh".as_slice()],
    );
    assert_eq!(
        scalar_entries(&snapshot),
        [
            ("Path".to_owned(), "host-case-value".to_owned()),
            ("PATH".to_owned(), "host-case-value".to_owned()),
            ("PaTh".to_owned(), "host-case-value".to_owned()),
        ],
    );
}

#[cfg(unix)]
#[test]
fn actual_unix_parent_capture_smoke_uses_the_host_getter_value() {
    let snapshot = ParentEnvSnapshot::capture();
    let name = RuntimeString::from_utf8("PATH");
    let Some(expected) = unix_getenv_for_test(b"PATH") else {
        return;
    };
    assert_eq!(
        snapshot.get(&name),
        Some(&NodeBufferUtf8::decode(&expected)),
    );
}

#[test]
fn windows_null_environment_block_fails_closed_like_node_check_eq() {
    let panic =
        std::panic::catch_unwind(|| require_windows_environment_block(std::ptr::null_mut()))
            .expect_err("GetEnvironmentStringsW NULL must be fatal");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(message.contains("CHECK_EQ(uv_os_environ(&items, &count), 0)"));
}

#[test]
fn parent_capture_and_every_repository_mutation_use_the_shared_environment_lock() {
    let capture_source = include_str!("parent_env_snapshot.rs");
    assert!(
        capture_source.contains("with_process_environment_lock"),
        "ParentEnvSnapshot::capture must take the repository-wide environment lock",
    );

    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    for (path, symbol) in SANDBOX_DYNAMIC_SYMBOLS {
        let source = std::fs::read_to_string(workspace.join(path)).unwrap();
        audit_fixed_windows_dynamic_lookup(&source, symbol)
            .unwrap_or_else(|error| panic!("{path}: {error}"));
    }
    let conpty_source =
        std::fs::read_to_string(workspace.join("crates/sandbox-win/src/elevated/conpty.rs"))
            .unwrap();
    audit_fixed_conpty_dynamic_lookup(&conpty_source).unwrap();
    let mut raw_mutations = Vec::new();
    collect_raw_environment_mutations(workspace, workspace, &mut raw_mutations);
    assert_eq!(
        raw_mutations,
        [
            // The browser pipe addon resolves one closed, mutation-tested list
            // of Node-API/libuv symbols. The companion dynamic-loader audit
            // below proves that this capability cannot name an environment
            // accessor or mutator and cannot escape as a function reference.
            "crates/browser-pipe-napi/src/lib.rs:GetProcAddress",
            "crates/browser-pipe-napi/src/lib.rs:GetProcAddress",
            // The first accessor is a post-fork child-only Node environment write;
            // the second is Rust's native-parent pointer read while the shared
            // environment lock is held. The dlsym call resolves addchdir_np, not
            // an environment accessor.
            "crates/core/src/runtime_process/macos_posix_spawn.rs:accessor=_NSGetEnviron",
            "crates/core/src/runtime_process/macos_posix_spawn.rs:accessor=_NSGetEnviron",
            "crates/core/src/runtime_process/macos_posix_spawn.rs:dlsym",
            "crates/core/src/runtime_process/parent_env_snapshot.rs:accessor=_NSGetEnviron",
            "crates/core/src/runtime_process/parent_env_snapshot.rs:accessor=unix_environ",
            "crates/core/src/runtime_process/parent_env_snapshot.rs:environ",
            "crates/core/src/runtime_process/parent_env_snapshot.rs:environ",
            "crates/core/src/runtime_process/process_environment.rs:remove_var",
            "crates/core/src/runtime_process/process_environment.rs:set_var",
            "crates/core/src/runtime_process/unix.rs:accessor=_NSGetEnviron",
            "crates/core/src/runtime_process/unix.rs:accessor=parent_environ",
            "crates/core/src/runtime_process/unix.rs:environ",
            "crates/core/src/runtime_process/unix.rs:environ",
            // These three writes are confined to the post-fork child before exec;
            // the parent process still enters every mutation through the shared lock.
            "crates/core/src/runtime_process/unix_spawn.rs:accessor=_NSGetEnviron",
            "crates/core/src/runtime_process/unix_spawn.rs:environ",
            "crates/core/src/runtime_process/unix_spawn.rs:environ",
            // The fresh, single-threaded namespace helper rewrites proxy values
            // in run_inner before exec; it never enters the engine runtime.
            "crates/sandbox-linux-bwrap/src/helper/proxy_routing.rs:set_var",
            // Each sandbox loader names one fixed non-environment Ntdll entry.
            // The companion audit below rejects dynamic symbols and escapes.
            "crates/sandbox-win/src/access.rs:GetProcAddress",
            "crates/sandbox-win/src/access.rs:GetProcAddress",
            "crates/sandbox-win/src/acl_object.rs:GetProcAddress",
            "crates/sandbox-win/src/acl_object.rs:GetProcAddress",
            // One import plus the three fixed pseudoconsole entry points. The
            // closed lookup audit rejects other symbols and escaped references.
            "crates/sandbox-win/src/elevated/conpty.rs:GetProcAddress",
            "crates/sandbox-win/src/elevated/conpty.rs:GetProcAddress",
            "crates/sandbox-win/src/elevated/conpty.rs:GetProcAddress",
            "crates/sandbox-win/src/elevated/conpty.rs:GetProcAddress",
        ],
        "all repository environment mutations must enter through the one shared lock",
    );
}

const SANDBOX_DYNAMIC_SYMBOLS: &[(&str, &str)] = &[
    ("crates/sandbox-win/src/access.rs", "NtCreateLowBoxToken"),
    (
        "crates/sandbox-win/src/acl_object.rs",
        "NtSetSecurityObject",
    ),
];

#[test]
fn sandbox_dynamic_lookup_is_closed_and_mutation_tested() {
    for (source, symbol) in [
        (
            include_str!("../../../sandbox-win/src/access.rs"),
            "NtCreateLowBoxToken",
        ),
        (
            include_str!("../../../sandbox-win/src/acl_object.rs"),
            "NtSetSecurityObject",
        ),
    ] {
        audit_fixed_windows_dynamic_lookup(source, symbol).unwrap();
        let literal = format!("c\"{symbol}\".as_ptr().cast()");
        for replacement in [
            "c\"SetEnvironmentVariableW\".as_ptr().cast()",
            "c\"GetEnvironmentStringsW\".as_ptr().cast()",
            "dynamic_name.as_ptr()",
            "concat!(\"SetEnvironment\", \"VariableW\\0\").as_ptr()",
        ] {
            let mutated = source.replacen(&literal, replacement, 1);
            assert_ne!(source, mutated);
            assert!(
                audit_fixed_windows_dynamic_lookup(&mutated, symbol).is_err(),
                "accepted a non-fixed symbol: {replacement}",
            );
        }
        for replacement in [
            "let _escaped = GetProcAddress; let entry = unsafe",
            "let _extra = unsafe { GetProcAddress(module, dynamic_name) }; let entry = unsafe",
        ] {
            let mutated = source.replacen("let entry = unsafe", replacement, 1);
            assert_ne!(source, mutated);
            assert!(audit_fixed_windows_dynamic_lookup(&mutated, symbol).is_err());
        }
        for (before, after) in [
            (
                "windows_sys::Win32::System::LibraryLoader",
                "untrusted_loader",
            ),
            ("GetProcAddress};", "GetProcAddress as lookup};"),
            ("GetProcAddress(module,", "lookup(module,"),
        ] {
            let mutated = source.replacen(before, after, 1);
            assert_ne!(source, mutated);
            assert!(audit_fixed_windows_dynamic_lookup(&mutated, symbol).is_err());
        }
    }
}

fn audit_fixed_windows_dynamic_lookup(source: &str, symbol: &str) -> Result<(), String> {
    let syntax = syn::parse_file(source).map_err(|error| error.to_string())?;
    let tokens = syntax.to_token_stream();
    if count_ident_tokens(&tokens, "GetProcAddress") != 2 {
        return Err("GetProcAddress must have only its import and one fixed call".to_owned());
    }
    let expected_import: syn::ItemUse = syn::parse_str(
        "use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};",
    )
    .unwrap();
    let expected_call: syn::ExprCall = syn::parse_str(&format!(
        "GetProcAddress(module, c\"{symbol}\".as_ptr().cast())"
    ))
    .unwrap();
    let mut audit = FixedWindowsDynamicLookup {
        expected_import: expected_import.to_token_stream().to_string(),
        expected_call: expected_call.to_token_stream().to_string(),
        imports: 0,
        calls: 0,
    };
    audit.visit_file(&syntax);
    if (audit.imports, audit.calls) != (1, 1) {
        return Err(format!(
            "fixed {symbol} lookup drift: imports={}, calls={}",
            audit.imports, audit.calls,
        ));
    }
    Ok(())
}

struct FixedWindowsDynamicLookup {
    expected_import: String,
    expected_call: String,
    imports: usize,
    calls: usize,
}

impl<'ast> Visit<'ast> for FixedWindowsDynamicLookup {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        self.imports += usize::from(item.to_token_stream().to_string() == self.expected_import);
        visit::visit_item_use(self, item);
    }

    fn visit_expr_call(&mut self, expression: &'ast syn::ExprCall) {
        self.calls += usize::from(expression.to_token_stream().to_string() == self.expected_call);
        visit::visit_expr_call(self, expression);
    }
}

const CONPTY_DYNAMIC_SYMBOLS: &[&str] = &[
    "CreatePseudoConsole",
    "ResizePseudoConsole",
    "ClosePseudoConsole",
];

fn audit_fixed_conpty_dynamic_lookup(source: &str) -> Result<(), String> {
    let syntax = syn::parse_file(source).map_err(|error| error.to_string())?;
    if count_ident_tokens(&syntax.to_token_stream(), "GetProcAddress")
        != CONPTY_DYNAMIC_SYMBOLS.len() + 1
    {
        return Err(
            "GetProcAddress must have only its import and three fixed console calls".into(),
        );
    }
    let expected_import: syn::ItemUse =
        syn::parse_str("use native_windows_sys::Win32::System::LibraryLoader::GetProcAddress;")
            .unwrap();
    for symbol in CONPTY_DYNAMIC_SYMBOLS {
        let expected_call: syn::ExprCall = syn::parse_str(&format!(
            "GetProcAddress(module, c\"{symbol}\".as_ptr().cast())"
        ))
        .unwrap();
        let mut audit = FixedWindowsDynamicLookup {
            expected_import: expected_import.to_token_stream().to_string(),
            expected_call: expected_call.to_token_stream().to_string(),
            imports: 0,
            calls: 0,
        };
        audit.visit_file(&syntax);
        if (audit.imports, audit.calls) != (1, 1) {
            return Err(format!(
                "fixed {symbol} lookup drift: imports={}, calls={}",
                audit.imports, audit.calls,
            ));
        }
    }
    Ok(())
}

#[test]
fn conpty_dynamic_lookup_is_closed_and_mutation_tested() {
    let source = include_str!("../../../sandbox-win/src/elevated/conpty.rs");
    audit_fixed_conpty_dynamic_lookup(source).unwrap();
    for symbol in CONPTY_DYNAMIC_SYMBOLS {
        let literal = format!("c\"{symbol}\".as_ptr().cast()");
        for replacement in [
            "c\"SetEnvironmentVariableW\".as_ptr().cast()",
            "c\"GetEnvironmentStringsW\".as_ptr().cast()",
            "dynamic_name.as_ptr()",
            "concat!(\"SetEnvironment\", \"VariableW\\0\").as_ptr()",
        ] {
            let mutated = source.replacen(&literal, replacement, 1);
            assert_ne!(source, mutated);
            assert!(audit_fixed_conpty_dynamic_lookup(&mutated).is_err());
        }
    }
    for (before, after) in [
        (
            "native_windows_sys::Win32::System::LibraryLoader",
            "untrusted_loader",
        ),
        (
            "LibraryLoader::GetProcAddress;",
            "LibraryLoader::GetProcAddress as lookup;",
        ),
        ("GetProcAddress(module,", "lookup(module,"),
    ] {
        let mutated = source.replacen(before, after, 1);
        assert_ne!(source, mutated);
        assert!(audit_fixed_conpty_dynamic_lookup(&mutated).is_err());
    }
    for extra in [
        "fn escaped_loader() { let _loader = GetProcAddress; }",
        "fn extra_lookup() { unsafe { GetProcAddress(module, dynamic_name) }; }",
    ] {
        assert!(audit_fixed_conpty_dynamic_lookup(&format!("{source}\n{extra}")).is_err());
    }
}

const BROWSER_PIPE_DYNAMIC_SYMBOLS: &[&str] = &[
    "napi_add_env_cleanup_hook",
    "napi_async_destroy",
    "napi_async_init",
    "napi_close_handle_scope",
    "napi_create_arraybuffer",
    "napi_create_bigint_uint64",
    "napi_create_error",
    "napi_create_object",
    "napi_create_reference",
    "napi_create_string_utf8",
    "napi_create_typedarray",
    "napi_create_uint32",
    "napi_define_properties",
    "napi_delete_reference",
    "napi_get_all_property_names",
    "napi_get_array_length",
    "napi_get_cb_info",
    "napi_get_element",
    "napi_get_global",
    "napi_get_named_property",
    "napi_get_reference_value",
    "napi_get_typedarray_info",
    "napi_get_undefined",
    "napi_get_uv_event_loop",
    "napi_get_value_double",
    "napi_get_value_string_utf8",
    "napi_make_callback",
    "napi_object_freeze",
    "napi_open_handle_scope",
    "napi_throw_error",
    "napi_typeof",
    "napi_unwrap",
    "napi_wrap",
    "uv_async_init",
    "uv_async_send",
    "uv_close",
    "uv_handle_get_data",
    "uv_handle_set_data",
    "uv_handle_size",
];

#[test]
fn browser_pipe_napi_dynamic_lookup_is_closed_and_mutation_tested() {
    let source = include_str!("../../../browser-pipe-napi/src/lib.rs");
    assert_eq!(
        audit_browser_pipe_dynamic_loader(source).unwrap(),
        BROWSER_PIPE_DYNAMIC_SYMBOLS,
    );

    let dynamic_argument = source.replacen(
        "resolve_symbol(b\"napi_define_properties\\0\")",
        "resolve_symbol(dynamic_symbol_name)",
        1,
    );
    assert!(audit_browser_pipe_dynamic_loader(&dynamic_argument)
        .unwrap_err()
        .contains("byte-string literal"),);

    let environment_mutator = source.replacen(
        "b\"napi_define_properties\\0\"",
        "b\"SetEnvironmentVariableW\\0\"",
        1,
    );
    assert!(audit_browser_pipe_dynamic_loader(&environment_mutator)
        .unwrap_err()
        .contains("dynamic symbol set drift"),);

    let extra_get_proc_address = source.replacen(
        "let symbol = unsafe { GetProcAddress(module, name.as_ptr()) }?;",
        "let _extra = unsafe { GetProcAddress(module, name.as_ptr()) };\n        let symbol = unsafe { GetProcAddress(module, name.as_ptr()) }?;",
        1,
    );
    assert!(audit_browser_pipe_dynamic_loader(&extra_get_proc_address)
        .unwrap_err()
        .contains("GetProcAddress"),);

    let escaped_resolver = source.replacen(
        "unsafe fn resolve_symbol<T: Copy>(name: &[u8]) -> Option<T> {",
        "unsafe fn resolve_symbol<T: Copy>(name: &[u8]) -> Option<T> {\n        let _escaped = resolve_symbol::<usize>;",
        1,
    );
    assert!(audit_browser_pipe_dynamic_loader(&escaped_resolver)
        .unwrap_err()
        .contains("resolver reference escaped"),);
}

#[test]
fn sandbox_dynamic_lookups_only_resolve_fixed_non_environment_symbols() {
    for (source, symbol) in [
        (
            include_str!("../../../sandbox-win/src/access.rs"),
            "NtCreateLowBoxToken",
        ),
        (
            include_str!("../../../sandbox-win/src/acl_object.rs"),
            "NtSetSecurityObject",
        ),
    ] {
        assert!(audit_fixed_sandbox_lookup(source, symbol));
        assert!(!audit_fixed_sandbox_lookup(
            &source.replace(symbol, "SetEnvironmentVariableW"),
            symbol,
        ));
        assert!(!audit_fixed_sandbox_lookup(
            &source.replace(&format!("c\"{symbol}\""), "dynamic_symbol"),
            symbol,
        ));
        assert!(!audit_fixed_sandbox_lookup(
            &format!("{source}\nfn escaped_loader() {{ let _loader = GetProcAddress; }}"),
            symbol,
        ));
    }
}

fn audit_fixed_sandbox_lookup(source: &str, symbol: &str) -> bool {
    let tokens = source.parse::<proc_macro2::TokenStream>().unwrap();
    if count_ident_tokens(&tokens, "GetProcAddress") != 2 {
        return false;
    }
    struct Calls(Vec<String>);
    impl<'ast> Visit<'ast> for Calls {
        fn visit_expr_call(&mut self, expression: &'ast syn::ExprCall) {
            if matches!(expression.func.as_ref(), Expr::Path(path)
                if path.path.is_ident("GetProcAddress"))
            {
                self.0.push(expression.to_token_stream().to_string());
            }
            visit::visit_expr_call(self, expression);
        }
    }
    let mut calls = Calls(Vec::new());
    calls.visit_file(&syn::parse_file(source).unwrap());
    let expected: syn::Expr = syn::parse_str(&format!(
        "GetProcAddress(module, c\"{symbol}\".as_ptr().cast())",
    ))
    .unwrap();
    calls.0 == [expected.to_token_stream().to_string()]
}

#[test]
fn parent_capture_surface_is_crate_private() {
    let syntax = syn::parse_file(include_str!("parent_env_snapshot.rs")).unwrap();
    let snapshot = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Struct(item) if item.ident == "ParentEnvSnapshot" => Some(item),
            _ => None,
        })
        .expect("ParentEnvSnapshot struct");
    assert!(matches!(snapshot.vis, syn::Visibility::Restricted(_)));

    let capture = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Impl(item) => item.items.iter().find_map(|item| match item {
                syn::ImplItem::Fn(item) if item.sig.ident == "capture" => Some(item),
                _ => None,
            }),
            _ => None,
        })
        .expect("ParentEnvSnapshot::capture");
    assert!(matches!(capture.vis, syn::Visibility::Restricted(_)));
}

#[test]
fn native_environment_vector_readers_are_sealed() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let mut readers = Vec::new();
    for relative in [
        "crates/core/src/runtime_process/parent_env_snapshot.rs",
        "crates/core/src/runtime_process/unix.rs",
    ] {
        let source = std::fs::read_to_string(workspace.join(relative)).unwrap();
        let syntax = syn::parse_file(&source).unwrap();
        for item in syntax.items {
            let syn::Item::Fn(function) = item else {
                continue;
            };
            if !matches!(
                function.sig.ident.to_string().as_str(),
                "unix_raw_environ_entries"
                    | "unix_environ"
                    | "read_parent_environ"
                    | "parent_environ"
            ) {
                continue;
            }
            readers.push((
                format!("{relative}:{}", function.sig.ident),
                hex_sha256(function.to_token_stream().to_string().as_bytes()),
            ));
        }
    }
    assert_eq!(
        readers,
        [
            (
                "crates/core/src/runtime_process/parent_env_snapshot.rs:unix_raw_environ_entries"
                    .to_owned(),
                "68a6b34ce0e79757dd75d4dd07ce891ff0cfde97249792035b2ae41d385903d4".to_owned(),
            ),
            (
                "crates/core/src/runtime_process/parent_env_snapshot.rs:unix_environ".to_owned(),
                "b64229880e709103d69f48933cf538a05b6500db6e862c7c3d4f9e76a42e62fc".to_owned(),
            ),
            (
                "crates/core/src/runtime_process/parent_env_snapshot.rs:unix_environ".to_owned(),
                "c3a7849a363919dfeaeeca9317c37d02c27022f3c35f49af0127f324b812b696".to_owned(),
            ),
            (
                "crates/core/src/runtime_process/unix.rs:read_parent_environ".to_owned(),
                "cae7da897d8f3f83466af629e1723fcd611175e073dab4eec64d8c02a0ee7b7f".to_owned(),
            ),
            (
                "crates/core/src/runtime_process/unix.rs:parent_environ".to_owned(),
                "d6a322f104978eb6a14df80fb3fc997bf7164e2029cfda437a055029201241b8".to_owned(),
            ),
            (
                "crates/core/src/runtime_process/unix.rs:parent_environ".to_owned(),
                "4d92eeafa9a6aeffc75aca2c1d009db363142d6fd43c80a21c7a933cd7355793".to_owned(),
            ),
        ],
        "the only native environment-vector readers must remain exact read-only shims",
    );
}

#[test]
fn capture_and_mutation_lock_adapters_are_sealed() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let process_source = std::fs::read_to_string(
        workspace.join("crates/core/src/runtime_process/process_environment.rs"),
    )
    .unwrap();
    let process_syntax = syn::parse_file(&process_source).unwrap();
    let mut adapters = process_syntax
        .items
        .iter()
        .filter_map(|item| {
            let syn::Item::Fn(function) = item else {
                return None;
            };
            matches!(
                function.sig.ident.to_string().as_str(),
                "with_process_environment_lock"
                    | "set_process_environment_var"
                    | "set_process_environment_var_unlocked"
                    | "remove_process_environment_var"
                    | "remove_process_environment_var_unlocked"
            )
            .then(|| {
                (
                    function.sig.ident.to_string(),
                    hex_sha256(function.to_token_stream().to_string().as_bytes()),
                )
            })
        })
        .collect::<Vec<_>>();
    adapters.sort();

    let capture_source = std::fs::read_to_string(
        workspace.join("crates/core/src/runtime_process/parent_env_snapshot.rs"),
    )
    .unwrap();
    let capture_syntax = syn::parse_file(&capture_source).unwrap();
    let capture = capture_syntax
        .items
        .iter()
        .find_map(|item| {
            let syn::Item::Impl(item) = item else {
                return None;
            };
            item.items.iter().find_map(|item| match item {
                syn::ImplItem::Fn(function) if function.sig.ident == "capture" => Some(function),
                _ => None,
            })
        })
        .expect("ParentEnvSnapshot::capture");
    assert_eq!(
        (
            adapters,
            hex_sha256(capture.to_token_stream().to_string().as_bytes()),
        ),
        (
            vec![
                (
                    "remove_process_environment_var".to_owned(),
                    "a76d29d191507b823e9204465ada8cc7321d0a9941c19f8cefd6265f61e3b01c".to_owned(),
                ),
                (
                    "remove_process_environment_var_unlocked".to_owned(),
                    "15c00c21ab3a7389f79a271c186318fc588acd02f8d4021e29bc67ff82a49039".to_owned(),
                ),
                (
                    "set_process_environment_var".to_owned(),
                    "0a3d052a8f233b30cc4a4b85255ca910169bfd0cbaab356da51c5412febea07e".to_owned(),
                ),
                (
                    "set_process_environment_var_unlocked".to_owned(),
                    "6563596b5adfee56c4dd4c99d12ac2511cd304394d9176d37b84b2ec0eac5541".to_owned(),
                ),
                (
                    "with_process_environment_lock".to_owned(),
                    "6539e571eb9b228445530caf1e3cea87379c0b5a7fc5f8769be69eed7d54d0ed".to_owned(),
                ),
            ],
            "e269fcdcdee553d829a7e16242291d314c4bdfe792fd037d28065760e213e2d1".to_owned(),
        ),
        "capture, lock, and the only mutation adapters must remain one sealed transaction",
    );
}

#[cfg(unix)]
#[test]
fn compiled_core_artifact_has_only_the_locked_environment_mutation_edges() {
    let test_executable = std::env::current_exe().expect("current core unit-test executable");
    let deps = test_executable
        .parent()
        .expect("Cargo unit-test deps directory");
    let artifact = std::fs::read_dir(deps)
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("libklaus_agent_core-") && name.ends_with(".rlib")
            })
        })
        .max_by_key(|entry| {
            entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        })
        .expect("current klaus-agent-core rlib");
    let output = Command::new("nm")
        .arg("-u")
        .arg(artifact.path())
        .output()
        .expect("nm must be available for the environment mutation symbol audit");
    assert!(
        output.status.success(),
        "nm environment mutation audit failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let symbol_output = String::from_utf8(output.stdout).unwrap();
    let symbols = symbol_output
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .collect::<Vec<_>>();
    let direct_native = symbols
        .iter()
        .copied()
        .filter(|symbol| {
            let symbol = symbol.trim_start_matches('_');
            let symbol = symbol.split_once('@').map_or(symbol, |(name, _)| name);
            matches!(symbol, "setenv" | "unsetenv" | "putenv" | "clearenv")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        direct_native,
        Vec::<&str>::new(),
        "compiled code contains a direct native environment mutation edge",
    );
    let std_set_edges = symbols
        .iter()
        .filter(|symbol| symbol.contains("3std3sys3env4unix6setenv17h"))
        .count();
    let std_remove_edges = symbols
        .iter()
        .filter(|symbol| symbol.contains("3std3sys3env4unix8unsetenv17h"))
        .count();
    assert_eq!(
        (std_set_edges, std_remove_edges),
        (1, 1),
        "generated code added a standard-library mutation edge outside the sealed wrappers",
    );
}

#[test]
fn environment_mutation_token_guard_rejects_native_api_bypasses() {
    let std_set = ["set", "_var"].concat();
    let std_remove = ["remove", "_var"].concat();
    let unix_set = ["set", "env"].concat();
    let unix_unset = ["unset", "env"].concat();
    let unix_put = ["put", "env"].concat();
    let unix_clear = ["clear", "env"].concat();
    let windows_set_wide = ["SetEnvironment", "VariableW"].concat();
    let windows_set_ansi = ["SetEnvironment", "VariableA"].concat();
    let crt_put = ["_put", "env"].concat();
    let crt_wide_put = ["_wput", "env"].concat();
    let crt_put_s = ["_put", "env_s"].concat();
    let crt_wide_put_s = ["_wput", "env_s"].concat();
    let unix_vector = ["envi", "ron"].concat();
    let windows_vector = ["_wenvi", "ron"].concat();
    let dynamic_unix = ["dl", "sym"].concat();
    let dynamic_windows = ["GetProc", "Address"].concat();
    let dynamic_crate = ["lib", "loading"].concat();
    let pasted_identifier = ["pa", "ste"].concat();
    let symbol_literal = ["set", "env"].concat();
    let unix_accessor = ["unix_", "environ"].concat();
    let parent_accessor = ["parent_", "environ"].concat();
    let mac_accessor = ["_NSGet", "Environ"].concat();
    let linux_accessor = ["__environ", "_location"].concat();
    let source = r#"
        unsafe extern "C" {
            #[link_name = "LINK_TARGET"]
            fn linked(name: *const i8, value: *const i8, replace: i32) -> i32;
            #[link_name = concat!("dl", "sym")]
            fn generated_lookup(handle: *mut u8, symbol: *const i8) -> *mut u8;
        }
        fn mutate() {
            std::env::STD_SET("A", "B");
            std::env::STD_REMOVE("A");
            unsafe { UNIX_SET(a, b, 1); UNIX_UNSET(a); UNIX_PUT(a); UNIX_CLEAR(); }
            unsafe { WINDOWS_SET_WIDE(name, value); WINDOWS_SET_ANSI(name, value); }
            unsafe { CRT_PUT(value); CRT_WIDE_PUT(value); CRT_PUT_S(name, value); CRT_WIDE_PUT_S(name, value); }
            unsafe { UNIX_VECTOR = value; WINDOWS_VECTOR = value; }
            let _ = unsafe { DYNAMIC_UNIX(handle, symbol) };
            let _ = unsafe { DYNAMIC_WINDOWS(handle, symbol) };
            let _ = DYNAMIC_CRATE::Library::new(path);
            PASTED_IDENTIFIER! { [<set env>](name, value, 1); }
            let _symbol = "SYMBOL_LITERAL";
            unsafe { UNIX_ACCESSOR(); PARENT_ACCESSOR(); MAC_ACCESSOR(); LINUX_ACCESSOR(); }
        }
    "#
    .replace("STD_SET", &std_set)
    .replace("STD_REMOVE", &std_remove)
    .replace("UNIX_SET", &unix_set)
    .replace("UNIX_UNSET", &unix_unset)
    .replace("UNIX_PUT", &unix_put)
    .replace("UNIX_CLEAR", &unix_clear)
    .replace("WINDOWS_SET_WIDE", &windows_set_wide)
    .replace("WINDOWS_SET_ANSI", &windows_set_ansi)
    .replace("CRT_WIDE_PUT_S", &crt_wide_put_s)
    .replace("CRT_WIDE_PUT", &crt_wide_put)
    .replace("CRT_PUT_S", &crt_put_s)
    .replace("CRT_PUT", &crt_put)
    .replace("UNIX_VECTOR", &unix_vector)
    .replace("WINDOWS_VECTOR", &windows_vector)
    .replace("DYNAMIC_UNIX", &dynamic_unix)
    .replace("DYNAMIC_WINDOWS", &dynamic_windows)
    .replace("DYNAMIC_CRATE", &dynamic_crate)
    .replace("PASTED_IDENTIFIER", &pasted_identifier)
    .replace("SYMBOL_LITERAL", &symbol_literal)
    .replace("UNIX_ACCESSOR", &unix_accessor)
    .replace("PARENT_ACCESSOR", &parent_accessor)
    .replace("MAC_ACCESSOR", &mac_accessor)
    .replace("LINUX_ACCESSOR", &linux_accessor)
    .replace("LINK_TARGET", &unix_set);
    let mut findings = Vec::new();
    collect_environment_mutations(&source, "synthetic.rs", &mut findings);
    findings.sort();
    assert_eq!(
        findings,
        [
            "synthetic.rs:GetProcAddress",
            "synthetic.rs:SetEnvironmentVariableA",
            "synthetic.rs:SetEnvironmentVariableW",
            "synthetic.rs:__environ_location",
            "synthetic.rs:_putenv",
            "synthetic.rs:_putenv_s",
            "synthetic.rs:_wenviron",
            "synthetic.rs:_wputenv",
            "synthetic.rs:_wputenv_s",
            "synthetic.rs:accessor=_NSGetEnviron",
            "synthetic.rs:accessor=parent_environ",
            "synthetic.rs:accessor=unix_environ",
            "synthetic.rs:clearenv",
            "synthetic.rs:dlsym",
            "synthetic.rs:dynamic_symbol=setenv",
            "synthetic.rs:environ",
            "synthetic.rs:libloading",
            "synthetic.rs:link_name=<non-literal>",
            "synthetic.rs:link_name=setenv",
            "synthetic.rs:paste",
            "synthetic.rs:putenv",
            "synthetic.rs:remove_var",
            "synthetic.rs:set_var",
            "synthetic.rs:setenv",
            "synthetic.rs:unsetenv",
        ],
    );
}

fn collect_raw_environment_mutations(
    workspace: &std::path::Path,
    directory: &std::path::Path,
    findings: &mut Vec<String>,
) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if matches!(name.to_str(), Some(".git" | "target" | "rust_out")) {
                continue;
            }
            collect_raw_environment_mutations(workspace, &path, findings);
            continue;
        }
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        // Forward slashes on every host: the expected findings below are written with
        // `/`, and `Path::display()` yields `\` on Windows, which made every key
        // mismatch and the whole audit fail there.
        let relative = path
            .strip_prefix(workspace)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        collect_environment_mutations(&source, &relative, findings);
    }
    findings.sort();
}

fn collect_environment_mutations(source: &str, path: &str, findings: &mut Vec<String>) {
    let tokens = source.parse::<proc_macro2::TokenStream>().unwrap();
    collect_environment_mutation_tokens(&tokens, path, findings);
    let syntax = syn::parse_file(source)
        .unwrap_or_else(|error| panic!("cannot audit Rust source {path}: {error}"));
    let mut visitor = EnvironmentLinkNameMutations {
        path,
        findings,
        inspect_symbol_literals: !path.ends_with("parent_env_snapshot_tests.rs"),
    };
    visitor.visit_file(&syntax);
}

fn audit_browser_pipe_dynamic_loader(source: &str) -> Result<Vec<&'static str>, String> {
    let tokens = source
        .parse::<proc_macro2::TokenStream>()
        .map_err(|error| format!("browser pipe token parse failed: {error}"))?;
    let get_proc_address_tokens = count_ident_tokens(&tokens, "GetProcAddress");
    if get_proc_address_tokens != 2 {
        return Err(format!(
            "GetProcAddress token drift: expected import plus one call, got {get_proc_address_tokens}",
        ));
    }

    let resolver_tokens = count_ident_tokens(&tokens, "resolve_symbol");
    let syntax = syn::parse_file(source)
        .map_err(|error| format!("browser pipe syntax parse failed: {error}"))?;
    let mut audit = BrowserPipeDynamicLoaderAudit::default();
    audit.visit_file(&syntax);
    if audit.get_proc_address_calls != 1 {
        return Err(format!(
            "GetProcAddress call drift: expected 1, got {}",
            audit.get_proc_address_calls,
        ));
    }
    if audit.resolver_definitions != 1 || !audit.resolver_definition_is_private_unsafe {
        return Err(
            "resolve_symbol definition must remain one private unsafe function".to_string(),
        );
    }
    if audit.symbols.contains(&"<non-byte-string literal>") {
        return Err(
            "resolve_symbol argument must remain one NUL-terminated byte-string literal"
                .to_string(),
        );
    }
    if audit.symbols.contains(&"<unknown dynamic symbol>") {
        return Err("browser pipe dynamic symbol set drift".to_string());
    }
    if audit.resolver_paths != audit.symbols.len()
        || resolver_tokens != audit.symbols.len() + audit.resolver_definitions
    {
        return Err(format!(
            "resolver reference escaped its literal callsites: paths={}, calls={}, tokens={}, definitions={}",
            audit.resolver_paths,
            audit.symbols.len(),
            resolver_tokens,
            audit.resolver_definitions,
        ));
    }
    let mut symbols = audit.symbols;
    symbols.sort_unstable();
    if symbols != BROWSER_PIPE_DYNAMIC_SYMBOLS {
        return Err(format!(
            "browser pipe dynamic symbol set drift: {symbols:?}"
        ));
    }
    Ok(BROWSER_PIPE_DYNAMIC_SYMBOLS.to_vec())
}

fn count_ident_tokens(tokens: &proc_macro2::TokenStream, expected: &str) -> usize {
    tokens
        .clone()
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Ident(ident) => usize::from(ident == expected),
            proc_macro2::TokenTree::Group(group) => count_ident_tokens(&group.stream(), expected),
            _ => 0,
        })
        .sum()
}

#[derive(Default)]
struct BrowserPipeDynamicLoaderAudit {
    get_proc_address_calls: usize,
    resolver_definitions: usize,
    resolver_definition_is_private_unsafe: bool,
    resolver_paths: usize,
    symbols: Vec<&'static str>,
}

impl<'ast> Visit<'ast> for BrowserPipeDynamicLoaderAudit {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if item.sig.ident == "resolve_symbol" {
            self.resolver_definitions += 1;
            self.resolver_definition_is_private_unsafe =
                matches!(item.vis, syn::Visibility::Inherited) && item.sig.unsafety.is_some();
        }
        visit::visit_item_fn(self, item);
    }

    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        if expression
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "resolve_symbol")
        {
            self.resolver_paths += 1;
        }
        visit::visit_expr_path(self, expression);
    }

    fn visit_expr_call(&mut self, expression: &'ast syn::ExprCall) {
        let callee = match expression.func.as_ref() {
            Expr::Path(path) => path.path.segments.last().map(|segment| &segment.ident),
            _ => None,
        };
        if callee.is_some_and(|ident| ident == "GetProcAddress") {
            self.get_proc_address_calls += 1;
        }
        if callee.is_some_and(|ident| ident == "resolve_symbol") {
            let symbol = expression
                .args
                .first()
                .and_then(|argument| match argument {
                    Expr::Lit(literal) => match &literal.lit {
                        Lit::ByteStr(value) => Some(value.value()),
                        _ => None,
                    },
                    _ => None,
                })
                .ok_or(())
                .and_then(|mut bytes| {
                    (bytes.pop() == Some(0) && !bytes.contains(&0))
                        .then_some(bytes)
                        .ok_or(())
                })
                .and_then(|bytes| String::from_utf8(bytes).map_err(|_| ()))
                .ok();
            let Some(symbol) = symbol else {
                self.symbols.clear();
                self.symbols.push("<non-byte-string literal>");
                visit::visit_expr_call(self, expression);
                return;
            };
            let Some(symbol) = BROWSER_PIPE_DYNAMIC_SYMBOLS
                .iter()
                .copied()
                .find(|expected| *expected == symbol)
            else {
                self.symbols.clear();
                self.symbols.push("<unknown dynamic symbol>");
                visit::visit_expr_call(self, expression);
                return;
            };
            self.symbols.push(symbol);
        }
        visit::visit_expr_call(self, expression);
    }
}

fn collect_environment_mutation_tokens(
    tokens: &proc_macro2::TokenStream,
    path: &str,
    findings: &mut Vec<String>,
) {
    for token in tokens.clone() {
        match token {
            proc_macro2::TokenTree::Ident(ident)
                if is_forbidden_environment_bypass_name(&ident.to_string()) =>
            {
                findings.push(format!("{path}:{ident}"));
            }
            proc_macro2::TokenTree::Group(group) => {
                collect_environment_mutation_tokens(&group.stream(), path, findings);
            }
            _ => {}
        }
    }
}

fn is_forbidden_environment_mutation_name(name: &str) -> bool {
    matches!(
        name,
        "set_var"
            | "remove_var"
            | "setenv"
            | "unsetenv"
            | "putenv"
            | "clearenv"
            | "SetEnvironmentVariableW"
            | "SetEnvironmentVariableA"
            | "_putenv"
            | "_wputenv"
            | "_putenv_s"
            | "_wputenv_s"
    )
}

fn is_forbidden_environment_bypass_name(name: &str) -> bool {
    is_forbidden_environment_mutation_name(name)
        || matches!(
            name,
            "environ"
                | "_wenviron"
                | "dlsym"
                | "dlvsym"
                | "dlopen"
                | "GetProcAddress"
                | "libloading"
                | "paste"
                | "concat_idents"
                | "__environ"
                | "__environ_location"
                | "__p__environ"
                | "__p__wenviron"
                | "_get_environ"
                | "_get_wenviron"
        )
}

fn is_forbidden_environment_symbol_name(name: &str) -> bool {
    is_forbidden_environment_mutation_name(name)
        || matches!(
            name,
            "environ"
                | "_wenviron"
                | "dlsym"
                | "dlvsym"
                | "dlopen"
                | "GetProcAddress"
                | "libloading"
                | "_NSGetEnviron"
                | "__environ"
                | "__environ_location"
                | "__p__environ"
                | "__p__wenviron"
                | "_get_environ"
                | "_get_wenviron"
        )
}

struct EnvironmentLinkNameMutations<'a> {
    path: &'a str,
    findings: &'a mut Vec<String>,
    inspect_symbol_literals: bool,
}

impl<'ast> Visit<'ast> for EnvironmentLinkNameMutations<'_> {
    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        if expression.path.segments.last().is_some_and(|segment| {
            matches!(
                segment.ident.to_string().as_str(),
                "unix_environ" | "parent_environ" | "_NSGetEnviron"
            )
        }) {
            let name = &expression.path.segments.last().unwrap().ident;
            self.findings.push(format!("{}:accessor={name}", self.path));
        }
        visit::visit_expr_path(self, expression);
    }

    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if attribute.path().is_ident("link_name") {
            let literal_name = match &attribute.meta {
                Meta::NameValue(value) => match &value.value {
                    Expr::Lit(value) => match &value.lit {
                        Lit::Str(value) => Some(value.value()),
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            };
            let Some(name) = literal_name else {
                self.findings
                    .push(format!("{}:link_name=<non-literal>", self.path));
                return;
            };
            if is_forbidden_environment_bypass_name(&name) {
                self.findings
                    .push(format!("{}:link_name={name}", self.path));
            }
            return;
        }
        visit::visit_attribute(self, attribute);
    }

    fn visit_expr_lit(&mut self, expression: &'ast syn::ExprLit) {
        if self.inspect_symbol_literals {
            let symbol = match &expression.lit {
                Lit::Str(value) => Some(value.value().into_bytes()),
                Lit::ByteStr(value) => Some(value.value()),
                _ => None,
            };
            if let Some(mut symbol) = symbol {
                while symbol.last() == Some(&0) {
                    symbol.pop();
                }
                if let Ok(symbol) = std::str::from_utf8(&symbol) {
                    if is_forbidden_environment_symbol_name(symbol) {
                        self.findings
                            .push(format!("{}:dynamic_symbol={symbol}", self.path));
                    }
                }
            }
        }
        visit::visit_expr_lit(self, expression);
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn parent_capture_ast_has_no_std_env_enumerator_or_rust_host_map_policy() {
    let syntax = syn::parse_file(include_str!("parent_env_snapshot.rs")).unwrap();
    let mut visitor = ForbiddenHostPolicy::default();
    visitor.visit_file(&syntax);
    assert!(
        visitor.findings.is_empty(),
        "forbidden host policy paths: {:?}",
        visitor.findings,
    );
}

#[derive(Default)]
struct ForbiddenHostPolicy {
    findings: Vec<String>,
}

impl<'ast> Visit<'ast> for ForbiddenHostPolicy {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        let segments = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let forbidden_container = segments
            .last()
            .is_some_and(|name| matches!(name.as_str(), "HashMap" | "BTreeMap" | "IndexMap"));
        let forbidden_std_env =
            segments.ends_with(&["std".to_owned(), "env".to_owned(), "vars".to_owned()])
                || segments.ends_with(&["std".to_owned(), "env".to_owned(), "vars_os".to_owned()]);
        if forbidden_container || forbidden_std_env {
            self.findings.push(segments.join("::"));
        }
        visit::visit_path(self, path);
    }
}

#[cfg(unix)]
#[test]
#[ignore = "requires Python and the pinned local Node 20.19.4 runtime"]
fn raw_execve_node_20_object_entries_differential() {
    const NODE_SCRIPT: &str = r#"
const units = (value) => Array.from({ length: value.length }, (_, i) => value.charCodeAt(i));
const allowed = new Set(['A', '\ufffd', '2', '10', '4294967294', '01', '4294967295', 'EQ']);
const entries = Object.entries(process.env)
  .filter(([key]) => allowed.has(key))
  .map(([key, value]) => ({ key: units(key), value: units(value) }));
process.stdout.write(JSON.stringify({
  versions: { node: process.versions.node, v8: process.versions.v8, uv: process.versions.uv },
  entries,
}));
"#;
    const PYTHON_LAUNCHER: &str = r#"
import ctypes, json, os, shutil, sys
request = json.loads(sys.stdin.read())
node = shutil.which('node')
if node is None:
    raise RuntimeError('node not found')
node = os.fsencode(node)
script = request['script'].encode('utf-8')
argv = (ctypes.c_char_p * 4)(b'node', b'-e', script, None)
entries = [bytes.fromhex(value) for value in request['envHex']]
envp = (ctypes.c_char_p * (len(entries) + 1))(*entries, None)
libc = ctypes.CDLL(None, use_errno=True)
libc.execve.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_char_p)]
libc.execve.restype = ctypes.c_int
libc.execve(node, argv, envp)
raise OSError(ctypes.get_errno(), 'execve failed')
"#;

    let raw = vec![
        b"A=first".to_vec(),
        b"A=second".to_vec(),
        b"\xff=invalid".to_vec(),
        b"\xef\xbf\xbd=projected".to_vec(),
        b"2=two".to_vec(),
        b"10=ten".to_vec(),
        b"4294967294=max-index".to_vec(),
        b"01=leading".to_vec(),
        b"4294967295=max-near".to_vec(),
        b"NO_EQUALS".to_vec(),
        b"EQ=a=\xff".to_vec(),
    ];
    let getter_entries = raw.clone();
    let snapshot = ParentEnvSnapshot::from_unix_raw_entries(raw.clone(), move |key| {
        getter_entries.iter().find_map(|entry| {
            let equals = entry.iter().position(|byte| *byte == b'=')?;
            (&entry[..equals] == key).then(|| entry[equals + 1..].to_vec())
        })
    });

    let request = json!({
        "script": NODE_SCRIPT,
        "envHex": raw.iter().map(|entry| encode_hex(entry)).collect::<Vec<_>>(),
    });
    let mut child = Command::new("python3")
        .args(["-c", PYTHON_LAUNCHER])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("python3 must launch the raw execve oracle");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "raw execve oracle failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let oracle: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(oracle["versions"]["node"], ParentEnvSnapshot::NODE_VERSION);
    assert_eq!(oracle["versions"]["v8"], ParentEnvSnapshot::V8_VERSION);
    assert_eq!(oracle["versions"]["uv"], ParentEnvSnapshot::LIBUV_VERSION);
    let expected = oracle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| (json_units(&entry["key"]), json_units(&entry["value"])))
        .collect::<Vec<_>>();
    assert_eq!(logical_entries(&snapshot), expected);
    assert!(
        snapshot.get(&RuntimeString::from_utf8("2")).is_none(),
        "canonical array-index key must be omitted by indexed property Get",
    );
}

#[cfg(unix)]
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(unix)]
fn json_units(value: &serde_json::Value) -> Vec<u16> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_u64().unwrap() as u16)
        .collect()
}
