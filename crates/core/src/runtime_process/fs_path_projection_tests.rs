use quote::ToTokens;
use sha2::{Digest, Sha256};
use syn::visit::{self, Visit};
use syn::{Expr, Lit, Meta, Type};

use super::*;
use crate::runtime_values::{RuntimePath, RuntimeString};

fn path(units: impl Into<Vec<u16>>) -> RuntimePath {
    RuntimePath::from_runtime_string(RuntimeString::from_utf16(units))
}

fn scalar(value: &str) -> RuntimePath {
    RuntimePath::from(value)
}

fn project_unix(path: &RuntimePath) -> Vec<u8> {
    match project_for_target(Node20FsPathTarget::Unix, path, "path").unwrap() {
        Node20FsProjectedPath::Unix(path) => path.as_bytes().to_vec(),
        Node20FsProjectedPath::Windows(_) => panic!("Unix target returned a Windows path"),
    }
}

fn project_windows(path: &RuntimePath) -> Vec<u16> {
    match project_for_target(Node20FsPathTarget::Windows, path, "path").unwrap() {
        Node20FsProjectedPath::Windows(path) => path.as_wide().to_vec(),
        Node20FsProjectedPath::Unix(_) => panic!("Windows target returned a Unix path"),
    }
}

#[test]
fn unix_uses_the_single_node_utf8_projection_at_the_terminal_boundary() {
    let logical = path(vec![
        b'/' as u16,
        b'a' as u16,
        b'/' as u16,
        0xd800,
        b'/' as u16,
        0xdc00,
        b'/' as u16,
        0xfffd,
        b'/' as u16,
        0xd83d,
        0xde00,
    ]);

    assert_eq!(
        project_unix(&logical),
        b"/a/\xef\xbf\xbd/\xef\xbf\xbd/\xef\xbf\xbd/\xf0\x9f\x98\x80",
    );
    assert_eq!(
        logical.as_runtime_string().as_utf16(),
        &[
            b'/' as u16,
            b'a' as u16,
            b'/' as u16,
            0xd800,
            b'/' as u16,
            0xdc00,
            b'/' as u16,
            0xfffd,
            b'/' as u16,
            0xd83d,
            0xde00,
        ],
    );
}

#[test]
fn windows_decodes_the_same_node_utf8_bytes_with_the_pinned_libuv_algorithm() {
    let logical = path(vec![
        b'C' as u16,
        b':' as u16,
        b'\\' as u16,
        b'a' as u16,
        b'/' as u16,
        0xd800,
        b'/' as u16,
        0xdc00,
        b'/' as u16,
        0xfffd,
        b'/' as u16,
        0xd83d,
        0xde00,
    ]);

    let projection = project_for_target(Node20FsPathTarget::Windows, &logical, "path").unwrap();
    let Node20FsProjectedPath::Windows(projection) = projection else {
        panic!("Windows target returned a Unix path");
    };
    assert_eq!(
        projection.node_utf8_bytes(),
        logical.as_runtime_string().to_node_utf8(),
    );
    assert_eq!(
        projection.as_wide(),
        vec![
            b'C' as u16,
            b':' as u16,
            b'\\' as u16,
            b'a' as u16,
            b'/' as u16,
            0xfffd,
            b'/' as u16,
            0xfffd,
            b'/' as u16,
            0xfffd,
            b'/' as u16,
            0xd83d,
            0xde00,
        ],
    );
}

#[test]
fn windows_preserves_node_vendored_libuv_u10000_boundary_behavior() {
    let u10000 = path(vec![0xd800, 0xdc00]);

    assert_eq!(project_unix(&u10000), [0xf0, 0x90, 0x80, 0x80]);
    assert_eq!(
        project_windows(&u10000),
        [0],
        "Node's vendored fs__wtf8_to_wide uses code_point > 0x10000, not >=",
    );
}

#[test]
fn windows_does_not_normalize_prefix_or_rewrite_path_grammar() {
    let long_tail = "a".repeat(360);
    for value in [
        r"C:/alpha\..\beta/",
        r"\\server/share\folder/../file",
        r"\\?\UNC\server\share/mixed\path",
        r"\\.\pipe/name\with/mixed/separators",
        &format!(r"C:\{long_tail}"),
    ] {
        assert_eq!(
            project_windows(&scalar(value)),
            value.encode_utf16().collect::<Vec<_>>(),
            "projection changed the lexical path {value:?}",
        );
    }
}

#[test]
fn logical_nul_is_rejected_before_either_host_projection() {
    for target in [Node20FsPathTarget::Unix, Node20FsPathTarget::Windows] {
        for units in [
            vec![0, b'a' as u16],
            vec![b'a' as u16, 0, b'b' as u16],
            vec![b'a' as u16, 0],
            vec![0xd800, 0],
        ] {
            let error = project_for_target(target, &path(units), "path").unwrap_err();
            assert_eq!(error.code(), &RuntimeString::from("ERR_INVALID_ARG_VALUE"));
            assert_eq!(error.argument_label(), &RuntimeString::from("path"));
        }
    }

    let error =
        project_for_target(Node20FsPathTarget::Unix, &path(vec![0xd800, 0]), "src").unwrap_err();
    assert_eq!(
        error.message(),
        &RuntimeString::from(
            "The argument 'src' must be a string, Uint8Array, or URL without null bytes. Received '\\ud800\\x00'",
        ),
    );
}

#[test]
fn nul_errors_match_quotes_truncation_and_multiline_concatenation() {
    for (label, logical, expected) in [
        (
            "path",
            RuntimeString::from_utf16(vec![b'x' as u16, 0, b'y' as u16]),
            "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received 'x\\x00y'".to_owned(),
        ),
        (
            "dest",
            RuntimeString::from("a'b\0c"),
            "The argument 'dest' must be a string, Uint8Array, or URL without null bytes. Received \"a'b\\x00c\"".to_owned(),
        ),
        (
            "path",
            RuntimeString::from("a'b\"c\0"),
            "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received `a'b\"c\\x00`".to_owned(),
        ),
        (
            "foo.bar",
            RuntimeString::from("a'b\"c`d${e}\0z"),
            "The property 'foo.bar' must be a string, Uint8Array, or URL without null bytes. Received 'a\\'b\"c`d${e}\\x00z'".to_owned(),
        ),
        (
            "path",
            RuntimeString::from(format!("{}\0tail", "a".repeat(130))),
            format!(
                "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received '{}...",
                "a".repeat(127),
            ),
        ),
        (
            "path",
            RuntimeString::from("a\u{000b}\0b"),
            "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received 'a\\x0B\\x00b'".to_owned(),
        ),
        (
            "path",
            RuntimeString::from(format!("{}\n\0", "a".repeat(75))),
            format!(
                "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received '{}\\n' +\n  '\\x00'",
                "a".repeat(75),
            ),
        ),
        (
            "path",
            RuntimeString::from(format!("{}'\n\0", "a".repeat(75))),
            format!(
                "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received \"{}'\\n\" +\n  '\\x00'",
                "a".repeat(75),
            ),
        ),
    ] {
        let error = project_for_target(
            Node20FsPathTarget::Windows,
            &RuntimePath::from_runtime_string(logical),
            label,
        )
        .unwrap_err();
        assert_eq!(error.message(), &RuntimeString::from(expected));
    }

    let far_quote = RuntimePath::from(format!("{}'\0", "a".repeat(10_000)));
    let error = project_for_target(Node20FsPathTarget::Unix, &far_quote, "path").unwrap_err();
    assert_eq!(
        error.message(),
        &RuntimeString::from(format!(
            "The argument 'path' must be a string, Uint8Array, or URL without null bytes. Received '{}...",
            "a".repeat(127),
        )),
    );
}

#[test]
fn module_ast_contains_one_projection_owned_only_by_the_sealed_entrypoint() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/runtime_process/fs_path_projection.rs");
    let matches = node_utf8_production_graph_contexts(&root);
    assert_eq!(
        matches,
        ["Node20FsPathProjection::project"],
        "the entire module must contain exactly one projection, owned by the sealed entrypoint",
    );
    assert_eq!(
        production_graph_indirect_projection_references(&root),
        Vec::<String>::new(),
        "the sealed production graph permits only the one direct method call",
    );
    assert_eq!(
        production_graph_sealed_projection_statements(&root),
        ["Node20FsPathProjection::project"],
        "the one direct projection must be the sealed entrypoint's top-level initializer",
    );
    assert_eq!(
        production_graph_conditional_sealed_entrypoints(&root),
        Vec::<String>::new(),
        "the sealed projection impl and method must compile on every target",
    );
    assert_eq!(
        production_graph_sealed_entrypoint_hashes(&root),
        [SEALED_PROJECT_ENTRYPOINT_SHA256],
        "the complete sealed entrypoint AST and call graph changed",
    );
    assert_eq!(
        production_graph_macro_contexts(&root),
        Vec::<String>::new(),
        "the sealed production graph has an empty macro allowlist",
    );
    assert_eq!(
        production_graph_unexpected_codegen_attributes(&root),
        Vec::<String>::new(),
        "the sealed production graph has no unpinned attribute/proc macros",
    );
    assert_eq!(
        production_graph_unexpected_imports(&root),
        Vec::<String>::new(),
        "the sealed production graph permits only its pinned runtime-value type import",
    );
    let crate_source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert_eq!(
        crate_macro_prelude_codegen_imports(&crate_source),
        Vec::<String>::new(),
        "crate ancestors may not shadow standard derives through the macro prelude",
    );
    assert_eq!(
        projection_parent_module_violations(include_str!("mod.rs")),
        Vec::<String>::new(),
        "the parent module edge must select the sealed source unconditionally",
    );
}

#[test]
fn module_ast_guard_follows_external_production_submodules() {
    static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "klaus-fs-projection-graph-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(directory.join("root")).unwrap();
    std::fs::write(
        directory.join("root.rs"),
        "struct Root; impl Root { fn project(value: &RuntimeString) { let _ = value.to_node_utf8(); } } mod hidden;",
    )
    .unwrap();
    std::fs::write(
        directory.join("root/hidden.rs"),
        "fn injected(value: &RuntimeString) { let _ = RuntimeString::to_node_utf8(value); }",
    )
    .unwrap();

    let matches = node_utf8_production_graph_contexts(&directory.join("root.rs"));
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(matches.len(), 2, "external module projection escaped");
    assert!(matches.iter().any(|owner| owner == "Root::project"));
    assert!(matches.iter().any(|owner| owner == "injected"));
}

#[test]
fn module_ast_guard_resolves_inline_path_attributes_like_rustc() {
    static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "klaus-fs-inline-path-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(directory.join("root/inline")).unwrap();
    std::fs::write(
        directory.join("root.rs"),
        "struct Root; impl Root { fn project(value: &RuntimeString) { let _ = value.to_node_utf8(); } } mod inline { #[path = \"projected.rs\"] mod projected; }",
    )
    .unwrap();
    std::fs::write(
        directory.join("projected.rs"),
        "fn harmless_decoy(value: &RuntimeString) { let _ = value; }",
    )
    .unwrap();
    std::fs::write(
        directory.join("root/inline/projected.rs"),
        "fn injected(value: &RuntimeString) { let _ = RuntimeString::to_node_utf8(value); }",
    )
    .unwrap();

    let matches = node_utf8_production_graph_contexts(&directory.join("root.rs"));
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(
        matches.len(),
        2,
        "scanner followed the decoy instead of rustc's inline-module path",
    );
    assert!(matches.iter().any(|owner| owner == "injected"));
}

#[test]
fn module_ast_guard_uses_an_inline_modules_own_path_as_its_nested_base() {
    static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "klaus-fs-inline-own-path-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(directory.join("thread_files")).unwrap();
    std::fs::create_dir_all(directory.join("root/thread")).unwrap();
    std::fs::write(
        directory.join("root.rs"),
        "struct Root; impl Root { fn project(value: &RuntimeString) { let _ = value.to_node_utf8(); } } #[path = \"thread_files\"] mod thread { #[path = \"tls.rs\"] mod hidden; }",
    )
    .unwrap();
    std::fs::write(
        directory.join("root/thread/tls.rs"),
        "fn harmless_decoy(value: &RuntimeString) { let _ = value; }",
    )
    .unwrap();
    std::fs::write(
        directory.join("thread_files/tls.rs"),
        "fn injected(value: &RuntimeString) { let _ = RuntimeString::to_node_utf8(value); }",
    )
    .unwrap();

    let matches = node_utf8_production_graph_contexts(&directory.join("root.rs"));
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(
        matches.len(),
        2,
        "scanner ignored the inline module's own #[path] nested base",
    );
    assert!(matches.iter().any(|owner| owner == "injected"));
}

#[test]
fn module_ast_guard_excludes_all_test_only_external_modules() {
    static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "klaus-fs-test-only-module-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(directory.join("root")).unwrap();
    std::fs::write(
        directory.join("root.rs"),
        "struct Root; impl Root { fn project(value: &RuntimeString) { let _ = value.to_node_utf8(); } } #[cfg(all(test, unix))] mod tests;",
    )
    .unwrap();
    std::fs::write(
        directory.join("root/tests.rs"),
        "fn test_only(value: &RuntimeString) { let _ = value.to_node_utf8(); }",
    )
    .unwrap();

    let matches = node_utf8_production_graph_contexts(&directory.join("root.rs"));
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(matches, ["Root::project"]);
}

#[test]
fn module_ast_guard_rejects_a_second_projection_hidden_in_a_helper() {
    let source = format!(
        "{}\nfn injected_projection(value: &RuntimeString) {{ let _ = value.to_node_utf8(); }}\n",
        include_str!("fs_path_projection.rs"),
    );
    let matches = node_utf8_call_contexts(&source);
    assert_eq!(
        matches,
        ["Node20FsPathProjection::project", "injected_projection",],
        "the module-wide visitor must expose a helper-owned second projection",
    );
    assert_ne!(
        matches,
        ["Node20FsPathProjection::project"],
        "the production exact-one contract must reject the mutated module",
    );
}

#[test]
fn module_ast_guard_rejects_associated_call_and_function_pointer_bypasses() {
    for injected in [
        "fn injected(value: &RuntimeString) { let _ = RuntimeString::to_node_utf8(value); }",
        "fn injected() { let _project: fn(&RuntimeString) -> Vec<u8> = RuntimeString::to_node_utf8; }",
        "fn injected(value: &RuntimeString) { use RuntimeString::to_node_utf8 as project; let _ = project(value); }",
    ] {
        let source = format!("{}\n{injected}\n", include_str!("fs_path_projection.rs"));
        assert_eq!(
            node_utf8_call_contexts(&source).len(),
            2,
            "the exact-one guard missed {injected}",
        );
    }
}

#[test]
fn module_ast_guard_rejects_reusing_one_function_pointer_for_two_projections() {
    let source = r#"
        struct Node20FsPathProjection;
        impl Node20FsPathProjection {
            fn project(logical: &RuntimeString) {
                let project = RuntimeString::to_node_utf8;
                let _first = project(logical);
                let _second = project(logical);
            }
        }
    "#;
    let analysis = analyze_projection_source(source);
    assert_eq!(
        analysis.contexts,
        ["Node20FsPathProjection::project"],
        "the identifier count alone reproduces the old false green",
    );
    assert_eq!(
        analysis.indirect_projection_references,
        ["Node20FsPathProjection::project"],
        "the closed guard must reject every indirect projection reference",
    );
}

#[test]
fn module_ast_guard_rejects_reusing_one_direct_call_via_closure_or_loop() {
    for source in [
        r#"
            struct Node20FsPathProjection;
            impl Node20FsPathProjection {
                fn project(logical: &RuntimeString) {
                    let project = || logical.to_node_utf8();
                    let _first = project();
                    let _second = project();
                }
            }
        "#,
        r#"
            struct Node20FsPathProjection;
            impl Node20FsPathProjection {
                fn project(logical: &RuntimeString) {
                    for _ in 0..2 {
                        let node_utf8 = logical.to_node_utf8();
                        drop(node_utf8);
                    }
                }
            }
        "#,
    ] {
        let analysis = analyze_projection_source(source);
        assert_eq!(
            analysis.contexts,
            ["Node20FsPathProjection::project"],
            "the direct-call count alone reproduces the old false green",
        );
        assert_eq!(
            analysis.sealed_projection_statements,
            Vec::<String>::new(),
            "a repeated projection must not match the sealed top-level statement",
        );
    }
}

#[test]
fn module_ast_guard_rejects_macro_token_and_inline_submodule_bypasses() {
    for injected in [
        "macro_rules! injected { ($value:expr) => { $value.to_node_utf8() }; } fn run(value: &RuntimeString) { let _ = injected!(value); }",
        "mod injected { use super::*; fn project(value: &RuntimeString) { let _ = value.to_node_utf8(); } }",
    ] {
        let source = format!("{}\n{injected}\n", include_str!("fs_path_projection.rs"));
        assert_eq!(
            node_utf8_call_contexts(&source).len(),
            2,
            "the exact-one guard missed {injected}",
        );
    }
}

#[test]
fn module_ast_guard_rejects_a_macro_without_a_literal_projection_identifier() {
    let source = format!(
        "{}\nmacro_rules! invoke_named_method {{ ($value:expr, $method:ident) => {{ $value.$method() }}; }}\n",
        include_str!("fs_path_projection.rs"),
    );
    assert_eq!(
        module_macro_contexts(&source).len(),
        1,
        "an identifier-generating macro escaped the empty production allowlist",
    );
}

#[test]
fn module_ast_guard_rejects_an_unpinned_attribute_macro() {
    let source = format!(
        "{}\n#[generate_projection] fn injected(value: &RuntimeString) {{ let _ = value; }}\n",
        include_str!("fs_path_projection.rs"),
    );
    assert_eq!(
        analyze_projection_source(&source).unexpected_codegen_attributes,
        ["generate_projection"],
        "an attribute proc macro escaped the closed allowlist",
    );
}

#[test]
fn module_ast_guard_rejects_imported_custom_standard_named_derive() {
    let source = format!(
        "{}\nuse evil_macros::Clone; #[derive(Clone)] struct Injected;\n",
        include_str!("fs_path_projection.rs"),
    );
    let analysis = analyze_projection_source(&source);
    assert_eq!(analysis.unexpected_imports, ["use evil_macros :: Clone ;"]);
}

#[test]
fn module_ast_guard_rejects_crate_ancestor_macro_prelude_shadowing() {
    for source in [
        "#[macro_use] extern crate evil_macros;",
        "#[cfg_attr(not(test), macro_use)] extern crate evil_macros;",
        "#[prelude_import] use evil_macros::*;",
    ] {
        let findings = macro_prelude_codegen_imports(source, "synthetic.rs");
        assert_eq!(
            findings.len(),
            1,
            "a crate-ancestor macro import could shadow the standard derive: {source}",
        );
    }
}

#[test]
fn module_ast_guard_rejects_target_split_entrypoints_and_parent_modules() {
    let source = r#"
        struct Node20FsPathProjection;
        #[cfg(windows)]
        impl Node20FsPathProjection {
            #[cfg(windows)]
            fn project(logical: &RuntimeString) {
                let node_utf8 = logical.to_node_utf8();
                drop(node_utf8);
            }
        }
        #[cfg(unix)]
        impl Node20FsPathProjection {
            fn project(_logical: &RuntimeString) {}
        }
    "#;
    let analysis = analyze_projection_source(source);
    assert_eq!(analysis.contexts, ["Node20FsPathProjection::project"]);
    assert_eq!(
        analysis.sealed_projection_statements,
        ["Node20FsPathProjection::project"],
        "the old exact-one checks reproduce the target-split false green",
    );
    assert_eq!(
        analysis.conditional_sealed_entrypoints.len(),
        3,
        "both conditional impls and the conditional method must be rejected",
    );

    let parent = "#[cfg(unix)] #[allow(dead_code)] pub(crate) mod fs_path_projection;";
    assert!(!projection_parent_module_violations(parent).is_empty());
}

#[test]
fn module_ast_guard_rejects_external_helper_and_recursive_callgraph_edges() {
    for source in [
        r#"
            struct Node20FsPathProjection;
            impl Node20FsPathProjection {
                fn project(logical: &RuntimeString) {
                    let _extra = crate::runtime_values::evil_project(logical);
                    let node_utf8 = logical.to_node_utf8();
                    drop(node_utf8);
                }
            }
        "#,
        r#"
            struct Node20FsPathProjection;
            impl Node20FsPathProjection {
                fn project(logical: &RuntimeString) {
                    let _extra = Self::project(logical);
                    let node_utf8 = logical.to_node_utf8();
                    drop(node_utf8);
                }
            }
        "#,
    ] {
        let analysis = analyze_projection_source(source);
        assert_eq!(analysis.contexts, ["Node20FsPathProjection::project"]);
        assert_eq!(
            analysis.sealed_projection_statements,
            ["Node20FsPathProjection::project"],
            "the old exact-one checks reproduce the external callgraph false green",
        );
        assert_ne!(
            analysis.sealed_entrypoint_hashes,
            [SEALED_PROJECT_ENTRYPOINT_SHA256],
            "the complete entrypoint pin must reject every added callgraph edge",
        );
    }
}

const SEALED_PROJECT_ENTRYPOINT_SHA256: &str =
    "22e0851bf4badc8dcc67ab68e552691587f3f806ed42e1bb8bfbe03e20d8087d";

#[derive(Default)]
struct NodeUtf8MethodCalls {
    current_impl: Option<String>,
    current_callable: Option<String>,
    direct_method_calls: usize,
    contexts: Vec<String>,
    indirect_projection_references: Vec<String>,
    macros: Vec<String>,
    unexpected_codegen_attributes: Vec<String>,
    unexpected_imports: Vec<String>,
    sealed_projection_statements: Vec<String>,
    conditional_sealed_entrypoints: Vec<String>,
    sealed_entrypoint_hashes: Vec<String>,
}

impl<'ast> Visit<'ast> for NodeUtf8MethodCalls {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        if !is_allowed_projection_use(item) {
            self.unexpected_imports
                .push(item.to_token_stream().to_string());
        }
        visit::visit_item_use(self, item);
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        self.unexpected_imports
            .push(item.to_token_stream().to_string());
        visit::visit_item_extern_crate(self, item);
    }

    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if !is_allowed_projection_attribute(attribute) {
            self.unexpected_codegen_attributes
                .push(attribute.path().to_token_stream().to_string());
        }
        visit::visit_attribute(self, attribute);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let impl_name = match item.self_ty.as_ref() {
            Type::Path(path) => path
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::"),
            _ => "<non-path impl>".to_owned(),
        };
        if impl_name == "Node20FsPathProjection" {
            self.conditional_sealed_entrypoints.extend(
                item.attrs
                    .iter()
                    .filter(|attribute| {
                        attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
                    })
                    .map(|attribute| {
                        format!(
                            "Node20FsPathProjection::<impl>:{}",
                            attribute.to_token_stream()
                        )
                    }),
            );
        }
        let previous = self.current_impl.replace(impl_name);
        visit::visit_item_impl(self, item);
        self.current_impl = previous;
    }

    fn visit_impl_item_fn(&mut self, method: &'ast syn::ImplItemFn) {
        let owner = self.current_impl.as_deref().unwrap_or("<unknown impl>");
        if owner == "Node20FsPathProjection" && method.sig.ident == "project" {
            self.sealed_entrypoint_hashes
                .push(hex_sha256(method.to_token_stream().to_string().as_bytes()));
            self.conditional_sealed_entrypoints.extend(
                method
                    .attrs
                    .iter()
                    .filter(|attribute| {
                        attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
                    })
                    .map(|attribute| format!("{owner}::project:{}", attribute.to_token_stream())),
            );
            self.sealed_projection_statements.extend(
                method
                    .block
                    .stmts
                    .iter()
                    .filter(|statement| is_sealed_projection_statement(statement))
                    .map(|_| format!("{owner}::project")),
            );
        }
        let previous = self
            .current_callable
            .replace(format!("{owner}::{}", method.sig.ident));
        visit::visit_impl_item_fn(self, method);
        self.current_callable = previous;
    }

    fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
        let previous = self
            .current_callable
            .replace(function.sig.ident.to_string());
        visit::visit_item_fn(self, function);
        self.current_callable = previous;
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "to_node_utf8" {
            self.direct_method_calls += 1;
            self.contexts.push(
                self.current_callable
                    .clone()
                    .unwrap_or_else(|| "<module scope>".to_owned()),
            );
        }
        visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        if path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "to_node_utf8")
        {
            self.record_projection();
            self.indirect_projection_references.push(
                self.current_callable
                    .clone()
                    .unwrap_or_else(|| "<module scope>".to_owned()),
            );
        }
        visit::visit_expr_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        let owner = self
            .current_callable
            .clone()
            .unwrap_or_else(|| "<module scope>".to_owned());
        let name = mac.path.segments.last().map_or_else(
            || "<anonymous>".to_owned(),
            |segment| segment.ident.to_string(),
        );
        self.macros.push(format!("{owner}:{name}!"));
        let count = count_projection_identifiers(&mac.tokens);
        for _ in 0..count {
            self.record_projection();
        }
        visit::visit_macro(self, mac);
    }
}

fn is_sealed_projection_statement(statement: &syn::Stmt) -> bool {
    let syn::Stmt::Local(local) = statement else {
        return false;
    };
    if !local.attrs.is_empty() {
        return false;
    }
    let syn::Pat::Ident(binding) = &local.pat else {
        return false;
    };
    if binding.ident != "node_utf8"
        || binding.by_ref.is_some()
        || binding.mutability.is_some()
        || binding.subpat.is_some()
    {
        return false;
    }
    let Some(initializer) = &local.init else {
        return false;
    };
    if initializer.diverge.is_some() {
        return false;
    }
    let syn::Expr::MethodCall(call) = initializer.expr.as_ref() else {
        return false;
    };
    if !call.attrs.is_empty()
        || call.method != "to_node_utf8"
        || call.turbofish.is_some()
        || !call.args.is_empty()
    {
        return false;
    }
    let syn::Expr::Path(receiver) = call.receiver.as_ref() else {
        return false;
    };
    receiver.attrs.is_empty() && receiver.qself.is_none() && receiver.path.is_ident("logical")
}

impl NodeUtf8MethodCalls {
    fn record_projection(&mut self) {
        self.contexts.push(
            self.current_callable
                .clone()
                .unwrap_or_else(|| "<module scope>".to_owned()),
        );
    }
}

fn count_projection_identifiers(tokens: &proc_macro2::TokenStream) -> usize {
    tokens
        .clone()
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Ident(ident) => usize::from(ident == "to_node_utf8"),
            proc_macro2::TokenTree::Group(group) => count_projection_identifiers(&group.stream()),
            _ => 0,
        })
        .sum()
}

fn node_utf8_call_contexts(source: &str) -> Vec<String> {
    analyze_projection_source(source).contexts
}

fn module_macro_contexts(source: &str) -> Vec<String> {
    analyze_projection_source(source).macros
}

fn analyze_projection_source(source: &str) -> NodeUtf8MethodCalls {
    let syntax = syn::parse_file(source).unwrap();
    let mut visitor = NodeUtf8MethodCalls::default();
    visitor.visit_file(&syntax);
    let tokens = source.parse::<proc_macro2::TokenStream>().unwrap();
    let token_count = count_projection_identifiers(&tokens);
    assert!(
        visitor.contexts.len() <= token_count,
        "AST visitor double-counted a to_node_utf8 token",
    );
    visitor.contexts.extend(std::iter::repeat_n(
        "<unowned token>".to_owned(),
        token_count - visitor.contexts.len(),
    ));
    let indirect_count = token_count.saturating_sub(visitor.direct_method_calls);
    assert!(
        visitor.indirect_projection_references.len() <= indirect_count,
        "AST visitor double-counted an indirect projection reference",
    );
    visitor
        .indirect_projection_references
        .extend(std::iter::repeat_n(
            "<unowned indirect token>".to_owned(),
            indirect_count - visitor.indirect_projection_references.len(),
        ));
    visitor
}

fn node_utf8_production_graph_contexts(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).contexts
}

fn production_graph_macro_contexts(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).macros
}

fn production_graph_indirect_projection_references(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).indirect_projection_references
}

fn production_graph_sealed_projection_statements(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).sealed_projection_statements
}

fn production_graph_conditional_sealed_entrypoints(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).conditional_sealed_entrypoints
}

fn production_graph_sealed_entrypoint_hashes(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).sealed_entrypoint_hashes
}

fn production_graph_unexpected_codegen_attributes(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).unexpected_codegen_attributes
}

fn production_graph_unexpected_imports(root: &std::path::Path) -> Vec<String> {
    production_graph_analysis(root).unexpected_imports
}

fn crate_macro_prelude_codegen_imports(root: &std::path::Path) -> Vec<String> {
    let mut pending = vec![root.to_path_buf()];
    let mut findings = Vec::new();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            let mut entries = std::fs::read_dir(&path)
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            entries.sort_by_key(std::fs::DirEntry::file_name);
            pending.extend(entries.into_iter().map(|entry| entry.path()));
            continue;
        }
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        let relative = path.strip_prefix(root).unwrap().display().to_string();
        findings.extend(macro_prelude_codegen_imports(&source, &relative));
    }
    findings.sort();
    findings
}

fn macro_prelude_codegen_imports(source: &str, path: &str) -> Vec<String> {
    let syntax = syn::parse_file(source).unwrap();
    let mut visitor = MacroPreludeCodegenImports {
        path,
        findings: Vec::new(),
    };
    visitor.visit_file(&syntax);
    visitor.findings
}

struct MacroPreludeCodegenImports<'a> {
    path: &'a str,
    findings: Vec<String>,
}

impl<'ast> Visit<'ast> for MacroPreludeCodegenImports<'_> {
    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        let tokens = attribute.meta.to_token_stream();
        if attribute.path().is_ident("macro_use")
            || attribute.path().is_ident("prelude_import")
            || count_identifier_named(&tokens, "macro_use") > 0
            || count_identifier_named(&tokens, "prelude_import") > 0
        {
            self.findings
                .push(format!("{}:{}", self.path, attribute.to_token_stream()));
        }
        visit::visit_attribute(self, attribute);
    }
}

fn count_identifier_named(tokens: &proc_macro2::TokenStream, expected: &str) -> usize {
    tokens
        .clone()
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Ident(identifier) => usize::from(identifier == expected),
            proc_macro2::TokenTree::Group(group) => {
                count_identifier_named(&group.stream(), expected)
            }
            _ => 0,
        })
        .sum()
}

fn production_graph_analysis(root: &std::path::Path) -> NodeUtf8MethodCalls {
    let mut pending = vec![root.to_path_buf()];
    let mut visited = std::collections::BTreeSet::new();
    let mut analysis = NodeUtf8MethodCalls::default();
    while let Some(path) = pending.pop() {
        let path = crate::fs::node_path::canonicalize_host(&path)
            .unwrap_or_else(|error| panic!("cannot resolve module {}: {error}", path.display()));
        if !visited.insert(path.clone()) {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        let source_analysis = analyze_projection_source(&source);
        analysis.contexts.extend(source_analysis.contexts);
        analysis
            .indirect_projection_references
            .extend(source_analysis.indirect_projection_references);
        analysis.macros.extend(source_analysis.macros);
        analysis
            .unexpected_codegen_attributes
            .extend(source_analysis.unexpected_codegen_attributes);
        analysis
            .unexpected_imports
            .extend(source_analysis.unexpected_imports);
        analysis
            .sealed_projection_statements
            .extend(source_analysis.sealed_projection_statements);
        analysis
            .conditional_sealed_entrypoints
            .extend(source_analysis.conditional_sealed_entrypoints);
        analysis
            .sealed_entrypoint_hashes
            .extend(source_analysis.sealed_entrypoint_hashes);
        let syntax = syn::parse_file(&source).unwrap();
        let module_directory =
            if path.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                path.parent().unwrap().to_path_buf()
            } else {
                path.with_extension("")
            };
        collect_external_production_modules(
            &syntax.items,
            &module_directory,
            path.parent().unwrap(),
            &mut pending,
        );
    }
    analysis
}

fn projection_parent_module_violations(source: &str) -> Vec<String> {
    let syntax = syn::parse_file(source).unwrap();
    let modules = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "fs_path_projection" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    if modules.len() != 1 {
        violations.push(format!("module-count={}", modules.len()));
    }
    for module in modules {
        let allowed_attribute = module.attrs.len() == 1
            && module.attrs[0].path().is_ident("allow")
            && module.attrs[0].meta.to_token_stream().to_string() == "allow (dead_code)";
        let crate_visibility = match &module.vis {
            syn::Visibility::Restricted(visibility) => {
                visibility.in_token.is_none() && visibility.path.is_ident("crate")
            }
            _ => false,
        };
        if !allowed_attribute
            || !crate_visibility
            || module.content.is_some()
            || module.semi.is_none()
        {
            violations.push(module.to_token_stream().to_string());
        }
    }
    violations
}

fn is_allowed_projection_attribute(attribute: &syn::Attribute) -> bool {
    if attribute.path().is_ident("doc")
        || attribute.path().is_ident("cfg")
        || attribute.path().is_ident("allow")
        || attribute.path().is_ident("path")
    {
        return true;
    }
    if !attribute.path().is_ident("derive") {
        return false;
    }
    let Ok(derives) = attribute.parse_args_with(
        syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
    ) else {
        return false;
    };
    derives.iter().all(|derive| {
        derive.leading_colon.is_none()
            && derive.segments.len() == 1
            && matches!(
                derive.segments[0].ident.to_string().as_str(),
                "Clone" | "Copy" | "Debug" | "Default" | "Eq" | "Hash" | "PartialEq"
            )
    })
}

fn is_allowed_projection_use(item: &syn::ItemUse) -> bool {
    if item.leading_colon.is_some() {
        return false;
    }
    let syn::UseTree::Path(crate_path) = &item.tree else {
        return false;
    };
    if crate_path.ident != "crate" {
        return false;
    }
    let syn::UseTree::Path(values_path) = crate_path.tree.as_ref() else {
        return false;
    };
    if values_path.ident != "runtime_values" {
        return false;
    }
    let syn::UseTree::Group(group) = values_path.tree.as_ref() else {
        return false;
    };
    let names = group
        .items
        .iter()
        .map(|item| match item {
            syn::UseTree::Name(name) => Some(name.ident.to_string()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    names == Some(vec!["RuntimePath".to_owned(), "RuntimeString".to_owned()])
}

fn collect_external_production_modules(
    items: &[syn::Item],
    module_directory: &std::path::Path,
    explicit_path_directory: &std::path::Path,
    pending: &mut Vec<std::path::PathBuf>,
) {
    for item in items {
        let syn::Item::Mod(module) = item else {
            continue;
        };
        if module.attrs.iter().any(is_test_only_cfg) {
            continue;
        }
        if let Some((_, inline_items)) = &module.content {
            let nested_directory = module_path_attribute(module, explicit_path_directory)
                .unwrap_or_else(|| module_directory.join(module.ident.to_string()));
            collect_external_production_modules(
                inline_items,
                &nested_directory,
                &nested_directory,
                pending,
            );
            continue;
        }

        let explicit = module_path_attribute(module, explicit_path_directory);
        if let Some(path) = explicit {
            pending.push(path);
            continue;
        }

        let name = module.ident.to_string();
        let flat = module_directory.join(format!("{name}.rs"));
        let nested = module_directory.join(name).join("mod.rs");
        match (flat.is_file(), nested.is_file()) {
            (true, false) => pending.push(flat),
            (false, true) => pending.push(nested),
            (false, false) => panic!(
                "production module {} has no source at {} or {}",
                module.ident,
                flat.display(),
                nested.display(),
            ),
            (true, true) => panic!(
                "production module {} is ambiguous between {} and {}",
                module.ident,
                flat.display(),
                nested.display(),
            ),
        }
    }
}

fn module_path_attribute(
    module: &syn::ItemMod,
    explicit_path_directory: &std::path::Path,
) -> Option<std::path::PathBuf> {
    module.attrs.iter().find_map(|attribute| {
        if !attribute.path().is_ident("path") {
            return None;
        }
        let Meta::NameValue(value) = &attribute.meta else {
            return None;
        };
        let Expr::Lit(value) = &value.value else {
            return None;
        };
        let Lit::Str(value) = &value.lit else {
            return None;
        };
        Some(explicit_path_directory.join(value.value()))
    })
}

fn is_test_only_cfg(attribute: &syn::Attribute) -> bool {
    if !attribute.path().is_ident("cfg") {
        return false;
    }
    let Meta::List(list) = &attribute.meta else {
        return false;
    };
    let Ok(predicate) = syn::parse2::<Meta>(list.tokens.clone()) else {
        return false;
    };
    !cfg_possibility_with_test_disabled(&predicate).can_be_true
}

#[derive(Clone, Copy)]
struct CfgPossibility {
    can_be_false: bool,
    can_be_true: bool,
}

fn cfg_possibility_with_test_disabled(predicate: &Meta) -> CfgPossibility {
    match predicate {
        Meta::Path(path) if path.is_ident("test") => CfgPossibility {
            can_be_false: true,
            can_be_true: false,
        },
        Meta::Path(_) | Meta::NameValue(_) => unknown_cfg_possibility(),
        Meta::List(list) => {
            let Ok(arguments) = list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            ) else {
                return unknown_cfg_possibility();
            };
            let possibilities = arguments
                .iter()
                .map(cfg_possibility_with_test_disabled)
                .collect::<Vec<_>>();
            if list.path.is_ident("all") {
                CfgPossibility {
                    can_be_false: possibilities.iter().any(|value| value.can_be_false),
                    can_be_true: possibilities.iter().all(|value| value.can_be_true),
                }
            } else if list.path.is_ident("any") {
                CfgPossibility {
                    can_be_false: possibilities.iter().all(|value| value.can_be_false),
                    can_be_true: possibilities.iter().any(|value| value.can_be_true),
                }
            } else if list.path.is_ident("not") && possibilities.len() == 1 {
                CfgPossibility {
                    can_be_false: possibilities[0].can_be_true,
                    can_be_true: possibilities[0].can_be_false,
                }
            } else {
                unknown_cfg_possibility()
            }
        }
    }
}

fn unknown_cfg_possibility() -> CfgPossibility {
    CfgPossibility {
        can_be_false: true,
        can_be_true: true,
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
