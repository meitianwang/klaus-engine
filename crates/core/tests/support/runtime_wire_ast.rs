use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;
use serde::{Deserialize, Serialize};
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{
    Attribute, Fields, ItemEnum, ItemImpl, ItemStruct, LitStr, Path as SynPath, Token, Type,
};

const SCANNED_CRATES: &[&str] = &["core", "protocol", "eval", "sdk", "server"];

/// Serde facade modules excluded from the exact-runtime-codec wire surface.
///
/// The app-server layer is a deliberate JSON-RPC 2.0 envelope: it passes the
/// inner Op/Event wire JSON through untouched and publishes its OWN contract
/// as an exported schemars bundle (see `schema_bundle_covers_the_api_surface`).
/// Its serde types therefore live outside the frozen runtime-codec inventory.
/// [`scan_repository`] still parses these files and enforces the exclusion
/// legitimacy: every serde container needs schema ownership for the same type
/// and cfg context. Types with native runtime codecs rejoin the scanned surface
/// individually; schema-owned serde-only peers remain facade-owned.
const SERDE_FACADE_DIRS: &[&str] = &["crates/server/src/app_server/"];

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectionContract {
    pub encode_origins: Vec<String>,
    pub decode_origins: Vec<String>,
    pub encode_bindings: Vec<DirectionBindingContract>,
    pub decode_bindings: Vec<DirectionBindingContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectionBindingContract {
    pub origin: String,
    pub cfg_context: Vec<String>,
    pub cfg_attributes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectionalStringContract {
    pub serialize: Option<String>,
    pub deserialize: Option<String>,
}

impl DirectionalStringContract {
    fn over(&self, fallback: &Self) -> Self {
        Self {
            serialize: self
                .serialize
                .clone()
                .or_else(|| fallback.serialize.clone()),
            deserialize: self
                .deserialize
                .clone()
                .or_else(|| fallback.deserialize.clone()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CanonicalNameContract {
    pub serialize: String,
    pub deserialize: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DefaultContract {
    pub enabled: bool,
    pub function: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SkipContract {
    pub serialize: bool,
    pub deserialize: bool,
    pub serialize_if: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdapterContract {
    pub with: Option<String>,
    pub serialize_with: Option<String>,
    pub deserialize_with: Option<String>,
    pub effective_serialize_with: Option<String>,
    pub effective_deserialize_with: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaggingContract {
    pub kind: String,
    pub tag: Option<String>,
    pub content: Option<String>,
}

impl Default for TaggingContract {
    fn default() -> Self {
        Self {
            kind: "external".to_string(),
            tag: None,
            content: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainerSerdeContract {
    pub rename: DirectionalStringContract,
    pub rename_all: DirectionalStringContract,
    pub rename_all_fields: DirectionalStringContract,
    pub deny_unknown_fields: bool,
    pub default: DefaultContract,
    pub transparent: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deserialize_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deserialize_try_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serialize_into: Option<String>,
    pub tagging: TaggingContract,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldSerdeContract {
    pub canonical_name: Option<CanonicalNameContract>,
    pub aliases: Vec<String>,
    pub default: DefaultContract,
    pub skip: SkipContract,
    pub flatten: bool,
    pub adapters: AdapterContract,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VariantSerdeContract {
    pub canonical_name: Option<CanonicalNameContract>,
    pub aliases: Vec<String>,
    pub rename_all_fields: DirectionalStringContract,
    pub skip: SkipContract,
    pub other: bool,
    pub rust_default: bool,
    pub adapters: AdapterContract,
}

impl DirectionContract {
    pub fn can_encode(&self) -> bool {
        !self.encode_origins.is_empty()
    }

    pub fn can_decode(&self) -> bool {
        !self.decode_origins.is_empty()
    }

    fn add_encode(&mut self, origin: &str, cfg_context: &[String], cfg_attributes: &[String]) {
        push_unique(&mut self.encode_origins, origin);
        push_unique_binding(
            &mut self.encode_bindings,
            origin,
            cfg_context,
            cfg_attributes,
        );
    }

    fn add_decode(&mut self, origin: &str, cfg_context: &[String], cfg_attributes: &[String]) {
        push_unique(&mut self.decode_origins, origin);
        push_unique_binding(
            &mut self.decode_bindings,
            origin,
            cfg_context,
            cfg_attributes,
        );
    }

    fn merge(&mut self, other: &Self) {
        for origin in &other.encode_origins {
            push_unique(&mut self.encode_origins, origin);
        }
        for origin in &other.decode_origins {
            push_unique(&mut self.decode_origins, origin);
        }
        for binding in &other.encode_bindings {
            if !self.encode_bindings.contains(binding) {
                self.encode_bindings.push(binding.clone());
            }
        }
        for binding in &other.decode_bindings {
            if !self.decode_bindings.contains(binding) {
                self.decode_bindings.push(binding.clone());
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldContract {
    pub order: usize,
    pub rust_name: Option<String>,
    pub rust_type: String,
    pub visibility: String,
    pub cfg_attributes: Vec<String>,
    pub serde_attributes: Vec<String>,
    pub serde_semantics: FieldSerdeContract,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VariantContract {
    pub order: usize,
    pub rust_name: String,
    pub shape: String,
    pub cfg_attributes: Vec<String>,
    pub rust_attributes: Vec<String>,
    pub serde_attributes: Vec<String>,
    pub serde_semantics: VariantSerdeContract,
    pub fields: Vec<FieldContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceContract {
    pub lexical_path: Vec<String>,
    pub rust_name: String,
    pub kind: String,
    pub shape: String,
    pub visibility: String,
    pub generics: String,
    pub cfg_context: Vec<String>,
    pub cfg_attributes: Vec<String>,
    pub serde_attributes: Vec<String>,
    pub serde_semantics: ContainerSerdeContract,
    pub directions: DirectionContract,
    pub fields: Vec<FieldContract>,
    pub variants: Vec<VariantContract>,
}

impl SourceContract {
    pub fn can_encode(&self) -> bool {
        self.directions.can_encode()
    }

    pub fn can_decode(&self) -> bool {
        self.directions.can_decode()
    }
}

pub fn scan_repository(root: &Path) -> Result<BTreeMap<String, SourceContract>, String> {
    let mut files = Vec::new();
    for crate_name in SCANNED_CRATES {
        let source_root = root.join("crates").join(crate_name).join("src");
        if source_root.is_dir() {
            collect_rust_files(&source_root, &mut files)?;
        }
    }
    files.sort();
    let module_cfg_contexts = resolve_module_cfg_contexts(&files)?;

    let mut result = BTreeMap::new();
    for path in files {
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let relative = path
            .strip_prefix(root)
            .map_err(|error| format!("{} is outside repository: {error}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        let inherited_cfg = module_cfg_contexts.get(&path).cloned().unwrap_or_default();
        let facade = SERDE_FACADE_DIRS
            .iter()
            .any(|prefix| relative.starts_with(prefix));
        let facts = if facade {
            scan_serde_facade_source_with_context(&relative, &source, inherited_cfg)?
        } else {
            scan_source_with_context(&relative, &source, inherited_cfg)?
        };
        if !facts.is_empty() && !module_cfg_contexts.contains_key(&path) {
            return Err(format!(
                "serialized source file {relative} is not reachable from a crate root/module declaration"
            ));
        }
        for (symbol_key, contract) in facts {
            if result.insert(symbol_key.clone(), contract).is_some() {
                return Err(format!("serialized symbol key collision for {symbol_key}"));
            }
        }
    }
    Ok(result)
}

pub(super) fn scan_source(
    relative: &str,
    source: &str,
) -> Result<BTreeMap<String, SourceContract>, String> {
    scan_source_with_context(relative, source, Vec::new())
}

/// Validate facade ownership and return only types that have native codecs.
fn scan_serde_facade_source_with_context(
    relative: &str,
    source: &str,
    inherited_cfg: Vec<String>,
) -> Result<BTreeMap<String, SourceContract>, String> {
    let syntax =
        syn::parse_file(source).map_err(|error| format!("failed to parse {relative}: {error}"))?;
    let mut visitor = ContractVisitor {
        cfg_context: inherited_cfg,
        facade_ownership: true,
        ..ContractVisitor::default()
    };
    visitor.visit_file(&syntax);
    visitor.apply_manual_directions()?;
    for identity in visitor.schema_bindings.keys() {
        visitor.unique_item_index(identity, "JsonSchema")?;
    }
    for item in &visitor.items {
        for binding in item
            .contract
            .directions
            .encode_bindings
            .iter()
            .chain(&item.contract.directions.decode_bindings)
        {
            let owned = visitor
                .schema_bindings
                .get(&item.identity)
                .into_iter()
                .flatten()
                .any(|schema| schema_covers_binding(schema, item, binding));
            if !owned {
                return Err(format!(
                    "serde facade type {} requires JsonSchema ownership covering {:?}",
                    qualified_symbol_key(relative, &item.identity),
                    binding
                ));
            }
        }
    }

    let mut result = BTreeMap::new();
    for (identity, directions) in &visitor.runtime_directions {
        let index = visitor.unique_item_index(identity, "runtime codec")?;
        let mut contract = visitor.items[index].contract.clone();
        contract.directions.merge(directions);
        let symbol_key = qualified_symbol_key(relative, identity);
        if result.insert(symbol_key.clone(), contract).is_some() {
            return Err(format!("serialized symbol key collision for {symbol_key}"));
        }
    }
    Ok(result)
}

fn schema_covers_binding(
    schema: &DirectionBindingContract,
    item: &DiscoveredItem,
    binding: &DirectionBindingContract,
) -> bool {
    let available = item
        .contract
        .cfg_context
        .iter()
        .chain(&item.contract.cfg_attributes)
        .chain(&binding.cfg_context)
        .chain(&binding.cfg_attributes)
        .collect::<Vec<_>>();
    schema
        .cfg_context
        .iter()
        .chain(&schema.cfg_attributes)
        .all(|condition| available.contains(&condition))
}

fn scan_source_with_context(
    relative: &str,
    source: &str,
    inherited_cfg: Vec<String>,
) -> Result<BTreeMap<String, SourceContract>, String> {
    let syntax =
        syn::parse_file(source).map_err(|error| format!("failed to parse {relative}: {error}"))?;
    let mut visitor = ContractVisitor {
        cfg_context: inherited_cfg,
        ..ContractVisitor::default()
    };
    visitor.visit_file(&syntax);
    visitor.apply_manual_directions()?;

    let mut result = BTreeMap::new();
    for item in visitor
        .items
        .into_iter()
        .filter(|item| item.contract.can_encode() || item.contract.can_decode())
    {
        let symbol_key = qualified_symbol_key(relative, &item.identity);
        if result.insert(symbol_key.clone(), item.contract).is_some() {
            return Err(format!("serialized symbol key collision for {symbol_key}"));
        }
    }
    Ok(result)
}

fn resolve_module_cfg_contexts(
    files: &[PathBuf],
) -> Result<BTreeMap<PathBuf, Vec<String>>, String> {
    let file_set = files
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut edges = BTreeMap::<PathBuf, Vec<ExternalModuleEdge>>::new();
    for path in files {
        let source = fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let syntax = syn::parse_file(&source)
            .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
        let mut visitor = ExternalModuleVisitor::new(path);
        visitor.visit_file(&syntax);
        edges.insert(path.clone(), visitor.edges);
    }

    let mut contexts = BTreeMap::new();
    let mut pending = Vec::new();
    for path in files.iter().filter(|path| is_crate_root(path)) {
        contexts.insert(path.clone(), Vec::new());
        pending.push(path.clone());
    }
    while let Some(parent) = pending.pop() {
        let parent_context = contexts.get(&parent).cloned().unwrap_or_default();
        for edge in edges.get(&parent).into_iter().flatten() {
            if !file_set.contains(&edge.target) {
                continue;
            }
            let mut child_context = parent_context.clone();
            child_context.extend(edge.cfg_context.clone());
            match contexts.get(&edge.target) {
                Some(existing) if existing != &child_context => {
                    return Err(format!(
                        "module {} is reachable under conflicting cfg contexts {:?} and {:?}",
                        edge.target.display(),
                        existing,
                        child_context
                    ));
                }
                Some(_) => {}
                None => {
                    contexts.insert(edge.target.clone(), child_context);
                    pending.push(edge.target.clone());
                }
            }
        }
    }
    Ok(contexts)
}

#[derive(Clone)]
struct ExternalModuleEdge {
    target: PathBuf,
    cfg_context: Vec<String>,
}

struct ExternalModuleVisitor {
    module_directory: PathBuf,
    path_directory: PathBuf,
    inline_cfg_context: Vec<String>,
    edges: Vec<ExternalModuleEdge>,
}

impl ExternalModuleVisitor {
    fn new(file: &Path) -> Self {
        let parent = file.parent().unwrap_or_else(|| Path::new("."));
        let stem = file
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("");
        let module_directory = if matches!(stem, "lib" | "main" | "mod") {
            parent.to_path_buf()
        } else {
            parent.join(stem)
        };
        Self {
            module_directory,
            path_directory: parent.to_path_buf(),
            inline_cfg_context: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn resolve_external_module(&self, item: &syn::ItemMod) -> Result<PathBuf, String> {
        if let Some(attribute) = item
            .attrs
            .iter()
            .find(|attribute| attribute.path().is_ident("path"))
        {
            let path = match &attribute.meta {
                syn::Meta::NameValue(name_value) => match &name_value.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(path),
                        ..
                    }) => path.value(),
                    _ => return Err("module path attribute must be a string".to_string()),
                },
                _ => return Err("module path attribute must be name-value".to_string()),
            };
            return Ok(self.path_directory.join(path));
        }
        let flat = self.module_directory.join(format!("{}.rs", item.ident));
        let nested = self
            .module_directory
            .join(item.ident.to_string())
            .join("mod.rs");
        match (flat.is_file(), nested.is_file()) {
            (true, false) => Ok(flat),
            (false, true) => Ok(nested),
            (false, false) => Ok(flat),
            (true, true) => Err(format!(
                "module {} resolves to both {} and {}",
                item.ident,
                flat.display(),
                nested.display()
            )),
        }
    }
}

impl<'ast> Visit<'ast> for ExternalModuleVisitor {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        let own_cfg = selected_attributes(&item.attrs, "cfg");
        if item.content.is_none() {
            match self.resolve_external_module(item) {
                Ok(target) => {
                    let mut cfg_context = self.inline_cfg_context.clone();
                    cfg_context.extend(own_cfg);
                    self.edges.push(ExternalModuleEdge {
                        target,
                        cfg_context,
                    });
                }
                Err(error) => panic!("failed to resolve module {}: {error}", item.ident),
            }
            return;
        }

        let previous_directory = self.module_directory.clone();
        let previous_cfg_len = self.inline_cfg_context.len();
        self.module_directory = self.module_directory.join(item.ident.to_string());
        self.inline_cfg_context.extend(own_cfg);
        syn::visit::visit_item_mod(self, item);
        self.inline_cfg_context.truncate(previous_cfg_len);
        self.module_directory = previous_directory;
    }
}

fn is_crate_root(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("lib.rs" | "main.rs")
    ) || path
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|parent| parent == "bin")
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?
    {
        let path = entry
            .map_err(|error| format!("failed to read entry in {}: {error}", directory.display()))?
            .path();
        if path.is_dir() {
            collect_rust_files(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

#[derive(Default)]
struct ContractVisitor {
    lexical_path: Vec<String>,
    cfg_context: Vec<String>,
    items: Vec<DiscoveredItem>,
    manual_directions: BTreeMap<ItemIdentity, DirectionContract>,
    facade_ownership: bool,
    runtime_directions: BTreeMap<ItemIdentity, DirectionContract>,
    schema_bindings: BTreeMap<ItemIdentity, Vec<DirectionBindingContract>>,
    errors: Vec<String>,
}

#[derive(Clone)]
struct DiscoveredItem {
    identity: ItemIdentity,
    contract: SourceContract,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ItemIdentity {
    lexical_path: Vec<String>,
    rust_name: String,
}

impl ContractVisitor {
    fn record_struct(&mut self, item: &ItemStruct) {
        self.record_derived_ownership(&item.ident.to_string(), &item.attrs);
        let parsed = match parse_serde_attributes(&item.attrs, SerdeAttributeSite::Container) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.errors.push(format!("{}: {error}", item.ident));
                return;
            }
        };
        let fields = match fields_contract(&item.fields, &parsed.rename_all) {
            Ok(fields) => fields,
            Err(error) => {
                self.errors.push(format!("{} fields: {error}", item.ident));
                return;
            }
        };
        self.items.push(DiscoveredItem {
            identity: ItemIdentity {
                lexical_path: self.lexical_path.clone(),
                rust_name: item.ident.to_string(),
            },
            contract: SourceContract {
                lexical_path: self.lexical_path.clone(),
                rust_name: item.ident.to_string(),
                kind: "struct".to_string(),
                shape: fields_shape(&item.fields).to_string(),
                visibility: visibility(&item.vis),
                generics: normalize_tokens(&item.generics),
                cfg_context: self.cfg_context.clone(),
                cfg_attributes: selected_attributes(&item.attrs, "cfg"),
                serde_attributes: selected_attributes(&item.attrs, "serde"),
                serde_semantics: container_semantics(&parsed),
                directions: derive_directions(&item.attrs, &self.cfg_context),
                fields,
                variants: Vec::new(),
            },
        });
    }

    fn record_enum(&mut self, item: &ItemEnum) {
        self.record_derived_ownership(&item.ident.to_string(), &item.attrs);
        let parsed = match parse_serde_attributes(&item.attrs, SerdeAttributeSite::Container) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.errors.push(format!("{}: {error}", item.ident));
                return;
            }
        };
        let mut variants = Vec::with_capacity(item.variants.len());
        for (order, variant) in item.variants.iter().enumerate() {
            let variant_parsed =
                match parse_serde_attributes(&variant.attrs, SerdeAttributeSite::Variant) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        self.errors
                            .push(format!("{}::{}: {error}", item.ident, variant.ident));
                        return;
                    }
                };
            let field_rule = variant_parsed.rename_all.over(&parsed.rename_all_fields);
            let fields = match fields_contract(&variant.fields, &field_rule) {
                Ok(fields) => fields,
                Err(error) => {
                    self.errors
                        .push(format!("{}::{} fields: {error}", item.ident, variant.ident));
                    return;
                }
            };
            variants.push(VariantContract {
                order,
                rust_name: variant.ident.to_string(),
                shape: fields_shape(&variant.fields).to_string(),
                cfg_attributes: selected_attributes(&variant.attrs, "cfg"),
                rust_attributes: selected_attributes(&variant.attrs, "default"),
                serde_attributes: selected_attributes(&variant.attrs, "serde"),
                serde_semantics: variant_semantics(
                    &variant.ident.to_string(),
                    &variant_parsed,
                    &parsed.rename_all,
                    &field_rule,
                    variant
                        .attrs
                        .iter()
                        .any(|attribute| attribute.path().is_ident("default")),
                ),
                fields,
            });
        }
        self.items.push(DiscoveredItem {
            identity: ItemIdentity {
                lexical_path: self.lexical_path.clone(),
                rust_name: item.ident.to_string(),
            },
            contract: SourceContract {
                lexical_path: self.lexical_path.clone(),
                rust_name: item.ident.to_string(),
                kind: "enum".to_string(),
                shape: "enum".to_string(),
                visibility: visibility(&item.vis),
                generics: normalize_tokens(&item.generics),
                cfg_context: self.cfg_context.clone(),
                cfg_attributes: selected_attributes(&item.attrs, "cfg"),
                serde_attributes: selected_attributes(&item.attrs, "serde"),
                serde_semantics: container_semantics(&parsed),
                directions: derive_directions(&item.attrs, &self.cfg_context),
                fields: Vec::new(),
                variants,
            },
        });
    }

    fn record_derived_ownership(&mut self, rust_name: &str, attributes: &[Attribute]) {
        if !self.facade_ownership {
            return;
        }
        let identity = ItemIdentity {
            lexical_path: self.lexical_path.clone(),
            rust_name: rust_name.to_string(),
        };
        let cfg_attributes = selected_attributes(attributes, "cfg");
        for attribute in attributes
            .iter()
            .filter(|attribute| attribute.path().is_ident("derive"))
        {
            let paths = match attribute
                .parse_args_with(Punctuated::<SynPath, Token![,]>::parse_terminated)
            {
                Ok(paths) => paths,
                Err(error) => {
                    self.errors
                        .push(format!("invalid derive for {identity:?}: {error}"));
                    return;
                }
            };
            for path in paths {
                match path_last_ident(&path).as_deref() {
                    Some("JsonSchema") => push_unique_binding(
                        self.schema_bindings.entry(identity.clone()).or_default(),
                        "derive",
                        &self.cfg_context,
                        &cfg_attributes,
                    ),
                    Some("RuntimeEncode") => self
                        .runtime_directions
                        .entry(identity.clone())
                        .or_default()
                        .add_encode("runtime-derive", &self.cfg_context, &cfg_attributes),
                    Some("RuntimeDecode") => self
                        .runtime_directions
                        .entry(identity.clone())
                        .or_default()
                        .add_decode("runtime-derive", &self.cfg_context, &cfg_attributes),
                    _ => {}
                }
            }
        }
    }

    fn record_manual_impl(&mut self, item: &ItemImpl) {
        let Some((_, trait_path, _)) = &item.trait_ else {
            return;
        };
        let Some(trait_name) = path_last_ident(trait_path) else {
            return;
        };
        if !matches!(
            trait_name.as_str(),
            "Serialize" | "Deserialize" | "JsonSchema" | "RuntimeEncode" | "RuntimeDecode"
        ) {
            return;
        }
        if !self.facade_ownership && !matches!(trait_name.as_str(), "Serialize" | "Deserialize") {
            return;
        }
        let Some(rust_name) = type_last_ident(&item.self_ty) else {
            if self.facade_ownership {
                self.errors.push(format!(
                    "{trait_name} impl has no identifiable struct or enum self type"
                ));
            }
            return;
        };
        if self.facade_ownership
            && !matches!(
                item.self_ty.as_ref(),
                Type::Path(path)
                    if path.qself.is_none()
                        && path.path.leading_colon.is_none()
                        && path.path.segments.len() == 1
                        && matches!(&path.path.segments[0].arguments, syn::PathArguments::None)
            )
        {
            self.errors.push(format!(
                "{trait_name} impl for {} must identify an unqualified declaration without type arguments in its lexical scope",
                normalize_tokens(item.self_ty.as_ref())
            ));
            return;
        }
        let identity = ItemIdentity {
            lexical_path: self.lexical_path.clone(),
            rust_name,
        };
        let cfg_attributes = selected_attributes(&item.attrs, "cfg");
        match trait_name.as_str() {
            "Serialize" => self
                .manual_directions
                .entry(identity)
                .or_default()
                .add_encode("manual", &self.cfg_context, &cfg_attributes),
            "Deserialize" => self
                .manual_directions
                .entry(identity)
                .or_default()
                .add_decode("manual", &self.cfg_context, &cfg_attributes),
            "JsonSchema" => push_unique_binding(
                self.schema_bindings.entry(identity).or_default(),
                "manual",
                &self.cfg_context,
                &cfg_attributes,
            ),
            "RuntimeEncode" => self
                .runtime_directions
                .entry(identity)
                .or_default()
                .add_encode("runtime-manual", &self.cfg_context, &cfg_attributes),
            "RuntimeDecode" => self
                .runtime_directions
                .entry(identity)
                .or_default()
                .add_decode("runtime-manual", &self.cfg_context, &cfg_attributes),
            _ => unreachable!("trait name was checked"),
        }
    }

    fn unique_item_index(&self, identity: &ItemIdentity, owner: &str) -> Result<usize, String> {
        let matches = self
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.identity == *identity)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Err(format!(
                "{owner} impl for {identity:?} has no struct or enum declaration"
            )),
            [index] => Ok(*index),
            _ => Err(format!("{owner} impl for {identity:?} is ambiguous")),
        }
    }

    fn apply_manual_directions(&mut self) -> Result<(), String> {
        if !self.errors.is_empty() {
            return Err(format!(
                "failed to normalize serde metadata: {}",
                self.errors.join("; ")
            ));
        }
        for (identity, directions) in &self.manual_directions {
            if !directions.can_encode() && !directions.can_decode() {
                continue;
            }
            let matches = self
                .items
                .iter()
                .enumerate()
                .filter(|(_, item)| item.identity == *identity)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [] => {
                    return Err(format!(
                        "manual serde impl for {identity:?} has no struct or enum declaration"
                    ));
                }
                [index] => self.items[*index].contract.directions.merge(directions),
                _ => {
                    return Err(format!("manual serde impl for {identity:?} is ambiguous"));
                }
            }
        }
        Ok(())
    }

    fn with_scope(&mut self, label: String, attrs: &[Attribute], visit: impl FnOnce(&mut Self)) {
        self.lexical_path.push(label);
        let cfg = selected_attributes(attrs, "cfg");
        let original_cfg_len = self.cfg_context.len();
        self.cfg_context.extend(cfg);
        visit(self);
        self.cfg_context.truncate(original_cfg_len);
        self.lexical_path.pop();
    }
}

impl<'ast> Visit<'ast> for ContractVisitor {
    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        self.record_struct(item);
        syn::visit::visit_item_struct(self, item);
    }

    fn visit_item_enum(&mut self, item: &'ast ItemEnum) {
        self.record_enum(item);
        syn::visit::visit_item_enum(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        self.record_manual_impl(item);
        let label = format!("impl:{}", normalize_tokens(item.self_ty.as_ref()));
        self.with_scope(label, &item.attrs, |visitor| {
            syn::visit::visit_item_impl(visitor, item)
        });
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        let label = format!("mod:{}", item.ident);
        self.with_scope(label, &item.attrs, |visitor| {
            syn::visit::visit_item_mod(visitor, item)
        });
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        let label = format!("fn:{}", item.sig.ident);
        self.with_scope(label, &item.attrs, |visitor| {
            syn::visit::visit_item_fn(visitor, item)
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        let label = format!("fn:{}", item.sig.ident);
        self.with_scope(label, &item.attrs, |visitor| {
            syn::visit::visit_impl_item_fn(visitor, item)
        });
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        let label = format!("trait-fn:{}", item.sig.ident);
        self.with_scope(label, &item.attrs, |visitor| {
            syn::visit::visit_trait_item_fn(visitor, item)
        });
    }
}

fn derive_directions(attributes: &[Attribute], cfg_context: &[String]) -> DirectionContract {
    let mut directions = DirectionContract::default();
    let cfg_attributes = selected_attributes(attributes, "cfg");
    for attribute in attributes {
        if attribute.path().is_ident("derive") {
            if let Ok(paths) =
                attribute.parse_args_with(Punctuated::<SynPath, Token![,]>::parse_terminated)
            {
                for path in paths {
                    match path_last_ident(&path).as_deref() {
                        Some("Serialize") => {
                            directions.add_encode("derive", cfg_context, &cfg_attributes)
                        }
                        Some("Deserialize") => {
                            directions.add_decode("derive", cfg_context, &cfg_attributes)
                        }
                        _ => {}
                    }
                }
            }
        } else if attribute.path().is_ident("cfg_attr") {
            let tokens = normalize_tokens(&attribute.meta);
            if tokens.contains("derive") && tokens.contains("Serialize") {
                directions.add_encode("cfg_attr", cfg_context, &cfg_attributes);
            }
            if tokens.contains("derive") && tokens.contains("Deserialize") {
                directions.add_decode("cfg_attr", cfg_context, &cfg_attributes);
            }
        }
    }
    directions
}

#[derive(Default)]
struct ParsedSerde {
    rename: DirectionalStringContract,
    rename_all: DirectionalStringContract,
    rename_all_fields: DirectionalStringContract,
    aliases: Vec<String>,
    default: DefaultContract,
    skip: SkipContract,
    flatten: bool,
    deny_unknown_fields: bool,
    transparent: bool,
    tag: Option<String>,
    content: Option<String>,
    untagged: bool,
    other: bool,
    with: Option<String>,
    serialize_with: Option<String>,
    deserialize_with: Option<String>,
    from_type: Option<String>,
    try_from_type: Option<String>,
    into_type: Option<String>,
}

#[derive(Clone, Copy)]
enum SerdeAttributeSite {
    Container,
    Field,
    Variant,
}

fn parse_serde_attributes(
    attributes: &[Attribute],
    site: SerdeAttributeSite,
) -> Result<ParsedSerde, String> {
    let mut parsed = ParsedSerde::default();
    let mut borrow_seen = false;
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("serde"))
    {
        attribute
            .parse_nested_meta(|meta| {
                if meta.path.is_ident("borrow") {
                    if !matches!(site, SerdeAttributeSite::Field) {
                        return Err(meta.error("serde borrow metadata is only supported on fields"));
                    }
                    if borrow_seen {
                        return Err(meta.error("duplicate serde borrow metadata"));
                    }
                    borrow_seen = true;
                    return parse_borrow_metadata(meta);
                }
                if meta.path.is_ident("from")
                    || meta.path.is_ident("try_from")
                    || meta.path.is_ident("into")
                {
                    if !matches!(site, SerdeAttributeSite::Container) {
                        return Err(
                            meta.error("serde conversion metadata is only valid on containers")
                        );
                    }
                    let target = if meta.path.is_ident("from") {
                        &mut parsed.from_type
                    } else if meta.path.is_ident("try_from") {
                        &mut parsed.try_from_type
                    } else {
                        &mut parsed.into_type
                    };
                    return parse_conversion_type(meta, target);
                }
                if meta.path.is_ident("rename") {
                    return parse_directional_string(meta, &mut parsed.rename);
                }
                if meta.path.is_ident("rename_all") {
                    return parse_directional_string(meta, &mut parsed.rename_all);
                }
                if meta.path.is_ident("rename_all_fields") {
                    return parse_directional_string(meta, &mut parsed.rename_all_fields);
                }
                if meta.path.is_ident("alias") {
                    parsed.aliases.push(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("default") {
                    parsed.default.enabled = true;
                    if meta.input.peek(Token![=]) {
                        parsed.default.function = Some(parse_string_value(&meta)?);
                    }
                    return Ok(());
                }
                if meta.path.is_ident("skip") {
                    parsed.skip.serialize = true;
                    parsed.skip.deserialize = true;
                    return Ok(());
                }
                if meta.path.is_ident("skip_serializing") {
                    parsed.skip.serialize = true;
                    return Ok(());
                }
                if meta.path.is_ident("skip_deserializing") {
                    parsed.skip.deserialize = true;
                    return Ok(());
                }
                if meta.path.is_ident("skip_serializing_if") {
                    parsed.skip.serialize_if = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("flatten") {
                    parsed.flatten = true;
                    return Ok(());
                }
                if meta.path.is_ident("deny_unknown_fields") {
                    parsed.deny_unknown_fields = true;
                    return Ok(());
                }
                if meta.path.is_ident("transparent") {
                    parsed.transparent = true;
                    return Ok(());
                }
                if meta.path.is_ident("tag") {
                    parsed.tag = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("content") {
                    parsed.content = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("untagged") {
                    parsed.untagged = true;
                    return Ok(());
                }
                if meta.path.is_ident("other") {
                    parsed.other = true;
                    return Ok(());
                }
                if meta.path.is_ident("with") {
                    parsed.with = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("serialize_with") {
                    parsed.serialize_with = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                if meta.path.is_ident("deserialize_with") {
                    parsed.deserialize_with = Some(parse_string_value(&meta)?);
                    return Ok(());
                }
                Err(meta.error(format!(
                    "unsupported serde metadata `{}`",
                    normalize_tokens(&meta.path)
                )))
            })
            .map_err(|error| error.to_string())?;
    }
    if parsed.from_type.is_some() && parsed.try_from_type.is_some() {
        return Err("serde from and try_from conflict with each other".to_string());
    }
    if parsed.transparent
        && (parsed.from_type.is_some()
            || parsed.try_from_type.is_some()
            || parsed.into_type.is_some())
    {
        return Err("serde transparent conflicts with conversion metadata".to_string());
    }
    if parsed.content.is_some() && parsed.tag.is_none() {
        return Err("serde content requires a tag".to_string());
    }
    if parsed.untagged && (parsed.tag.is_some() || parsed.content.is_some()) {
        return Err("serde untagged conflicts with tag/content".to_string());
    }
    Ok(parsed)
}

fn parse_borrow_metadata(meta: syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if !meta.input.peek(Token![=]) {
        return Ok(());
    }
    let lifetimes = meta
        .value()?
        .parse::<LitStr>()?
        .parse_with(Punctuated::<syn::Lifetime, Token![+]>::parse_terminated)
        .map_err(|error| meta.error(format!("invalid serde borrowed lifetimes: {error}")))?;
    if lifetimes.is_empty() {
        return Err(meta.error("serde borrow requires at least one lifetime"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for lifetime in lifetimes {
        if !seen.insert(lifetime.ident.to_string()) {
            return Err(meta.error("duplicate serde borrowed lifetime"));
        }
    }
    Ok(())
}

fn parse_conversion_type(
    meta: syn::meta::ParseNestedMeta<'_>,
    target: &mut Option<String>,
) -> syn::Result<()> {
    if target.is_some() {
        return Err(meta.error("duplicate serde conversion target"));
    }
    let value = parse_string_value(&meta)?;
    syn::parse_str::<Type>(&value)
        .map_err(|error| meta.error(format!("invalid serde conversion type: {error}")))?;
    *target = Some(value);
    Ok(())
}

fn parse_directional_string(
    meta: syn::meta::ParseNestedMeta<'_>,
    target: &mut DirectionalStringContract,
) -> syn::Result<()> {
    if meta.input.peek(Token![=]) {
        let value = parse_string_value(&meta)?;
        target.serialize = Some(value.clone());
        target.deserialize = Some(value);
        return Ok(());
    }
    meta.parse_nested_meta(|nested| {
        let value = parse_string_value(&nested)?;
        if nested.path.is_ident("serialize") {
            target.serialize = Some(value);
            Ok(())
        } else if nested.path.is_ident("deserialize") {
            target.deserialize = Some(value);
            Ok(())
        } else {
            Err(nested.error("expected serialize or deserialize"))
        }
    })
}

fn parse_string_value(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<String> {
    meta.value()?.parse::<LitStr>().map(|value| value.value())
}

fn container_semantics(parsed: &ParsedSerde) -> ContainerSerdeContract {
    let tagging = if parsed.untagged {
        TaggingContract {
            kind: "untagged".to_string(),
            tag: None,
            content: None,
        }
    } else if parsed.tag.is_some() && parsed.content.is_some() {
        TaggingContract {
            kind: "adjacent".to_string(),
            tag: parsed.tag.clone(),
            content: parsed.content.clone(),
        }
    } else if parsed.tag.is_some() {
        TaggingContract {
            kind: "internal".to_string(),
            tag: parsed.tag.clone(),
            content: None,
        }
    } else {
        TaggingContract::default()
    };
    ContainerSerdeContract {
        rename: parsed.rename.clone(),
        rename_all: parsed.rename_all.clone(),
        rename_all_fields: parsed.rename_all_fields.clone(),
        deny_unknown_fields: parsed.deny_unknown_fields,
        default: parsed.default.clone(),
        transparent: parsed.transparent,
        deserialize_from: parsed.from_type.clone(),
        deserialize_try_from: parsed.try_from_type.clone(),
        serialize_into: parsed.into_type.clone(),
        tagging,
    }
}

fn variant_semantics(
    rust_name: &str,
    parsed: &ParsedSerde,
    variant_rename_rule: &DirectionalStringContract,
    field_rename_rule: &DirectionalStringContract,
    rust_default: bool,
) -> VariantSerdeContract {
    VariantSerdeContract {
        canonical_name: Some(canonical_name(
            rust_name,
            &parsed.rename,
            variant_rename_rule,
            NameKind::Variant,
        )),
        aliases: parsed.aliases.clone(),
        rename_all_fields: field_rename_rule.clone(),
        skip: parsed.skip.clone(),
        other: parsed.other,
        rust_default,
        adapters: adapter_semantics(parsed),
    }
}

fn field_semantics(
    rust_name: Option<&str>,
    parsed: &ParsedSerde,
    rename_rule: &DirectionalStringContract,
) -> FieldSerdeContract {
    FieldSerdeContract {
        canonical_name: rust_name.map(|rust_name| {
            canonical_name(rust_name, &parsed.rename, rename_rule, NameKind::Field)
        }),
        aliases: parsed.aliases.clone(),
        default: parsed.default.clone(),
        skip: parsed.skip.clone(),
        flatten: parsed.flatten,
        adapters: adapter_semantics(parsed),
    }
}

fn adapter_semantics(parsed: &ParsedSerde) -> AdapterContract {
    AdapterContract {
        with: parsed.with.clone(),
        serialize_with: parsed.serialize_with.clone(),
        deserialize_with: parsed.deserialize_with.clone(),
        effective_serialize_with: parsed.serialize_with.clone().or_else(|| {
            parsed
                .with
                .as_ref()
                .map(|path| format!("{path}::serialize"))
        }),
        effective_deserialize_with: parsed.deserialize_with.clone().or_else(|| {
            parsed
                .with
                .as_ref()
                .map(|path| format!("{path}::deserialize"))
        }),
    }
}

#[derive(Clone, Copy)]
enum NameKind {
    Field,
    Variant,
}

fn canonical_name(
    rust_name: &str,
    explicit: &DirectionalStringContract,
    rename_rule: &DirectionalStringContract,
    kind: NameKind,
) -> CanonicalNameContract {
    let rust_name = rust_name.strip_prefix("r#").unwrap_or(rust_name);
    CanonicalNameContract {
        serialize: explicit.serialize.clone().unwrap_or_else(|| {
            rename_rule.serialize.as_deref().map_or_else(
                || rust_name.to_string(),
                |rule| apply_rename(rule, rust_name, kind),
            )
        }),
        deserialize: explicit.deserialize.clone().unwrap_or_else(|| {
            rename_rule.deserialize.as_deref().map_or_else(
                || rust_name.to_string(),
                |rule| apply_rename(rule, rust_name, kind),
            )
        }),
    }
}

fn apply_rename(rule: &str, name: &str, kind: NameKind) -> String {
    match (rule, kind) {
        ("lowercase", NameKind::Field) => name.to_string(),
        ("lowercase", NameKind::Variant) => name.to_ascii_lowercase(),
        ("UPPERCASE", _) => name.to_ascii_uppercase(),
        ("PascalCase", NameKind::Field) => field_pascal_case(name),
        ("PascalCase", NameKind::Variant) => name.to_string(),
        ("camelCase", NameKind::Field) => lower_first(&field_pascal_case(name)),
        ("camelCase", NameKind::Variant) => lower_first(name),
        ("snake_case", NameKind::Field) => name.to_string(),
        ("snake_case", NameKind::Variant) => variant_snake_case(name),
        ("SCREAMING_SNAKE_CASE", NameKind::Field) => name.to_ascii_uppercase(),
        ("SCREAMING_SNAKE_CASE", NameKind::Variant) => {
            variant_snake_case(name).to_ascii_uppercase()
        }
        ("kebab-case", NameKind::Field) => name.replace('_', "-"),
        ("kebab-case", NameKind::Variant) => variant_snake_case(name).replace('_', "-"),
        ("SCREAMING-KEBAB-CASE", NameKind::Field) => name.to_ascii_uppercase().replace('_', "-"),
        ("SCREAMING-KEBAB-CASE", NameKind::Variant) => variant_snake_case(name)
            .to_ascii_uppercase()
            .replace('_', "-"),
        _ => panic!("unsupported serde rename rule {rule:?}"),
    }
}

fn field_pascal_case(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut capitalize = true;
    for character in name.chars() {
        if character == '_' {
            capitalize = true;
        } else if capitalize {
            result.push(character.to_ascii_uppercase());
            capitalize = false;
        } else {
            result.push(character);
        }
    }
    result
}

fn lower_first(name: &str) -> String {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return String::new();
    };
    std::iter::once(first.to_ascii_lowercase())
        .chain(characters)
        .collect()
}

fn variant_snake_case(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    for (index, character) in name.char_indices() {
        if index > 0 && character.is_uppercase() {
            result.push('_');
        }
        result.push(character.to_ascii_lowercase());
    }
    result
}

fn fields_contract(
    fields: &Fields,
    rename_rule: &DirectionalStringContract,
) -> Result<Vec<FieldContract>, String> {
    fields
        .iter()
        .enumerate()
        .map(|(order, field)| {
            let parsed = parse_serde_attributes(&field.attrs, SerdeAttributeSite::Field)?;
            let rust_name = field.ident.as_ref().map(ToString::to_string);
            Ok(FieldContract {
                order,
                rust_name: rust_name.clone(),
                rust_type: normalize_tokens(&field.ty),
                visibility: visibility(&field.vis),
                cfg_attributes: selected_attributes(&field.attrs, "cfg"),
                serde_attributes: selected_attributes(&field.attrs, "serde"),
                serde_semantics: field_semantics(rust_name.as_deref(), &parsed, rename_rule),
            })
        })
        .collect()
}

fn fields_shape(fields: &Fields) -> &'static str {
    match fields {
        Fields::Named(_) => "named",
        Fields::Unnamed(_) => "unnamed",
        Fields::Unit => "unit",
    }
}

fn selected_attributes(attributes: &[Attribute], name: &str) -> Vec<String> {
    attributes
        .iter()
        .filter(|attribute| {
            attribute.path().is_ident(name)
                || (name == "cfg" && attribute.path().is_ident("cfg_attr"))
        })
        .map(|attribute| normalize_tokens(&attribute.meta))
        .collect()
}

fn visibility(visibility: &syn::Visibility) -> String {
    match visibility {
        syn::Visibility::Inherited => "private".to_string(),
        visibility => normalize_tokens(visibility),
    }
}

fn path_last_ident(path: &SynPath) -> Option<String> {
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn type_last_ident(value: &Type) -> Option<String> {
    match value {
        Type::Path(path) => path_last_ident(&path.path),
        Type::Group(group) => type_last_ident(&group.elem),
        Type::Paren(paren) => type_last_ident(&paren.elem),
        _ => None,
    }
}

fn normalize_tokens(value: &impl ToTokens) -> String {
    value.to_token_stream().to_string()
}

fn qualified_symbol_key(relative: &str, identity: &ItemIdentity) -> String {
    let mut key = relative.to_string();
    for segment in &identity.lexical_path {
        key.push_str("::");
        key.push_str(segment);
    }
    key.push_str("::");
    key.push_str(&identity.rust_name);
    key
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

fn push_unique_binding(
    bindings: &mut Vec<DirectionBindingContract>,
    origin: &str,
    cfg_context: &[String],
    cfg_attributes: &[String],
) {
    let binding = DirectionBindingContract {
        origin: origin.to_string(),
        cfg_context: cfg_context.to_vec(),
        cfg_attributes: cfg_attributes.to_vec(),
    };
    if !bindings.contains(&binding) {
        bindings.push(binding);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_module_cfg_contexts, scan_repository, scan_serde_facade_source_with_context,
        scan_source, scan_source_with_context, ContainerSerdeContract, SourceContract,
    };
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = TEMP_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "klaus-runtime-wire-ast-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn only_contract(source: &str) -> SourceContract {
        let facts = scan_source("crates/example/src/lib.rs", source).unwrap();
        assert_eq!(facts.len(), 1);
        facts.into_values().next().unwrap()
    }

    #[test]
    fn lexical_identity_keeps_same_named_local_manual_codecs_distinct() {
        let source = r#"
            fn encode_owner() {
                struct LocalWire;
                impl serde::Serialize for LocalWire {}
            }

            fn decode_owner() {
                struct LocalWire;
                impl<'de> serde::Deserialize<'de> for LocalWire {}
            }
        "#;
        let facts = scan_source("crates/example/src/lib.rs", source).unwrap();
        assert_eq!(facts.len(), 2);

        let encode = facts
            .get("crates/example/src/lib.rs::fn:encode_owner::LocalWire")
            .unwrap();
        assert!(encode.can_encode());
        assert!(!encode.can_decode());

        let decode = facts
            .get("crates/example/src/lib.rs::fn:decode_owner::LocalWire")
            .unwrap();
        assert!(!decode.can_encode());
        assert!(decode.can_decode());
    }

    #[test]
    fn manual_impl_direction_freezes_impl_and_module_cfg() {
        let facts = scan_source(
            "crates/example/src/lib.rs",
            r#"
                #[cfg(target_os = "linux")]
                mod scoped {
                    struct ManualWire;

                    #[cfg(feature = "wire-encode")]
                    impl serde::Serialize for ManualWire {}
                }
            "#,
        )
        .unwrap();
        let contract = facts.into_values().next().unwrap();
        let [binding] = contract.directions.encode_bindings.as_slice() else {
            panic!("expected one encode binding");
        };
        assert_eq!(binding.origin, "manual");
        assert_eq!(binding.cfg_context, ["cfg (target_os = \"linux\")"]);
        assert_eq!(binding.cfg_attributes, ["cfg (feature = \"wire-encode\")"]);
    }

    #[test]
    fn external_module_cfg_reaches_type_and_manual_direction() {
        let directory = TestDirectory::new();
        let root = directory.0.join("lib.rs");
        let module = directory.0.join("wire.rs");
        fs::write(
            &root,
            r#"
                #[cfg(target_os = "linux")]
                mod wire;
            "#,
        )
        .unwrap();
        let module_source = r#"
            struct ManualWire;
            impl serde::Serialize for ManualWire {}
        "#;
        fs::write(&module, module_source).unwrap();

        let contexts = resolve_module_cfg_contexts(&[root, module.clone()]).unwrap();
        let inherited = contexts.get(&module).cloned().unwrap();
        assert_eq!(inherited, ["cfg (target_os = \"linux\")"]);

        let facts = scan_source_with_context("wire.rs", module_source, inherited).unwrap();
        let contract = facts.into_values().next().unwrap();
        assert_eq!(contract.cfg_context, ["cfg (target_os = \"linux\")"]);
        let [binding] = contract.directions.encode_bindings.as_slice() else {
            panic!("expected one encode binding");
        };
        assert_eq!(binding.cfg_context, ["cfg (target_os = \"linux\")"]);
    }

    #[test]
    fn direction_split_rename_alias_order_and_container_policies_are_materialized() {
        let contract = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(
                    rename(serialize = "WireOut", deserialize = "WireIn"),
                    rename_all(serialize = "camelCase", deserialize = "kebab-case"),
                    deny_unknown_fields,
                    default
                )]
                struct SplitNames {
                    plain_field: String,
                    #[serde(
                        rename(serialize = "output", deserialize = "input"),
                        alias = "legacyOne",
                        alias = "legacyTwo"
                    )]
                    explicit_field: String,
                }
            "#,
        );
        assert_eq!(
            contract.serde_semantics.rename.serialize.as_deref(),
            Some("WireOut")
        );
        assert_eq!(
            contract.serde_semantics.rename.deserialize.as_deref(),
            Some("WireIn")
        );
        assert!(contract.serde_semantics.deny_unknown_fields);
        assert!(contract.serde_semantics.default.enabled);

        let plain = &contract.fields[0].serde_semantics;
        let plain_name = plain.canonical_name.as_ref().unwrap();
        assert_eq!(plain_name.serialize, "plainField");
        assert_eq!(plain_name.deserialize, "plain-field");

        let explicit = &contract.fields[1].serde_semantics;
        let explicit_name = explicit.canonical_name.as_ref().unwrap();
        assert_eq!(explicit_name.serialize, "output");
        assert_eq!(explicit_name.deserialize, "input");
        assert_eq!(explicit.aliases, ["legacyOne", "legacyTwo"]);
    }

    #[test]
    fn defaults_skips_flatten_and_adapters_are_materialized() {
        let contract = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                struct Policies {
                    #[serde(default)]
                    implicit_default: String,
                    #[serde(default = "make_default")]
                    named_default: String,
                    #[serde(skip)]
                    skipped: String,
                    #[serde(skip_serializing, skip_serializing_if = "omit_value")]
                    encode_skip: String,
                    #[serde(skip_deserializing)]
                    decode_skip: String,
                    #[serde(flatten)]
                    rest: Map,
                    #[serde(with = "wire")]
                    bidirectional_adapter: Value,
                    #[serde(serialize_with = "encode", deserialize_with = "decode")]
                    split_adapter: Value,
                }
            "#,
        );
        assert!(contract.fields[0].serde_semantics.default.enabled);
        assert_eq!(
            contract.fields[1]
                .serde_semantics
                .default
                .function
                .as_deref(),
            Some("make_default")
        );
        assert!(contract.fields[2].serde_semantics.skip.serialize);
        assert!(contract.fields[2].serde_semantics.skip.deserialize);
        assert!(contract.fields[3].serde_semantics.skip.serialize);
        assert_eq!(
            contract.fields[3]
                .serde_semantics
                .skip
                .serialize_if
                .as_deref(),
            Some("omit_value")
        );
        assert!(contract.fields[4].serde_semantics.skip.deserialize);
        assert!(contract.fields[5].serde_semantics.flatten);

        let with = &contract.fields[6].serde_semantics.adapters;
        assert_eq!(with.with.as_deref(), Some("wire"));
        assert_eq!(
            with.effective_serialize_with.as_deref(),
            Some("wire::serialize")
        );
        assert_eq!(
            with.effective_deserialize_with.as_deref(),
            Some("wire::deserialize")
        );
        let split = &contract.fields[7].serde_semantics.adapters;
        assert_eq!(split.effective_serialize_with.as_deref(), Some("encode"));
        assert_eq!(split.effective_deserialize_with.as_deref(), Some("decode"));
    }

    #[test]
    fn all_enum_tagging_forms_and_variant_aliases_are_materialized() {
        let facts = scan_source(
            "crates/example/src/lib.rs",
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                enum External {
                    #[serde(
                        rename(serialize = "wireOut", deserialize = "wireIn"),
                        alias = "oldOne",
                        alias = "oldTwo"
                    )]
                    Named { value: String },
                }

                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(tag = "type")]
                enum Internal { Named { value: String } }

                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(tag = "kind", content = "payload")]
                enum Adjacent { Named(String) }

                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(untagged)]
                enum Untagged { Text(String), Number(u64) }
            "#,
        )
        .unwrap();
        let by_name = |name: &str| {
            facts
                .values()
                .find(|contract| contract.rust_name == name)
                .unwrap()
        };
        assert_eq!(by_name("External").serde_semantics.tagging.kind, "external");
        assert_eq!(by_name("Internal").serde_semantics.tagging.kind, "internal");
        assert_eq!(
            by_name("Internal").serde_semantics.tagging.tag.as_deref(),
            Some("type")
        );
        assert_eq!(by_name("Adjacent").serde_semantics.tagging.kind, "adjacent");
        assert_eq!(
            by_name("Adjacent")
                .serde_semantics
                .tagging
                .content
                .as_deref(),
            Some("payload")
        );
        assert_eq!(by_name("Untagged").serde_semantics.tagging.kind, "untagged");

        let variant = &by_name("External").variants[0].serde_semantics;
        let name = variant.canonical_name.as_ref().unwrap();
        assert_eq!(name.serialize, "wireOut");
        assert_eq!(name.deserialize, "wireIn");
        assert_eq!(variant.aliases, ["oldOne", "oldTwo"]);
    }

    #[test]
    fn variant_field_rules_apply_with_explicit_and_variant_overrides() {
        let contract = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(
                    rename_all = "snake_case",
                    rename_all_fields(serialize = "camelCase", deserialize = "kebab-case")
                )]
                enum FieldRules {
                    StructVariant {
                        some_value: String,
                        #[serde(rename(serialize = "wireOut", deserialize = "wireIn"))]
                        explicit_value: String,
                    },
                    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
                    LocalOverride { local_value: String },
                }
            "#,
        );
        let first_variant_name = contract.variants[0]
            .serde_semantics
            .canonical_name
            .as_ref()
            .unwrap();
        assert_eq!(first_variant_name.serialize, "struct_variant");
        assert_eq!(first_variant_name.deserialize, "struct_variant");

        let inherited = contract.variants[0].fields[0]
            .serde_semantics
            .canonical_name
            .as_ref()
            .unwrap();
        assert_eq!(inherited.serialize, "someValue");
        assert_eq!(inherited.deserialize, "some-value");
        let explicit = contract.variants[0].fields[1]
            .serde_semantics
            .canonical_name
            .as_ref()
            .unwrap();
        assert_eq!(explicit.serialize, "wireOut");
        assert_eq!(explicit.deserialize, "wireIn");

        let local = contract.variants[1].fields[0]
            .serde_semantics
            .canonical_name
            .as_ref()
            .unwrap();
        assert_eq!(local.serialize, "LOCAL_VALUE");
        assert_eq!(local.deserialize, "LOCAL_VALUE");
    }

    #[test]
    fn transparent_container_is_materialized() {
        let contract = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(transparent)]
                struct Wrapper(String);
            "#,
        );
        assert!(contract.serde_semantics.transparent);
    }

    #[test]
    fn rust_default_variant_is_frozen_with_enum_semantics() {
        let contract = only_contract(
            r#"
                #[derive(Default, serde::Serialize, serde::Deserialize)]
                enum Defaulted {
                    #[default]
                    Primary,
                    Secondary,
                }
            "#,
        );
        assert!(contract.variants[0].serde_semantics.rust_default);
        assert_eq!(contract.variants[0].rust_attributes, ["default"]);
        assert!(!contract.variants[1].serde_semantics.rust_default);
        assert!(contract.variants[1].rust_attributes.is_empty());
    }

    #[test]
    fn facade_schema_ownership_is_bound_to_type_scope_and_cfg() {
        for (source, expected) in [
            (
                r#"
                    #[derive(serde::Deserialize)]
                    struct Missing;
                    #[derive(schemars::JsonSchema)]
                    struct Other;
                "#,
                "Missing",
            ),
            (
                r#"
                    mod first {
                        #[derive(serde::Serialize)]
                        struct Wire;
                    }
                    mod second {
                        struct Wire;
                        impl schemars::JsonSchema for Wire {}
                    }
                "#,
                "mod:first::Wire",
            ),
            (
                r#"
                    struct Wire;
                    impl serde::Serialize for Wire {}
                    #[cfg(feature = "schema-only")]
                    impl schemars::JsonSchema for Wire {}
                "#,
                "Wire",
            ),
            (
                r#"
                    struct Wire;
                    impl serde::Serialize for Wire {}
                    mod other { struct Wire; }
                    impl schemars::JsonSchema for other::Wire {}
                "#,
                "lexical scope",
            ),
            (
                r#"
                    #[derive(schemars::JsonSchema)]
                    struct Wire;
                    mod other { struct Wire; }
                    impl serde::Serialize for other::Wire {}
                "#,
                "lexical scope",
            ),
            (
                r#"
                    #[derive(schemars::JsonSchema)]
                    struct Wire;
                    mod other { struct Wire; }
                    impl<'de> serde::Deserialize<'de> for other::Wire {}
                "#,
                "lexical scope",
            ),
            (
                r#"
                    #[derive(schemars::JsonSchema)]
                    struct Wire;
                    impl serde::Serialize for (Wire,) {}
                "#,
                "no identifiable",
            ),
            (
                r#"
                    struct Wire<T>(T);
                    impl serde::Serialize for Wire<u8> {}
                    impl schemars::JsonSchema for Wire<u16> {}
                "#,
                "without type arguments",
            ),
            (
                r#"
                    #[derive(serde::Serialize)]
                    #[cfg_attr(feature = "schema-only", derive(schemars::JsonSchema))]
                    struct Wire;
                "#,
                "Wire",
            ),
        ] {
            let error =
                scan_serde_facade_source_with_context("facade.rs", source, Vec::new()).unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn facade_manual_schema_ownership_covers_matching_conditional_serde() {
        let source = r#"
            struct Wire;
            #[cfg(feature = "wire")]
            impl serde::Serialize for Wire {}
            #[cfg(feature = "wire")]
            impl schemars::JsonSchema for Wire {}
        "#;
        assert!(scan_serde_facade_source_with_context(
            "facade.rs",
            source,
            vec!["cfg (target_os = \"linux\")".to_string()],
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn facade_native_codec_rejoins_by_identity_without_exempting_serde_peers() {
        let source = r#"
            struct Native;
            impl serde::Serialize for Native {}
            impl<'de> serde::Deserialize<'de> for Native {}
            impl schemars::JsonSchema for Native {}
            impl RuntimeEncode for Native {}
            impl RuntimeDecode for Native {}

            #[derive(serde::Serialize, schemars::JsonSchema)]
            struct Peer;
        "#;
        let facts = scan_serde_facade_source_with_context("facade.rs", source, Vec::new()).unwrap();
        assert_eq!(facts.len(), 1);
        let native = facts.get("facade.rs::Native").unwrap();
        assert!(native.can_encode());
        assert!(native.can_decode());
        assert_eq!(
            native.directions.encode_origins,
            ["manual", "runtime-manual"]
        );
        assert_eq!(
            native.directions.decode_origins,
            ["manual", "runtime-manual"]
        );
        assert!(!facts.contains_key("facade.rs::Peer"));

        let unowned_peer = format!("{source}\n#[derive(serde::Serialize)] struct Unowned;");
        let error = scan_serde_facade_source_with_context("facade.rs", &unowned_peer, Vec::new())
            .unwrap_err();
        assert!(error.contains("Unowned"), "{error}");
    }

    #[test]
    fn facade_native_codec_refuses_missing_and_ambiguous_declarations() {
        for (source, expected) in [
            ("impl RuntimeEncode for Missing {}", "no struct or enum"),
            (
                "struct Duplicate; struct Duplicate; impl RuntimeDecode for Duplicate {}",
                "ambiguous",
            ),
        ] {
            let error =
                scan_serde_facade_source_with_context("facade.rs", source, Vec::new()).unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn facade_native_codec_rejoin_retains_external_module_cfg() {
        let directory = TestDirectory::new();
        let server = directory.0.join("crates/server/src");
        let facade = server.join("app_server");
        fs::create_dir_all(&facade).unwrap();
        fs::write(server.join("lib.rs"), "mod app_server;").unwrap();
        fs::write(
            facade.join("mod.rs"),
            "#[cfg(feature = \"wire\")] mod wire;",
        )
        .unwrap();
        fs::write(
            facade.join("wire.rs"),
            r#"
                struct Native;
                impl serde::Serialize for Native {}
                impl schemars::JsonSchema for Native {}
                impl RuntimeEncode for Native {}

                #[derive(serde::Serialize, schemars::JsonSchema)]
                struct Peer;
            "#,
        )
        .unwrap();
        let facts = scan_repository(&directory.0).unwrap();
        assert_eq!(facts.len(), 1);
        let native = facts
            .get("crates/server/src/app_server/wire.rs::Native")
            .unwrap();
        assert_eq!(native.cfg_context, ["cfg (feature = \"wire\")"]);
        assert!(native
            .directions
            .encode_bindings
            .iter()
            .all(|binding| binding.cfg_context == ["cfg (feature = \"wire\")"]));
    }

    #[test]
    fn field_borrow_metadata_preserves_wire_semantics_and_sibling_adapters() {
        let source = r#"
            #[derive(serde::Serialize, serde::Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Borrowed<'a, 'b> {
                #[serde(BORROW rename = "payload", default,
                    deserialize_with = "borrowed_field",
                    skip_serializing_if = "Option::is_none")]
                value: Option<(&'a str, &'b str)>,
            }
        "#;
        let plain = only_contract(&source.replace("BORROW ", ""));
        for metadata in ["borrow,", "borrow = \"'a\",", "borrow = \"'a + 'b\","] {
            let borrowed = only_contract(&source.replace("BORROW", metadata));
            assert_eq!(borrowed.serde_semantics, plain.serde_semantics);
            assert_eq!(borrowed.directions, plain.directions);
            assert_eq!(borrowed.fields.len(), 1);
            assert_eq!(borrowed.fields[0].rust_type, plain.fields[0].rust_type);
            assert_eq!(
                borrowed.fields[0].serde_semantics,
                plain.fields[0].serde_semantics
            );
            assert_eq!(
                borrowed.fields[0]
                    .serde_semantics
                    .adapters
                    .deserialize_with
                    .as_deref(),
                Some("borrowed_field")
            );
        }
    }

    #[test]
    fn field_borrow_metadata_rejects_invalid_forms_and_unknown_siblings() {
        for metadata in [
            "borrow = 7",
            "borrow = \"\"",
            "borrow = \"not_a_lifetime\"",
            "borrow = \"'a + 7\"",
            "borrow = \"'a + 'a\"",
            "borrow, borrow",
            "borrow, borrow = \"'a\"",
            "borrow(unknown)",
            "borrow, unrecognized = \"Input\"",
        ] {
            let source = format!(
                "#[derive(serde::Deserialize)] struct Borrowed<'a> {{ #[serde({metadata})] value: Option<&'a str> }}"
            );
            assert!(scan_source("wire.rs", &source).is_err(), "{metadata}");
        }
        for source in [
            "#[derive(serde::Deserialize)] #[serde(borrow)] struct Borrowed<'a> { value: &'a str }",
            "#[derive(serde::Deserialize)] enum Borrowed<'a> { #[serde(borrow)] Value(&'a str) }",
        ] {
            let error = scan_source("wire.rs", source).unwrap_err();
            assert!(error.contains("only supported on fields"), "{error}");
        }
    }

    #[test]
    fn container_conversions_retain_exact_directional_type_metadata() {
        let converted = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(from = "wire::Input<String>", into = "wire::Output")]
                struct Converted { value: String }
            "#,
        );
        assert!(converted.can_encode());
        assert!(converted.can_decode());
        assert_eq!(
            converted.serde_semantics.deserialize_from.as_deref(),
            Some("wire::Input<String>")
        );
        assert!(converted.serde_semantics.deserialize_try_from.is_none());
        assert_eq!(
            converted.serde_semantics.serialize_into.as_deref(),
            Some("wire::Output")
        );

        let fallible = only_contract(
            r#"
                #[derive(serde::Deserialize)]
                #[serde(try_from = "wire::CheckedInput")]
                struct Checked { value: String }
            "#,
        );
        assert!(!fallible.can_encode());
        assert!(fallible.can_decode());
        assert!(fallible.serde_semantics.deserialize_from.is_none());
        assert_eq!(
            fallible.serde_semantics.deserialize_try_from.as_deref(),
            Some("wire::CheckedInput")
        );
        assert!(fallible.serde_semantics.serialize_into.is_none());
    }

    #[test]
    fn container_conversions_refuse_invalid_sites_types_and_conflicts() {
        for name in ["from", "try_from", "into"] {
            let field = format!(
                "#[derive(serde::Deserialize)] struct Wire {{ #[serde({name} = \"Input\")] value: String }}"
            );
            let variant = format!(
                "#[derive(serde::Deserialize)] enum Wire {{ #[serde({name} = \"Input\")] Value(String) }}"
            );
            for source in [field, variant] {
                let error = scan_source("wire.rs", &source).unwrap_err();
                assert!(error.contains("only valid on containers"), "{error}");
            }

            let duplicate = format!(
                "#[derive(serde::Deserialize)] #[serde({name} = \"Input\", {name} = \"Other\")] struct Wire;"
            );
            let error = scan_source("wire.rs", &duplicate).unwrap_err();
            assert!(
                error.contains("duplicate serde conversion target"),
                "{error}"
            );

            let transparent = format!(
                "#[derive(serde::Deserialize)] #[serde(transparent, {name} = \"Input\")] struct Wire(String);"
            );
            let error = scan_source("wire.rs", &transparent).unwrap_err();
            assert!(error.contains("transparent conflicts"), "{error}");
        }
        for (source, expected) in [
            (
                r#"#[derive(serde::Deserialize)] #[serde(from = "Input", try_from = "Checked")] struct Wire;"#,
                "from and try_from conflict",
            ),
            (
                r#"#[derive(serde::Deserialize)] #[serde(from = "not a Rust type!")] struct Wire;"#,
                "invalid serde conversion type",
            ),
            (
                r#"#[derive(serde::Deserialize)] #[serde(unrecognized = "Input")] struct Wire;"#,
                "unsupported serde metadata",
            ),
        ] {
            let error = scan_source("wire.rs", source).unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
        assert!(scan_source(
            "wire.rs",
            "#[derive(serde::Deserialize)] #[serde(from = 7)] struct Wire;",
        )
        .is_err());
    }

    #[test]
    fn absent_container_conversions_preserve_legacy_serde_contract_json() {
        let contract = only_contract(
            r#"
                #[derive(serde::Serialize, serde::Deserialize)]
                #[serde(rename_all = "camelCase", deny_unknown_fields)]
                struct Plain { original_field: String }
            "#,
        );
        let encoded = serde_json::to_value(&contract.serde_semantics).unwrap();
        for key in ["deserializeFrom", "deserializeTryFrom", "serializeInto"] {
            assert!(encoded.get(key).is_none());
        }
        let decoded: ContainerSerdeContract = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, contract.serde_semantics);
    }

    #[test]
    fn facade_container_conversion_still_requires_exact_schema_ownership() {
        let source = r#"
            #[derive(serde::Deserialize)]
            #[serde(try_from = "Input")]
            struct Converted;
            impl schemars::JsonSchema for Converted {}
        "#;
        assert!(
            scan_serde_facade_source_with_context("facade.rs", source, Vec::new())
                .unwrap()
                .is_empty()
        );
        let unowned = source.replace("JsonSchema for Converted", "JsonSchema for Other");
        let error =
            scan_serde_facade_source_with_context("facade.rs", &unowned, Vec::new()).unwrap_err();
        assert!(error.contains("no struct or enum declaration"), "{error}");
    }
}
