use std::cell::RefCell;
use std::cmp::Ordering;

use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::windows::{
    WindowsCwdImage, WindowsEnvironmentEntry, WindowsEnvironmentImage, WindowsEnvironmentPlatform,
    WindowsExecImage, WindowsExecImageBuildError, WindowsImageField,
    WindowsNode20ResolvedExecutable, WindowsRawEnvironmentRecord, WindowsResolvedExecutable,
};
use super::{
    NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan, RuntimeEnv,
    Rust193EnvMutation,
};
use crate::runtime_values::RuntimeString;

fn runtime(value: impl Into<RuntimeString>) -> ProcessAtom {
    ProcessAtom::Runtime(value.into())
}

fn node_plan(args: Vec<ProcessAtom>, environment: RuntimeEnv, verbatim: bool) -> NodeSpawnPlan {
    NodeSpawnPlan::from_normalized_parts(
        runtime("node"),
        runtime("custom argv0"),
        args,
        None,
        ProcessEnvironmentPlan::NodeRuntime(environment),
        false,
        false,
        verbatim,
    )
    .expect("valid normalized Windows Node plan")
}

fn node_plan_with(
    file: ProcessAtom,
    cwd: Option<ProcessAtom>,
    environment: RuntimeEnv,
) -> NodeSpawnPlan {
    NodeSpawnPlan::from_normalized_parts(
        file,
        runtime("custom argv0"),
        Vec::new(),
        cwd,
        ProcessEnvironmentPlan::NodeRuntime(environment),
        false,
        false,
        false,
    )
    .expect("valid normalized Windows Node plan")
}

fn rust_preimage(
    argv0: ProcessAtom,
    args: Vec<ProcessAtom>,
    environment: Rust193EnvironmentPlan,
) -> Rust193SpawnPreimage {
    Rust193SpawnPreimage::from_deferred_parts(
        runtime("requested.exe"),
        argv0,
        args,
        None,
        environment,
    )
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}

fn nul_terminated(value: &str) -> Vec<u16> {
    value.encode_utf16().chain([0]).collect()
}

fn block_entries(block: &[u16]) -> Vec<Vec<u16>> {
    let mut entries = Vec::new();
    let mut start = 0;
    while start < block.len() && block[start] != 0 {
        let end = block[start..]
            .iter()
            .position(|unit| *unit == 0)
            .map(|offset| start + offset)
            .expect("environment entry terminator");
        entries.push(block[start..end].to_vec());
        start = end + 1;
    }
    entries
}

type NodeSearch = (Vec<u16>, Vec<u16>, Option<Vec<u16>>);

struct FixturePlatform {
    parent: Vec<WindowsEnvironmentEntry>,
    raw_parent: Vec<WindowsRawEnvironmentRecord>,
    required: Vec<(Vec<u16>, Vec<u16>)>,
    node_cwd: Result<Vec<u16>, String>,
    node_resolution: Option<Result<WindowsNode20ResolvedExecutable, String>>,
    node_sort_error: Option<String>,
    node_searches: RefCell<Vec<NodeSearch>>,
    rust_batch: Option<(Vec<u16>, Vec<u16>)>,
    events: RefCell<Vec<&'static str>>,
    resolved_child_paths: RefCell<Vec<Option<Vec<u16>>>>,
}

impl Default for FixturePlatform {
    fn default() -> Self {
        Self {
            parent: Vec::new(),
            raw_parent: Vec::new(),
            required: Vec::new(),
            node_cwd: Ok(wide(r"C:\fixture-cwd")),
            node_resolution: None,
            node_sort_error: None,
            node_searches: RefCell::new(Vec::new()),
            rust_batch: None,
            events: RefCell::new(Vec::new()),
            resolved_child_paths: RefCell::new(Vec::new()),
        }
    }
}

impl FixturePlatform {
    fn with_parent(mut self, entries: &[(&str, &str)]) -> Self {
        self.parent = entries
            .iter()
            .map(|(key, value)| WindowsEnvironmentEntry::new(wide(key), wide(value)).unwrap())
            .collect();
        self
    }

    fn with_required(mut self, entries: &[(&str, &str)]) -> Self {
        self.required = entries
            .iter()
            .map(|(key, value)| (wide(key), wide(value)))
            .collect();
        self
    }

    fn with_raw_parent(mut self, records: &[&str]) -> Self {
        self.raw_parent = records
            .iter()
            .map(|record| WindowsRawEnvironmentRecord::new(wide(record)).unwrap())
            .collect();
        self
    }

    fn with_rust_batch(mut self, application: &str, script: &str) -> Self {
        self.rust_batch = Some((wide(application), wide(script)));
        self
    }

    fn with_node_cwd(mut self, cwd: &str) -> Self {
        self.node_cwd = Ok(wide(cwd));
        self
    }

    fn with_node_cwd_error(mut self, error: &str) -> Self {
        self.node_cwd = Err(error.to_string());
        self
    }

    fn with_node_resolution(
        mut self,
        resolution: Result<WindowsNode20ResolvedExecutable, &str>,
    ) -> Self {
        self.node_resolution = Some(resolution.map_err(str::to_string));
        self
    }

    fn with_node_sort_error(mut self, error: &str) -> Self {
        self.node_sort_error = Some(error.to_string());
        self
    }

    fn node_searches(&self) -> Vec<NodeSearch> {
        self.node_searches.borrow().clone()
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.borrow().clone()
    }

    fn resolved_child_paths(&self) -> Vec<Option<Vec<u16>>> {
        self.resolved_child_paths.borrow().clone()
    }
}

impl WindowsEnvironmentPlatform for FixturePlatform {
    fn node_current_directory(&self) -> Result<Vec<u16>, String> {
        self.events.borrow_mut().push("node-cwd");
        self.node_cwd.clone()
    }

    fn resolve_node20_executable(
        &self,
        requested: &[u16],
        cwd: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsNode20ResolvedExecutable, String> {
        self.events.borrow_mut().push("node-resolve");
        self.node_searches.borrow_mut().push((
            requested.to_vec(),
            cwd.to_vec(),
            child_path.map(|value| value.to_vec()),
        ));
        self.node_resolution.clone().unwrap_or_else(|| {
            WindowsNode20ResolvedExecutable::from_search_result(wide(r"C:\node.exe"))
                .map_err(|error| error.to_string())
        })
    }

    fn node_required_value(&self, name: &[u16]) -> Result<Option<Vec<u16>>, String> {
        Ok(self
            .required
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty()))
    }

    fn sort_libuv_environment(
        &self,
        entries: &mut [WindowsEnvironmentEntry],
    ) -> Result<(), String> {
        self.events.borrow_mut().push("node-env-sort");
        if let Some(error) = &self.node_sort_error {
            return Err(error.clone());
        }
        entries.sort_by(|left, right| fixture_libuv_cmp(left.key(), right.key()));
        Ok(())
    }

    fn compare_libuv_environment_key(
        &self,
        left: &[u16],
        right: &[u16],
    ) -> Result<Ordering, String> {
        Ok(fixture_libuv_cmp(left, right))
    }

    fn compare_ordinal_ignore_case(&self, left: &[u16], right: &[u16]) -> Result<Ordering, String> {
        Ok(fixture_case_cmp(left, right))
    }

    fn parent_environment(&self) -> Result<Vec<WindowsEnvironmentEntry>, String> {
        self.events.borrow_mut().push("capture-parent");
        Ok(self.parent.clone())
    }

    fn raw_parent_environment(&self) -> Result<Vec<WindowsRawEnvironmentRecord>, String> {
        self.events.borrow_mut().push("capture-raw-parent");
        Ok(self.raw_parent.clone())
    }

    fn resolve_rust193_executable(
        &self,
        requested: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsResolvedExecutable, String> {
        self.events.borrow_mut().push("resolve");
        self.resolved_child_paths
            .borrow_mut()
            .push(child_path.map(|value| value.to_vec()));
        if requested.is_empty() || requested.contains(&0) {
            return Err("program path has no valid file name".to_string());
        }
        if let Some((application, script)) = &self.rust_batch {
            WindowsResolvedExecutable::batch(application.clone(), script.clone())
                .map_err(|error| error.to_string())
        } else {
            WindowsResolvedExecutable::binary(wide(r"C:\resolved.exe"))
                .map_err(|error| error.to_string())
        }
    }

    fn prepare_rust193_cwd(&self, cwd: &mut [u16]) -> Result<usize, String> {
        self.events.borrow_mut().push("cwd");
        let verbatim_unc = wide(r"\\?\UNC");
        let verbatim = wide(r"\\?\");
        if cwd.starts_with(&verbatim_unc) && !cwd.windows(3).any(|part| part == wide(r"\..")) {
            let start = wide(r"\\?\UN").len();
            cwd[start] = b'\\' as u16;
            return Ok(start);
        }
        if cwd.starts_with(&verbatim) && !cwd.windows(3).any(|part| part == wide(r"\..")) {
            return Ok(verbatim.len());
        }
        Ok(0)
    }
}

fn fixture_case_cmp(left: &[u16], right: &[u16]) -> Ordering {
    left.iter()
        .copied()
        .map(ascii_upper)
        .cmp(right.iter().copied().map(ascii_upper))
}

fn fixture_libuv_cmp(left: &[u16], right: &[u16]) -> Ordering {
    left.iter()
        .copied()
        .map(|unit| {
            // Deliberately differs from ASCII/ordinal ordering so the
            // required-variable merge proves it reuses the platform's libuv
            // mapping rather than a local approximation.
            if unit == 0x00e9 {
                b'A' as u16
            } else {
                ascii_upper(unit)
            }
        })
        .cmp(right.iter().copied().map(|unit| {
            if unit == 0x00e9 {
                b'A' as u16
            } else {
                ascii_upper(unit)
            }
        }))
}

fn ascii_upper(unit: u16) -> u16 {
    if (u16::from(b'a')..=u16::from(b'z')).contains(&unit) {
        unit - u16::from(b'a') + u16::from(b'A')
    } else {
        unit
    }
}

#[test]
fn node20_plan_file_cannot_be_bypassed_by_an_external_other_binary() {
    let plan = node_plan_with(runtime("missing"), None, RuntimeEnv::new());
    let platform = FixturePlatform::default().with_node_resolution(Err("missing from search"));

    let error = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform)
        .expect_err("plan.file must be the only executable search input");

    assert_eq!(
        error,
        WindowsExecImageBuildError::Platform("missing from search".to_string())
    );
    assert_eq!(
        platform.node_searches(),
        vec![(wide("missing"), wide(r"C:\fixture-cwd"), None,)]
    );
    assert_eq!(
        platform.events(),
        vec!["node-env-sort", "node-cwd", "node-resolve"]
    );
}

#[test]
fn node20_cmd_classification_cannot_be_bypassed_with_a_binary_tag() {
    let plan = node_plan_with(runtime("run.cmd"), None, RuntimeEnv::new());
    let platform = FixturePlatform::default().with_node_resolution(Ok(
        WindowsNode20ResolvedExecutable::from_search_result(wide(r"C:\scripts\run.cmd")).unwrap(),
    ));

    let error = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform)
        .expect_err("libuv cannot execute a resolved .cmd image");

    assert_eq!(error, WindowsExecImageBuildError::NodeBatchExecutable);
    assert_eq!(
        platform.node_searches(),
        vec![(wide("run.cmd"), wide(r"C:\fixture-cwd"), None,)]
    );
}

#[test]
fn node20_omitted_cwd_snapshot_and_final_child_path_are_bound_to_search() {
    let mut environment = RuntimeEnv::new();
    environment.insert(
        RuntimeString::from("PATH"),
        RuntimeString::from(r"C:\child-bin"),
    );
    let plan = node_plan_with(runtime("node"), None, environment);
    let platform = FixturePlatform::default().with_node_cwd(r"C:\snapshot");

    let image =
        WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform).unwrap();

    let Some(WindowsCwdImage::Node20LibuvSnapshot(cwd)) = image.cwd() else {
        panic!("omitted Node cwd must become an owned libuv snapshot")
    };
    assert_eq!(cwd.as_ref(), nul_terminated(r"C:\snapshot"));
    assert_eq!(image.application(), nul_terminated(r"C:\node.exe"));
    assert_eq!(
        platform.node_searches(),
        vec![(
            wide("node"),
            wide(r"C:\snapshot"),
            Some(wide(r"C:\child-bin")),
        )]
    );
    assert_eq!(
        platform.events(),
        vec!["node-env-sort", "node-cwd", "node-resolve"]
    );
}

#[test]
fn node20_source_order_reports_env_then_cwd_then_search_failures() {
    let plan = node_plan_with(runtime("node"), None, RuntimeEnv::new());

    let env_first = FixturePlatform::default()
        .with_node_sort_error("env failed")
        .with_node_cwd_error("cwd failed")
        .with_node_resolution(Err("search failed"));
    let error =
        WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &env_first).unwrap_err();
    assert_eq!(
        error,
        WindowsExecImageBuildError::Platform("env failed".to_string())
    );
    assert_eq!(env_first.events(), vec!["node-env-sort"]);

    let cwd_second = FixturePlatform::default()
        .with_node_cwd_error("cwd failed")
        .with_node_resolution(Err("search failed"));
    let error = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &cwd_second)
        .unwrap_err();
    assert_eq!(
        error,
        WindowsExecImageBuildError::Platform("cwd failed".to_string())
    );
    assert_eq!(cwd_second.events(), vec!["node-env-sort", "node-cwd"]);

    let search_third = FixturePlatform::default().with_node_resolution(Err("search failed"));
    let error = WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &search_third)
        .unwrap_err();
    assert_eq!(
        error,
        WindowsExecImageBuildError::Platform("search failed".to_string())
    );
    assert_eq!(
        search_third.events(),
        vec!["node-env-sort", "node-cwd", "node-resolve"]
    );
}

#[test]
fn node20_empty_child_path_does_not_fall_back_to_parent_path() {
    let mut environment = RuntimeEnv::new();
    environment.insert(RuntimeString::from("Path"), RuntimeString::from(""));
    let plan = node_plan_with(runtime("node"), None, environment);
    let platform = FixturePlatform::default();

    WindowsExecImage::build_node(&plan, ProcessEnvironmentGuard::None, &platform).unwrap();

    assert_eq!(
        platform.node_searches(),
        vec![(wide("node"), wide(r"C:\fixture-cwd"), Some(Vec::new()),)]
    );
}

#[test]
fn node20_command_line_ports_libuv_quoting_and_verbatim_modes() {
    let lone_high = RuntimeString::from_utf16(vec![0xd800]);
    let args = vec![
        runtime(""),
        runtime("plain"),
        runtime("hello world\\"),
        runtime("hello\\\"world"),
        runtime(lone_high),
    ];
    let quoted = WindowsExecImage::build_node(
        &node_plan(args.clone(), RuntimeEnv::new(), false),
        ProcessEnvironmentGuard::None,
        &FixturePlatform::default(),
    )
    .unwrap();
    assert_eq!(quoted.application(), nul_terminated(r"C:\node.exe"));
    assert_eq!(
        quoted.command_line(),
        nul_terminated("\"custom argv0\" \"\" plain \"hello world\\\\\" \"hello\\\\\\\"world\" �")
    );

    let verbatim = WindowsExecImage::build_node(
        &node_plan(args, RuntimeEnv::new(), true),
        ProcessEnvironmentGuard::None,
        &FixturePlatform::default(),
    )
    .unwrap();
    assert_eq!(
        verbatim.command_line(),
        nul_terminated("custom argv0  plain hello world\\ hello\\\"world �")
    );
}

#[test]
fn node20_environment_prefilters_with_pinned_js_uppercase_then_keeps_projected_collisions() {
    let mut environment = RuntimeEnv::new();
    environment.insert(RuntimeString::from("path"), RuntimeString::from("lower"));
    environment.insert(RuntimeString::from("PATH"), RuntimeString::from("first"));
    environment.insert(RuntimeString::from("ß"), RuntimeString::from("eszett"));
    environment.insert(RuntimeString::from("SS"), RuntimeString::from("ascii"));
    environment.insert(
        RuntimeString::from_utf16(vec![0xd800]),
        RuntimeString::from("surrogate"),
    );
    environment.insert(
        RuntimeString::from_utf16(vec![0xfffd]),
        RuntimeString::from("replacement"),
    );
    let platform = FixturePlatform::default().with_required(&[
        ("PATH", "parent-path"),
        ("SYSTEMROOT", r"C:\Windows"),
        ("TEMP", ""),
    ]);
    let image = WindowsExecImage::build_node(
        &node_plan(Vec::new(), environment, false),
        ProcessEnvironmentGuard::None,
        &platform,
    )
    .unwrap();

    let WindowsEnvironmentImage::Explicit(block) = image.environment() else {
        panic!("Node environment must always be explicit")
    };
    assert_eq!(block.last(), Some(&0));
    assert_eq!(block.get(block.len() - 2), Some(&0));
    assert_eq!(
        block_entries(block),
        vec![
            wide("PATH=first"),
            wide("SS=ascii"),
            wide(r"SYSTEMROOT=C:\Windows"),
            wide("�=surrogate"),
            wide("�=replacement"),
        ]
    );
}

#[test]
fn node20_required_merge_reuses_the_exact_libuv_platform_comparator() {
    let mut environment = RuntimeEnv::new();
    environment.insert(RuntimeString::from("é"), RuntimeString::from("first"));
    let platform = FixturePlatform::default().with_required(&[("HOMEDRIVE", "C:")]);
    let image = WindowsExecImage::build_node(
        &node_plan(Vec::new(), environment, false),
        ProcessEnvironmentGuard::None,
        &platform,
    )
    .unwrap();
    let WindowsEnvironmentImage::Explicit(block) = image.environment() else {
        panic!("Node environment must always be explicit")
    };
    assert_eq!(
        block_entries(block),
        vec![wide("é=first"), wide("HOMEDRIVE=C:")]
    );
    assert_eq!(
        platform.node_searches(),
        vec![(wide("node"), wide(r"C:\fixture-cwd"), None,)]
    );
}

#[test]
fn rust193_binary_and_batch_command_lines_use_their_own_encoder() {
    let preimage = rust_preimage(
        runtime("requested.exe"),
        vec![
            runtime(""),
            runtime("hello world\\"),
            runtime("hello\\\"world"),
        ],
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    let binary = WindowsExecImage::build_rust193(
        &preimage,
        ProcessEnvironmentGuard::None,
        &FixturePlatform::default(),
    )
    .unwrap();
    assert_eq!(binary.application(), nul_terminated(r"C:\resolved.exe"));
    assert_eq!(
        binary.command_line(),
        nul_terminated("\"requested.exe\" \"\" \"hello world\\\\\" hello\\\\\\\"world")
    );

    let batch_platform = FixturePlatform::default()
        .with_rust_batch(r"C:\Windows\System32\cmd.exe", r"C:\scripts\run.cmd");
    let batch =
        WindowsExecImage::build_rust193(&preimage, ProcessEnvironmentGuard::None, &batch_platform)
            .unwrap();
    assert_eq!(
        batch.application(),
        nul_terminated(r"C:\Windows\System32\cmd.exe")
    );
    assert_eq!(
        batch.command_line(),
        nul_terminated(
            "cmd.exe /e:ON /v:OFF /d /c \"\"C:\\scripts\\run.cmd\" \"\" \"hello world\\\\\" \"hello\\\\\"\"world\"\"",
        )
    );
}

#[test]
fn rust193_cwd_is_platform_prepared_while_node_owns_the_libuv_cwd_snapshot() {
    let cwd = r"\\?\C:\work";
    let node = NodeSpawnPlan::from_normalized_parts(
        runtime("node"),
        runtime("node"),
        Vec::new(),
        Some(runtime(cwd)),
        ProcessEnvironmentPlan::NodeRuntime(RuntimeEnv::new()),
        false,
        false,
        false,
    )
    .unwrap();
    let node_image = WindowsExecImage::build_node(
        &node,
        ProcessEnvironmentGuard::None,
        &FixturePlatform::default(),
    )
    .unwrap();
    let Some(WindowsCwdImage::Node20LibuvSnapshot(_)) = node_image.cwd() else {
        panic!("Node cwd must not enter Rust make_dirp")
    };
    assert_eq!(node_image.cwd().unwrap().argument(), nul_terminated(cwd));

    let rust = Rust193SpawnPreimage::from_deferred_parts(
        runtime("requested.exe"),
        runtime("requested.exe"),
        Vec::new(),
        Some(runtime(cwd)),
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    let rust_image = WindowsExecImage::build_rust193(
        &rust,
        ProcessEnvironmentGuard::None,
        &FixturePlatform::default(),
    )
    .unwrap();
    let Some(WindowsCwdImage::Rust193PlatformPrepared { .. }) = rust_image.cwd() else {
        panic!("Rust cwd must retain its platform-prepared pointer state")
    };
    assert_eq!(rust_image.cwd().unwrap().storage(), nul_terminated(cwd));
    assert_eq!(
        rust_image.cwd().unwrap().argument(),
        nul_terminated(r"C:\work")
    );
}

#[test]
fn rust193_environment_distinguishes_omitted_and_explicit_empty_and_preserves_first_key_case() {
    let platform = FixturePlatform::default().with_parent(&[("Path", "one"), ("PATH", "two")]);
    let inherit = WindowsExecImage::build_rust193(
        &rust_preimage(
            runtime("requested.exe"),
            Vec::new(),
            Rust193EnvironmentPlan::NativeParentInherit,
        ),
        ProcessEnvironmentGuard::None,
        &platform,
    )
    .unwrap();
    assert!(matches!(
        inherit.environment(),
        WindowsEnvironmentImage::NativeParentInherit
    ));

    let overlay = WindowsExecImage::build_rust193(
        &rust_preimage(
            runtime("requested.exe"),
            Vec::new(),
            Rust193EnvironmentPlan::Overlay {
                mutations: vec![
                    Rust193EnvMutation::RuntimeSet {
                        key: RuntimeString::from("path"),
                        value: RuntimeString::from("three"),
                    },
                    Rust193EnvMutation::RuntimeRemove {
                        key: RuntimeString::from("PATH"),
                    },
                ],
            },
        ),
        ProcessEnvironmentGuard::None,
        &platform,
    )
    .unwrap();
    let WindowsEnvironmentImage::Explicit(block) = overlay.environment() else {
        panic!("changed Rust environment must be explicit")
    };
    assert_eq!(block.as_ref(), &[0, 0]);
}

#[test]
fn rust193_sealed_native_parent_filters_only_ordinary_node_options_aliases() {
    let platform = FixturePlatform::default().with_raw_parent(&[
        "z=last",
        "Path=one",
        "PATH=two",
        "NODE_OPTIONS=--require=evil",
        "node_options=--import=evil",
        r"=C:=C:\work",
        "NO_EQUALS",
        "a=first",
    ]);
    let image = WindowsExecImage::build_rust193(
        &rust_preimage(
            runtime("requested.exe"),
            Vec::new(),
            Rust193EnvironmentPlan::NativeParentInherit,
        ),
        ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
        &platform,
    )
    .unwrap();
    let WindowsEnvironmentImage::Explicit(block) = image.environment() else {
        panic!("sealed native parent must become an explicit block")
    };
    assert_eq!(
        block_entries(block),
        vec![
            wide("z=last"),
            wide("Path=one"),
            wide("PATH=two"),
            wide(r"=C:=C:\work"),
            wide("NO_EQUALS"),
            wide("a=first"),
        ]
    );
    assert_eq!(platform.events(), vec!["capture-raw-parent", "resolve"]);
}

#[test]
fn rust193_command_env_folds_mutations_before_validating_surviving_values() {
    let nul = RuntimeString::from_utf16(vec![0]);
    let cases = [
        (
            vec![
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("A"),
                    value: nul.clone(),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("A"),
                    value: RuntimeString::from("valid"),
                },
            ],
            vec![wide("A=valid")],
        ),
        (
            vec![
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("A"),
                    value: nul,
                },
                Rust193EnvMutation::RuntimeRemove {
                    key: RuntimeString::from("A"),
                },
            ],
            Vec::new(),
        ),
        (
            vec![
                Rust193EnvMutation::RuntimeRemove {
                    key: RuntimeString::from("a"),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("A"),
                    value: RuntimeString::from("valid"),
                },
            ],
            vec![wide("a=valid")],
        ),
    ];

    for (mutations, expected) in cases {
        let image = WindowsExecImage::build_rust193(
            &rust_preimage(
                runtime("requested.exe"),
                Vec::new(),
                Rust193EnvironmentPlan::Overlay { mutations },
            ),
            ProcessEnvironmentGuard::None,
            &FixturePlatform::default(),
        )
        .unwrap();
        let WindowsEnvironmentImage::Explicit(block) = image.environment() else {
            panic!("changed Rust environment must be explicit")
        };
        assert_eq!(block_entries(block), expected);
    }
}

#[test]
fn rust193_capture_supplies_the_final_child_path_to_resolution_before_cwd() {
    let platform = FixturePlatform::default().with_parent(&[("Path", r"C:\parent")]);
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime("requested.exe"),
        runtime("requested.exe"),
        Vec::new(),
        Some(runtime(r"C:\work")),
        Rust193EnvironmentPlan::Overlay {
            mutations: vec![
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("PATH"),
                    value: RuntimeString::from_utf16(vec![0]),
                },
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from("path"),
                    value: RuntimeString::from(r"C:\child"),
                },
            ],
        },
    );

    let image =
        WindowsExecImage::build_rust193(&preimage, ProcessEnvironmentGuard::None, &platform)
            .unwrap();

    assert_eq!(platform.events(), vec!["capture-parent", "resolve", "cwd"]);
    assert_eq!(
        platform.resolved_child_paths(),
        vec![Some(wide(r"C:\child"))]
    );
    let WindowsEnvironmentImage::Explicit(block) = image.environment() else {
        panic!("changed Rust environment must be explicit")
    };
    assert_eq!(block_entries(block), vec![wide(r"Path=C:\child")]);
}

#[test]
fn rust193_resolves_before_argv_validation_and_serializes_env_before_cwd_validation() {
    let argv_nul = Rust193SpawnPreimage::from_deferred_parts(
        runtime("requested.exe"),
        runtime(RuntimeString::from_utf16(vec![0])),
        Vec::new(),
        Some(runtime(RuntimeString::from_utf16(vec![0]))),
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    let argv_platform = FixturePlatform::default();
    let error =
        WindowsExecImage::build_rust193(&argv_nul, ProcessEnvironmentGuard::None, &argv_platform)
            .unwrap_err();
    assert_eq!(
        error,
        WindowsExecImageBuildError::ContainsNul {
            field: WindowsImageField::Argument(0),
            unit_index: 0,
        }
    );
    assert_eq!(argv_platform.events(), vec!["resolve"]);

    let env_and_cwd_nul = Rust193SpawnPreimage::from_deferred_parts(
        runtime("requested.exe"),
        runtime("requested.exe"),
        Vec::new(),
        Some(runtime(RuntimeString::from_utf16(vec![0]))),
        Rust193EnvironmentPlan::Overlay {
            mutations: vec![Rust193EnvMutation::RuntimeSet {
                key: RuntimeString::from("BAD"),
                value: RuntimeString::from_utf16(vec![0]),
            }],
        },
    );
    let env_platform = FixturePlatform::default();
    let error = WindowsExecImage::build_rust193(
        &env_and_cwd_nul,
        ProcessEnvironmentGuard::None,
        &env_platform,
    )
    .unwrap_err();
    assert_eq!(
        error,
        WindowsExecImageBuildError::ContainsNul {
            field: WindowsImageField::EnvironmentValue(0),
            unit_index: 0,
        }
    );
    assert_eq!(env_platform.events(), vec!["capture-parent", "resolve"]);
}

#[test]
fn rust193_requested_program_validation_is_owned_by_the_resolver_boundary() {
    let preimage = Rust193SpawnPreimage::from_deferred_parts(
        runtime(RuntimeString::from_utf16(vec![0])),
        runtime("requested.exe"),
        Vec::new(),
        None,
        Rust193EnvironmentPlan::NativeParentInherit,
    );
    let platform = FixturePlatform::default();

    let error =
        WindowsExecImage::build_rust193(&preimage, ProcessEnvironmentGuard::None, &platform)
            .unwrap_err();

    assert_eq!(
        error,
        WindowsExecImageBuildError::Platform("program path has no valid file name".to_string())
    );
    assert_eq!(platform.events(), vec!["resolve"]);
}
