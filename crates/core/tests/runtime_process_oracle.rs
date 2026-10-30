use klaus_agent_core::runtime_process::{
    normalize_child_env, LogicalSpawnRequest, ProcessAtom, ProcessEnvironmentPlan,
    ProcessPlanErrorKind, ProcessPlanField, RuntimeEnv, RuntimeEnvEntries,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, RuntimeCodecErrorKind, RuntimeJson, RuntimeObject, RuntimeString,
};

fn string(value: &str) -> RuntimeString {
    RuntimeString::from_utf8(value)
}

fn units(value: &[u16]) -> RuntimeString {
    RuntimeString::from_utf16(value.to_vec())
}

fn environment(entries: impl IntoIterator<Item = (RuntimeString, RuntimeString)>) -> RuntimeEnv {
    let mut object = RuntimeObject::new();
    for (key, value) in entries {
        object.insert(key, RuntimeJson::String(value));
    }
    runtime_codec::decode_value(&RuntimeJson::Object(object)).unwrap()
}

fn empty_environment() -> RuntimeEnv {
    environment(std::iter::empty())
}

fn entries(env: &RuntimeEnv) -> Vec<(Vec<u16>, Vec<u16>)> {
    let ordered: RuntimeEnvEntries<'_> = env.entries();
    ordered
        .map(|(key, value)| (key.as_utf16().to_vec(), value.as_utf16().to_vec()))
        .collect()
}

#[test]
fn runtime_env_enumerates_array_indices_before_ordinary_first_insertion_slots() {
    let mut env = empty_environment();
    for (key, value) in [
        ("b", "b-value"),
        ("10", "ten"),
        ("2", "two"),
        ("01", "leading-zero"),
        ("1", "one"),
        ("a", "a-value"),
        ("4294967294", "max-index"),
        ("4294967295", "not-an-index"),
        ("0", "zero"),
    ] {
        assert_eq!(env.insert(string(key), string(value)), None);
    }

    let actual = env
        .entries()
        .map(|(key, value)| {
            (
                key.try_to_utf8().expect("scalar fixture key"),
                value.try_to_utf8().expect("scalar fixture value"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            ("0", "zero"),
            ("1", "one"),
            ("2", "two"),
            ("10", "ten"),
            ("4294967294", "max-index"),
            ("b", "b-value"),
            ("01", "leading-zero"),
            ("a", "a-value"),
            ("4294967295", "not-an-index"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
}

#[test]
fn runtime_env_overwrite_keeps_slots_and_delete_reinsert_appends_ordinary_keys() {
    let mut env = empty_environment();
    env.insert(string("a"), string("first"));
    env.insert(string("2"), string("two-first"));
    env.insert(string("b"), string("second"));
    env.insert(string("1"), string("one"));

    assert_eq!(
        env.insert(string("a"), string("overwritten")),
        Some(string("first"))
    );
    assert_eq!(
        env.insert(string("2"), string("two-overwritten")),
        Some(string("two-first"))
    );
    assert_eq!(
        env.entries()
            .map(|(key, _)| key.try_to_utf8().expect("scalar fixture key"))
            .collect::<Vec<_>>(),
        ["1", "2", "a", "b"]
    );

    assert_eq!(env.remove(&string("a")), Some(string("overwritten")));
    assert_eq!(env.remove(&string("1")), Some(string("one")));
    env.insert(string("a"), string("reinserted"));
    env.insert(string("1"), string("one-reinserted"));

    assert_eq!(
        env.entries()
            .map(|(key, _)| key.try_to_utf8().expect("scalar fixture key"))
            .collect::<Vec<_>>(),
        ["1", "2", "b", "a"]
    );
}

#[test]
fn runtime_env_preserves_arbitrary_utf16_key_and_value_identity() {
    let rows = [
        (vec![0xd800], vec![0xdc00]),
        (vec![0xd801], vec![0xd800]),
        (vec![0xfffd], vec![0xfffd]),
        (
            vec![
                0xfdd0,
                b'u'.into(),
                b'd'.into(),
                b'8'.into(),
                b'0'.into(),
                b'0'.into(),
            ],
            vec![
                0xfdd0,
                b'u'.into(),
                b'd'.into(),
                b'c'.into(),
                b'0'.into(),
                b'0'.into(),
            ],
        ),
    ];
    let mut env = empty_environment();
    for (key, value) in &rows {
        env.insert(units(key), units(value));
    }

    assert_eq!(env.len(), rows.len());
    assert_eq!(entries(&env), rows.to_vec());
    for (key, value) in rows {
        assert_eq!(
            env.get(&units(&key)).map(RuntimeString::as_utf16),
            Some(value.as_slice())
        );
    }
}

#[test]
fn runtime_env_overlay_uses_javascript_assignment_order() {
    let mut base = environment([
        (string("a"), string("base-a")),
        (string("b"), string("base-b")),
        (string("3"), string("base-three")),
    ]);
    let overlay = environment([
        (string("b"), string("overlay-b")),
        (string("c"), string("overlay-c")),
        (string("1"), string("overlay-one")),
    ]);

    base.overlay(&overlay);
    let actual = base
        .entries()
        .map(|(key, value)| {
            (
                key.try_to_utf8().expect("scalar fixture key"),
                value.try_to_utf8().expect("scalar fixture value"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            ("1", "overlay-one"),
            ("3", "base-three"),
            ("a", "base-a"),
            ("b", "overlay-b"),
            ("c", "overlay-c"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
}

#[test]
fn node_v8_coverage_parent_propagation_obeys_truthiness_and_exact_has_own() {
    let key = string("NODE_V8_COVERAGE");

    let parent_unset = environment([(string("PARENT"), string("visible"))]);
    let explicit_empty = empty_environment();
    assert!(explicit_empty.is_empty());
    assert_eq!(
        normalize_child_env(&parent_unset, Some(&explicit_empty)),
        explicit_empty
    );

    let parent_empty = environment([(key.clone(), RuntimeString::default())]);
    assert_eq!(
        normalize_child_env(&parent_empty, Some(&explicit_empty)),
        explicit_empty
    );

    let parent_nonempty = environment([
        (string("PARENT"), string("visible")),
        (key.clone(), string("/tmp/coverage")),
    ]);
    let inherited = normalize_child_env(&parent_nonempty, None);
    assert_eq!(inherited, parent_nonempty);

    let propagated = normalize_child_env(&parent_nonempty, Some(&explicit_empty));
    assert_eq!(propagated.len(), 1);
    assert_eq!(propagated.get(&key), Some(&string("/tmp/coverage")));

    let explicit_own_empty = environment([
        (string("CHILD"), string("yes")),
        (key.clone(), RuntimeString::default()),
    ]);
    let suppressed = normalize_child_env(&parent_nonempty, Some(&explicit_own_empty));
    assert_eq!(suppressed, explicit_own_empty);
    assert_eq!(suppressed.get(&key), Some(&RuntimeString::default()));

    let explicit_own_nonempty = environment([(key.clone(), string("/tmp/child-coverage"))]);
    assert_eq!(
        normalize_child_env(&parent_nonempty, Some(&explicit_own_nonempty)),
        explicit_own_nonempty
    );

    let near_miss = environment([(
        string("node_v8_coverage"),
        string("not-an-exact-own-property"),
    )]);
    let exact_has_own = normalize_child_env(&parent_nonempty, Some(&near_miss));
    assert_eq!(exact_has_own.len(), 2);
    assert_eq!(exact_has_own.get(&key), Some(&string("/tmp/coverage")));
}

#[test]
fn runtime_env_mutation_api_is_runtime_string_only() {
    let _: fn(&mut RuntimeEnv, RuntimeString, RuntimeString) -> Option<RuntimeString> =
        RuntimeEnv::insert;
    let _: fn(&mut RuntimeEnv, &RuntimeString) -> Option<RuntimeString> = RuntimeEnv::remove;
    let _: for<'a> fn(&'a RuntimeEnv, &RuntimeString) -> Option<&'a RuntimeString> =
        RuntimeEnv::get;
}

#[test]
fn runtime_env_wire_codec_accepts_only_exact_string_values() {
    let environment = environment([
        (string("b"), units(&[0xd800])),
        (string("1"), string("numeric-first")),
    ]);
    let encoded = runtime_codec::encode_value(&environment).unwrap();
    assert_eq!(
        runtime_codec::encode_text(&environment).unwrap().as_utf16(),
        [
            u16::from(b'{'),
            u16::from(b'"'),
            u16::from(b'1'),
            u16::from(b'"'),
            u16::from(b':'),
            u16::from(b'"'),
            u16::from(b'n'),
            u16::from(b'u'),
            u16::from(b'm'),
            u16::from(b'e'),
            u16::from(b'r'),
            u16::from(b'i'),
            u16::from(b'c'),
            u16::from(b'-'),
            u16::from(b'f'),
            u16::from(b'i'),
            u16::from(b'r'),
            u16::from(b's'),
            u16::from(b't'),
            u16::from(b'"'),
            u16::from(b','),
            u16::from(b'"'),
            u16::from(b'b'),
            u16::from(b'"'),
            u16::from(b':'),
            u16::from(b'"'),
            u16::from(b'\\'),
            u16::from(b'u'),
            u16::from(b'd'),
            u16::from(b'8'),
            u16::from(b'0'),
            u16::from(b'0'),
            u16::from(b'"'),
            u16::from(b'}'),
        ]
    );
    assert_eq!(
        runtime_codec::decode_value::<RuntimeEnv>(&encoded).unwrap(),
        environment
    );

    let mut invalid = RuntimeObject::new();
    invalid.insert("ENV", RuntimeJson::Bool(true));
    let error =
        runtime_codec::decode_value::<RuntimeEnv>(&RuntimeJson::Object(invalid)).unwrap_err();
    assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedString);
    assert_eq!(error.pointer(), RuntimeString::from("/ENV"));
}

#[test]
fn runtime_env_debug_does_not_expose_keys_values_or_backing_maps() {
    let environment = environment([(
        string("SECRET_ENVIRONMENT_KEY"),
        string("secret-environment-value"),
    )]);

    assert_eq!(
        format!("{environment:?}"),
        "RuntimeEnv { property_count: 1, .. }"
    );
}

fn runtime_atom(value: &str) -> ProcessAtom {
    ProcessAtom::Runtime(string(value))
}

#[test]
fn logical_spawn_normalization_fixes_argv_zero_and_environment_presence() {
    let parent = environment([
        (string("PARENT"), string("visible")),
        (string("NODE_V8_COVERAGE"), string("/tmp/coverage")),
    ]);

    let inherited = LogicalSpawnRequest::new(runtime_atom("node"))
        .with_args(vec![runtime_atom("script.js")])
        .normalize_node(&parent)
        .unwrap();
    assert_eq!(inherited.file(), &runtime_atom("node"));
    assert_eq!(inherited.argv0(), &runtime_atom("node"));
    assert_eq!(inherited.args(), &[runtime_atom("script.js")]);
    assert_eq!(inherited.cwd(), None);
    assert_eq!(
        inherited.environment(),
        &ProcessEnvironmentPlan::NodeRuntime(parent.clone())
    );

    let explicit_empty = LogicalSpawnRequest::new(runtime_atom("node"))
        .with_argv0(runtime_atom("custom-zero"))
        .with_env(empty_environment())
        .normalize_node(&parent)
        .unwrap();
    assert_eq!(explicit_empty.argv0(), &runtime_atom("custom-zero"));
    let ProcessEnvironmentPlan::NodeRuntime(environment) = explicit_empty.environment() else {
        panic!("Node normalization must produce a Node runtime environment");
    };
    assert_eq!(environment.len(), 1);
    assert_eq!(
        environment.get(&string("NODE_V8_COVERAGE")),
        Some(&string("/tmp/coverage"))
    );
    assert_eq!(environment.get(&string("PARENT")), None);
}

#[test]
fn logical_spawn_accepts_explicit_empty_cwd_without_collapsing_it_to_omitted() {
    let plan = LogicalSpawnRequest::new(runtime_atom("node"))
        .with_cwd(ProcessAtom::Runtime(RuntimeString::default()))
        .normalize_node(&empty_environment())
        .unwrap();

    assert_eq!(
        plan.cwd(),
        Some(&ProcessAtom::Runtime(RuntimeString::default()))
    );
}

#[test]
fn logical_spawn_rejects_empty_file_and_nul_at_each_runtime_boundary() {
    let empty_file = LogicalSpawnRequest::new(ProcessAtom::Runtime(RuntimeString::default()))
        .normalize_node(&empty_environment())
        .unwrap_err();
    assert_eq!(empty_file.kind(), ProcessPlanErrorKind::EmptyExecutable);
    assert_eq!(empty_file.field(), ProcessPlanField::File);

    let cases = [
        (
            LogicalSpawnRequest::new(ProcessAtom::Runtime(units(&[b'n'.into(), 0, b'e'.into()]))),
            ProcessPlanField::File,
            1,
        ),
        (
            LogicalSpawnRequest::new(runtime_atom("node"))
                .with_argv0(ProcessAtom::Runtime(units(&[0]))),
            ProcessPlanField::Argv0,
            0,
        ),
        (
            LogicalSpawnRequest::new(runtime_atom("node"))
                .with_args(vec![runtime_atom("ok"), ProcessAtom::Runtime(units(&[0]))]),
            ProcessPlanField::Argument(1),
            0,
        ),
        (
            LogicalSpawnRequest::new(runtime_atom("node"))
                .with_cwd(ProcessAtom::Runtime(units(&[b'.'.into(), 0]))),
            ProcessPlanField::Cwd,
            1,
        ),
        (
            LogicalSpawnRequest::new(runtime_atom("node"))
                .with_env(environment([(units(&[b'K'.into(), 0]), string("value"))])),
            ProcessPlanField::EnvironmentKey(0),
            1,
        ),
        (
            LogicalSpawnRequest::new(runtime_atom("node"))
                .with_env(environment([(string("KEY"), units(&[b'v'.into(), 0]))])),
            ProcessPlanField::EnvironmentValue(0),
            1,
        ),
    ];

    for (request, expected_field, expected_index) in cases {
        let error = request.normalize_node(&empty_environment()).unwrap_err();
        assert_eq!(error.field(), expected_field);
        assert_eq!(
            error.kind(),
            ProcessPlanErrorKind::ContainsNul {
                unit_index: expected_index,
            }
        );
    }
}

#[test]
fn logical_spawn_validates_only_the_selected_normalized_environment() {
    let invalid_parent = environment([(string("BAD"), units(&[0]))]);

    let inherited_error = LogicalSpawnRequest::new(runtime_atom("node"))
        .normalize_node(&invalid_parent)
        .unwrap_err();
    assert_eq!(
        inherited_error.field(),
        ProcessPlanField::EnvironmentValue(0)
    );

    let explicit_empty = LogicalSpawnRequest::new(runtime_atom("node"))
        .with_env(empty_environment())
        .normalize_node(&invalid_parent)
        .unwrap();
    assert_eq!(
        explicit_empty.environment(),
        &ProcessEnvironmentPlan::NodeRuntime(empty_environment())
    );
}
