//! Node-compatible locale sorting backed by the process's embedded V8/ICU 77.

use std::sync::{mpsc, OnceLock};
use std::thread;

use crate::runtime_values::RuntimeString;
use crate::v8_platform::initialize_v8;

const COLLATOR_BATCH_SOURCE: &str = r#"
(() => {
  // The locale is explicit. A bare `new Intl.Collator()` follows the host's default,
  // so the same repository listed on a zh-CN Windows box and an en-US Mac came back in
  // different orders — and every committed trace that contains a sorted listing became
  // machine-dependent. en-US is the ordering this module's oracles are taken from.
  const collator = new Intl.Collator('en-US');
  return (values, compareAdjacent) => {
    if (compareAdjacent) {
      return values
        .slice(0, -1)
        .map((value, index) => Math.sign(collator.compare(value, values[index + 1])));
    }
    return values
      .map((value, index) => ({ value, index }))
      .sort((left, right) => collator.compare(left.value, right.value))
      .map(item => item.index);
  };
})()
"#;

// Global handles must be dropped before their isolate. Rust drops struct fields
// in declaration order, so keep the isolate last.
struct LocaleSortRuntime {
    batch_function: v8::Global<v8::Function>,
    context: v8::Global<v8::Context>,
    isolate: v8::OwnedIsolate,
}

impl LocaleSortRuntime {
    fn new() -> Result<Self, String> {
        initialize_v8()?;
        let mut isolate = v8::Isolate::new(v8::CreateParams::default());
        let (batch_function, context) = {
            v8::scope!(let scope, &mut isolate);
            let context = v8::Context::new(scope, Default::default());
            let context_global = v8::Global::new(scope, context);
            let scope = &mut v8::ContextScope::new(scope, context);
            let source = v8::String::new(scope, COLLATOR_BATCH_SOURCE)
                .ok_or_else(|| "failed to allocate locale-sort source".to_string())?;
            let script = v8::Script::compile(scope, source, None)
                .ok_or_else(|| "failed to compile locale-sort source".to_string())?;
            let value = script
                .run(scope)
                .ok_or_else(|| "failed to initialize locale-sort function".to_string())?;
            let function = v8::Local::<v8::Function>::try_from(value)
                .map_err(|_| "locale-sort source did not return a function".to_string())?;
            (v8::Global::new(scope, function), context_global)
        };
        Ok(Self {
            batch_function,
            context,
            isolate,
        })
    }

    fn evaluate(
        &mut self,
        values: &[RuntimeString],
        compare_adjacent: bool,
    ) -> Result<Vec<i32>, String> {
        let array_len = i32::try_from(values.len())
            .map_err(|_| "locale-sort input exceeds the V8 array limit".to_string())?;
        let batch_function = &self.batch_function;
        let context = &self.context;
        let isolate = &mut self.isolate;
        v8::scope_with_context!(let scope, isolate, context);

        let input = v8::Array::new(scope, array_len);
        for (index, value) in values.iter().enumerate() {
            let value =
                v8::String::new_from_two_byte(scope, value.as_utf16(), v8::NewStringType::Normal)
                    .ok_or_else(|| "failed to allocate locale-sort input".to_string())?;
            if input.set_index(scope, index as u32, value.into()) != Some(true) {
                return Err("failed to append locale-sort input".to_string());
            }
        }

        let function = v8::Local::new(scope, batch_function);
        let receiver = v8::undefined(scope).into();
        let compare_adjacent = v8::Boolean::new(scope, compare_adjacent);
        let result = function
            .call(scope, receiver, &[input.into(), compare_adjacent.into()])
            .ok_or_else(|| "locale-sort function threw".to_string())?;
        let result = v8::Local::<v8::Array>::try_from(result)
            .map_err(|_| "locale-sort function returned a non-array".to_string())?;

        (0..result.length())
            .map(|index| {
                result
                    .get_index(scope, index)
                    .and_then(|value| value.int32_value(scope))
                    .ok_or_else(|| "locale-sort function returned a non-integer".to_string())
            })
            .collect()
    }
}

struct LocaleSortRequest {
    values: Vec<RuntimeString>,
    compare_adjacent: bool,
    response: mpsc::SyncSender<Result<Vec<i32>, String>>,
}

fn start_worker() -> Result<mpsc::Sender<LocaleSortRequest>, String> {
    let (request_tx, request_rx) = mpsc::channel::<LocaleSortRequest>();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("klaus-locale-sort".to_string())
        .spawn(move || {
            let mut runtime = match LocaleSortRuntime::new() {
                Ok(runtime) => {
                    let _ = ready_tx.send(Ok(()));
                    runtime
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            for request in request_rx {
                let result = runtime.evaluate(&request.values, request.compare_adjacent);
                let _ = request.response.send(result);
            }
        })
        .map_err(|error| format!("failed to start locale-sort worker: {error}"))?;

    ready_rx
        .recv()
        .map_err(|_| "locale-sort worker exited during initialization".to_string())??;
    Ok(request_tx)
}

fn worker() -> Result<&'static mpsc::Sender<LocaleSortRequest>, String> {
    static WORKER: OnceLock<Result<mpsc::Sender<LocaleSortRequest>, String>> = OnceLock::new();
    match WORKER.get_or_init(start_worker) {
        Ok(worker) => Ok(worker),
        Err(error) => Err(error.clone()),
    }
}

fn evaluate_runtime(values: &[RuntimeString], compare_adjacent: bool) -> Result<Vec<i32>, String> {
    let (response_tx, response_rx) = mpsc::sync_channel(1);
    worker()?
        .send(LocaleSortRequest {
            values: values.to_vec(),
            compare_adjacent,
            response: response_tx,
        })
        .map_err(|_| "locale-sort worker stopped".to_string())?;
    response_rx
        .recv()
        .map_err(|_| "locale-sort worker dropped its response".to_string())?
}

#[cfg(test)]
fn evaluate(values: &[&str], compare_adjacent: bool) -> Result<Vec<i32>, String> {
    let values = values
        .iter()
        .map(|value| RuntimeString::from(*value))
        .collect::<Vec<_>>();
    evaluate_runtime(&values, compare_adjacent)
}

fn sort_order(values: &[RuntimeString]) -> Result<Vec<usize>, String> {
    let raw_order = evaluate_runtime(values, false)?;
    if raw_order.len() != values.len() {
        return Err("locale-sort function returned the wrong number of indices".to_string());
    }
    let mut seen = vec![false; values.len()];
    raw_order
        .into_iter()
        .map(|index| {
            let index = usize::try_from(index)
                .map_err(|_| "locale-sort function returned a negative index".to_string())?;
            let Some(was_seen) = seen.get_mut(index) else {
                return Err("locale-sort function returned an out-of-range index".to_string());
            };
            if *was_seen {
                return Err("locale-sort function returned a duplicate index".to_string());
            }
            *was_seen = true;
            Ok(index)
        })
        .collect()
}

/// Stably sort a vector by a string key using Node's no-argument
/// `String.prototype.localeCompare` locale. The complete sort is performed in
/// one V8 call.
#[cfg(test)]
pub(crate) fn sort_by_key<T>(items: &mut Vec<T>, key: impl Fn(&T) -> &str) {
    if items.len() < 2 {
        return;
    }
    let keys = items
        .iter()
        .map(|item| RuntimeString::from(key(item)))
        .collect::<Vec<_>>();
    let order = sort_order(&keys).expect("embedded V8 locale sort must remain available");
    drop(keys);

    let mut original = std::mem::take(items)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    items.reserve(original.len());
    for index in order {
        items.push(
            original[index]
                .take()
                .expect("validated locale-sort index must be unique"),
        );
    }
}

/// Stably sort by an exact JavaScript-string key. The worker constructs each
/// V8 input with the two-byte UTF-16 API, so unpaired surrogates remain distinct
/// from each other and from U+FFFD through ICU comparison.
pub(crate) fn sort_runtime_by_key<T>(items: &mut Vec<T>, key: impl Fn(&T) -> &RuntimeString) {
    if items.len() < 2 {
        return;
    }
    let keys = items
        .iter()
        .map(|item| key(item).clone())
        .collect::<Vec<_>>();
    let order = sort_order(&keys).expect("embedded V8 locale sort must remain available");
    drop(keys);

    let mut original = std::mem::take(items)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    items.reserve(original.len());
    for index in order {
        items.push(
            original[index]
                .take()
                .expect("validated locale-sort index must be unique"),
        );
    }
}

/// Stably sort by an exact JavaScript-string key returned by value. This is for
/// trait-object surfaces whose key is synthesized on demand rather than stored
/// behind a borrowable field.
pub(crate) fn sort_runtime_by_owned_key<T>(items: &mut Vec<T>, key: impl Fn(&T) -> RuntimeString) {
    if items.len() < 2 {
        return;
    }
    let keys = items.iter().map(key).collect::<Vec<_>>();
    let order = sort_order(&keys).expect("embedded V8 locale sort must remain available");
    drop(keys);

    let mut original = std::mem::take(items)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    items.reserve(original.len());
    for index in order {
        items.push(
            original[index]
                .take()
                .expect("validated locale-sort index must be unique"),
        );
    }
}

#[cfg(test)]
pub(crate) fn compare_adjacent_signs(values: &[&str]) -> Vec<i32> {
    evaluate(values, true).expect("embedded V8 locale comparison must remain available")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sort_strings(values: &[&str]) -> Vec<String> {
        let mut values = values
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        sort_by_key(&mut values, String::as_str);
        values
    }

    #[test]
    fn matches_node_20_en_us_oracles() {
        assert_eq!(
            sort_strings(&["\u{2010}.rs", "\u{10d6e}.rs"]),
            ["\u{10d6e}.rs", "\u{2010}.rs"]
        );
        assert_eq!(sort_strings(&["z.rs", "ä.rs"]), ["ä.rs", "z.rs"]);
        assert_eq!(sort_strings(&["A", "a"]), ["a", "A"]);
        assert_eq!(sort_strings(&["a2", "a10"]), ["a10", "a2"]);
        assert_eq!(sort_strings(&["ab", "a-b"]), ["a-b", "ab"]);
        assert_eq!(sort_strings(&["résumé", "resume"]), ["resume", "résumé"]);
        assert_eq!(sort_strings(&["é", "e"]), ["e", "é"]);
        assert_eq!(sort_strings(&["O", "o"]), ["o", "O"]);
    }

    #[test]
    fn preserves_stable_order_for_collation_equal_values() {
        let values = ["e\u{301}.rs", "é.rs"];
        assert_eq!(compare_adjacent_signs(&values), [0]);
        assert_eq!(sort_strings(&values), values);
    }

    #[test]
    fn repeated_calls_reuse_the_cached_batch_worker() {
        for _ in 0..128 {
            assert_eq!(sort_strings(&["z.rs", "ä.rs"]), ["ä.rs", "z.rs"]);
        }
    }

    #[test]
    fn exact_sort_keeps_lone_surrogates_distinct_from_replacement() {
        let mut values = vec![
            RuntimeString::from("\u{fffd}"),
            RuntimeString::from_utf16(vec![0xdc00]),
            RuntimeString::from_utf16(vec![0xd800]),
        ];
        sort_runtime_by_key(&mut values, |value| value);
        assert_eq!(
            values,
            vec![
                RuntimeString::from_utf16(vec![0xd800]),
                RuntimeString::from_utf16(vec![0xdc00]),
                RuntimeString::from("\u{fffd}"),
            ]
        );
    }

    #[test]
    fn concurrent_threads_share_the_cached_batch_worker() {
        let threads = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..32 {
                        assert_eq!(
                            sort_strings(&["\u{2010}.rs", "\u{10d6e}.rs"]),
                            ["\u{10d6e}.rs", "\u{2010}.rs"]
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
