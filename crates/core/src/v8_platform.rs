//! Process-wide V8 and ICU initialization shared by every embedded runtime.

use std::sync::OnceLock;

/// Initialize the single process-wide V8 platform with the bundled ICU 77
/// data. All isolates must call this boundary before construction.
pub(crate) fn initialize_v8() -> Result<(), String> {
    static PLATFORM: OnceLock<Result<v8::SharedRef<v8::Platform>, String>> = OnceLock::new();

    match PLATFORM.get_or_init(|| {
        v8::icu::set_common_data_77(klaus_agent_icu_data::common_data_77()?)
            .map_err(|error_code| format!("failed to initialize ICU data: {error_code}"))?;
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform.clone());
        v8::V8::initialize();
        Ok(platform)
    }) {
        Ok(_) => Ok(()),
        Err(error_text) => Err(error_text.clone()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn independent_icu_data_crate_supplies_the_v8_boundary() {
        let data = klaus_agent_icu_data::common_data_77().unwrap();
        assert_eq!(data.len(), 31_895_376);
        assert_eq!(data.as_ptr() as usize % 16, 0);
    }
}
