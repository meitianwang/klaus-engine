//! Embedded namespace launcher. Call only in a fresh helper process before threads start.

#[cfg(target_os = "linux")]
pub fn run(arguments: &[std::ffi::OsString]) -> ! {
    use std::os::raw::{c_char, c_int};
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn klaus_bwrap_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    }
    let mut arguments: Vec<Vec<u8>> = arguments
        .iter()
        .map(|argument| {
            let bytes = argument.as_os_str().as_bytes();
            assert!(!bytes.contains(&0), "namespace argument contains NUL");
            let mut bytes = bytes.to_vec();
            bytes.push(0);
            bytes
        })
        .collect();
    let mut pointers: Vec<*mut c_char> = arguments
        .iter_mut()
        .map(|argument| argument.as_mut_ptr().cast())
        .collect();
    pointers.push(std::ptr::null_mut());
    let count = c_int::try_from(arguments.len()).expect("namespace argument count");
    // The C entry point receives writable, NUL-terminated argv storage. This
    // process is disposable and the entry point either returns or replaces it.
    let status = unsafe { klaus_bwrap_main(count, pointers.as_mut_ptr()) };
    std::process::exit(status);
}

#[cfg(target_os = "linux")]
pub mod helper;
