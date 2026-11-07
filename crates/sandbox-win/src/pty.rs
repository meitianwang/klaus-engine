//! Pseudoconsole support, for targets that expect a terminal.
//!
//! A command run without a terminal behaves differently: `git` stops paging, build
//! tools drop their progress rendering, and anything that checks `isatty` takes its
//! non-interactive branch. When the host has a terminal, the sandboxed target should
//! see one too, so this wires a ConPTY and forwards resize events through it.
//!
//! # The desktop assignment is not optional here
//!
//! A process launched with a restricted or AppContainer token and **no explicit
//! `lpDesktop`** can die during DLL initialization with `STATUS_DLL_INIT_FAILED`,
//! before any of its own code runs. The cause is that the default inherited desktop
//! is one the sandboxed token cannot open, and `conhost` — which ConPTY starts on
//! the target's behalf — needs a desktop it can attach to. The failure looks like
//! the command silently not existing: no output, a non-zero exit, nothing in the
//! target's own logs, because the target never started.
//!
//! So [`desktop_for_launch`] always returns a name, and it names the interactive
//! desktop of the current window station. That keeps the target inside a desktop it
//! can open while leaving the AppContainer's own restrictions in force — the desktop
//! is not the boundary here, the token and the ACLs are.

/// Desktop to hand `CreateProcessW` for a sandboxed launch.
///
/// `WinSta0\Default` is the interactive window station's desktop. Passing an empty
/// name (which is what "inherit" means to Win32) is what triggers the DLL-init
/// failure described in the module note, so this is never empty.
pub fn desktop_for_launch() -> &'static str {
    "WinSta0\\Default"
}

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, S_OK};
    use windows_sys::Win32::System::Console::{
        ClosePseudoConsole, CreatePseudoConsole, ResizePseudoConsole, COORD, HPCON,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;

    use crate::protocol::TerminalSize;

    /// Why a pseudoconsole could not be created or resized.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum PtyError {
        #[error("creating the pty pipes failed: Win32 error {0}")]
        Pipes(u32),
        #[error("creating the pseudoconsole failed: HRESULT 0x{0:08x}")]
        Create(i32),
        #[error("resizing the pseudoconsole failed: HRESULT 0x{0:08x}")]
        Resize(i32),
    }

    /// An owned pipe end.
    struct PipeEnd(HANDLE);

    impl PipeEnd {
        fn raw(&self) -> HANDLE {
            self.0
        }

        fn close(&mut self) {
            let handle = std::mem::replace(&mut self.0, 0);
            if handle != 0 && handle != INVALID_HANDLE_VALUE {
                unsafe { CloseHandle(handle) };
            }
        }
    }

    impl Drop for PipeEnd {
        fn drop(&mut self) {
            self.close();
        }
    }

    /// A pseudoconsole plus the host's ends of its pipes.
    ///
    /// The console holds its own references to the supplied pipe ends. The caller
    /// closes its copies after target creation; the wrapper retains the opposite
    /// ends for relaying bytes.
    pub struct Pty {
        console: HPCON,
        /// Host writes target's stdin here.
        input: PipeEnd,
        /// Host reads target's output here.
        output: PipeEnd,
        /// Caller-owned copies supplied to CreatePseudoConsole, closed after launch.
        launch_handles: Option<(PipeEnd, PipeEnd)>,
    }

    impl Drop for Pty {
        fn drop(&mut self) {
            // Also release these copies when launch was skipped or failed before
            // the wrapper reached its explicit post-CreateProcess cleanup.
            self.close_launch_handles();
            // Active relays own the transferred host handles and keep reading
            // final output until ClosePseudoConsole returns. If I/O was never
            // transferred, close our unread ends before starting console shutdown.
            self.input.close();
            self.output.close();
            // `HPCON` is `isize` in this binding, like `HANDLE`, so "none" is 0.
            if self.console != 0 {
                // Closing the pseudoconsole is what tells the target's console host
                // to shut down; without it a finished session leaves a conhost
                // process behind.
                unsafe { ClosePseudoConsole(self.console) };
            }
        }
    }

    unsafe impl Send for Pty {}
    unsafe impl Sync for Pty {}

    fn create_pipe() -> Result<(PipeEnd, PipeEnd), PtyError> {
        let mut read: HANDLE = 0;
        let mut write: HANDLE = 0;
        let ok = unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 0) };
        if ok == 0 {
            return Err(PtyError::Pipes(unsafe {
                windows_sys::Win32::Foundation::GetLastError()
            }));
        }
        Ok((PipeEnd(read), PipeEnd(write)))
    }

    impl Pty {
        /// Create a pseudoconsole of the given size.
        pub fn open(size: TerminalSize) -> Result<Self, PtyError> {
            // Retain all four caller-owned handles until target creation finishes.
            // CreatePseudoConsole duplicates the console-side references; it does
            // not close or take ownership of the caller's original handles.
            let (console_input_read, host_input_write) = create_pipe()?;
            let (host_output_read, console_output_write) = create_pipe()?;

            let mut console: HPCON = 0;
            let hr = unsafe {
                CreatePseudoConsole(
                    COORD {
                        X: size.cols as i16,
                        Y: size.rows as i16,
                    },
                    console_input_read.raw(),
                    console_output_write.raw(),
                    0,
                    &mut console,
                )
            };
            if hr != S_OK || console == 0 {
                return Err(PtyError::Create(hr));
            }
            Ok(Self {
                console,
                input: host_input_write,
                output: host_output_read,
                launch_handles: Some((console_input_read, console_output_write)),
            })
        }

        /// Close the caller's console-side copies once CreateProcess has returned.
        /// Keeping an output writer open would prevent the relay from observing EOF.
        pub fn close_launch_handles(&mut self) {
            self.launch_handles.take();
        }

        pub fn console(&self) -> HPCON {
            self.console
        }

        /// Handle the wrapper writes the target's stdin to.
        pub fn input_handle(&self) -> HANDLE {
            self.input.raw()
        }

        /// Handle the wrapper reads the target's output from.
        pub fn output_handle(&self) -> HANDLE {
            self.output.raw()
        }

        /// Transfer the host pipe ends to the supervisor's I/O workers.
        /// Keeping another input writer here would suppress EOF when the host
        /// closes stdin. The console itself retains only the opposite pipe ends.
        pub(crate) fn take_io(&mut self) -> (std::fs::File, std::fs::File) {
            use std::os::windows::io::FromRawHandle;
            let input = std::mem::replace(&mut self.input.0, 0);
            let output = std::mem::replace(&mut self.output.0, 0);
            // This is called once by the supervisor after a successful launch.
            assert!(
                input != 0 && output != 0,
                "PTY I/O ownership already transferred"
            );
            unsafe {
                (
                    std::fs::File::from_raw_handle(input as *mut std::ffi::c_void),
                    std::fs::File::from_raw_handle(output as *mut std::ffi::c_void),
                )
            }
        }

        /// Apply a new terminal size.
        pub fn resize(&self, size: TerminalSize) -> Result<(), PtyError> {
            let hr = unsafe {
                ResizePseudoConsole(
                    self.console,
                    COORD {
                        X: size.cols as i16,
                        Y: size.rows as i16,
                    },
                )
            };
            if hr != S_OK {
                return Err(PtyError::Resize(hr));
            }
            Ok(())
        }
    }
    /// Resize a console by raw handle.
    ///
    /// Takes the handle rather than `&Pty` because the control thread acts on resize
    /// messages while the main thread owns the `Pty`. A failure here is swallowed:
    /// the host's terminal changed size, and a console that could not follow is a
    /// cosmetic problem, not a reason to kill a running command.
    pub fn resize_console(console: HPCON, size: TerminalSize) {
        unsafe {
            ResizePseudoConsole(
                console,
                COORD {
                    X: size.cols as i16,
                    Y: size.rows as i16,
                },
            )
        };
    }
}

#[cfg(windows)]
pub use imp::{resize_console, Pty, PtyError};

/// Clamp a requested terminal size to what a `COORD` can carry.
///
/// `COORD` fields are signed 16-bit, so a size past 32767 wraps negative and the
/// console ends up with a nonsensical geometry rather than a large one. Zero is
/// also refused: a zero-width console makes output rendering undefined, and hosts
/// do report 0 while a terminal is being set up.
pub fn clamp_size(size: crate::protocol::TerminalSize) -> crate::protocol::TerminalSize {
    const MAX: u16 = i16::MAX as u16;
    crate::protocol::TerminalSize {
        rows: size.rows.clamp(1, MAX),
        cols: size.cols.clamp(1, MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TerminalSize;

    #[test]
    fn a_launch_always_names_a_desktop() {
        // An empty name means "inherit", which is what produces
        // STATUS_DLL_INIT_FAILED under a sandboxed token.
        assert!(!desktop_for_launch().is_empty());
        assert_eq!(desktop_for_launch(), "WinSta0\\Default");
    }

    #[test]
    fn an_ordinary_size_passes_through() {
        let size = TerminalSize {
            rows: 40,
            cols: 120,
        };
        assert_eq!(clamp_size(size), size);
    }

    #[test]
    fn a_zero_size_becomes_one_row_and_column() {
        // Hosts report 0 while a terminal is still being set up; a zero-width
        // console leaves output rendering undefined.
        assert_eq!(
            clamp_size(TerminalSize { rows: 0, cols: 0 }),
            TerminalSize { rows: 1, cols: 1 }
        );
        assert_eq!(
            clamp_size(TerminalSize { rows: 24, cols: 0 }),
            TerminalSize { rows: 24, cols: 1 }
        );
    }

    #[test]
    fn a_size_past_the_coord_range_is_clamped_not_wrapped() {
        // COORD is i16: 40000 would arrive as a negative geometry.
        let clamped = clamp_size(TerminalSize {
            rows: 40_000,
            cols: 50_000,
        });
        assert_eq!(clamped.rows, i16::MAX as u16);
        assert_eq!(clamped.cols, i16::MAX as u16);
        // And the clamped values are representable without sign loss.
        assert!(clamped.rows as i16 > 0);
        assert!(clamped.cols as i16 > 0);
    }

    #[test]
    fn the_largest_valid_size_survives() {
        let size = TerminalSize {
            rows: i16::MAX as u16,
            cols: i16::MAX as u16,
        };
        assert_eq!(clamp_size(size), size);
    }
}
