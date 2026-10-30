pub fn dispatch() {
    // This function IS the wrapper entry point, so reaching it is what makes the Windows
    // restricted-token backend usable: it re-executes this binary with the wrapper marker,
    // and only a binary that inspects its own argv for the marker can serve that.
    #[cfg(windows)]
    klaus_agent_core::sandbox::windows::declare_wrapper_entry_point();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some(klaus_sandbox_win::mxc::LAUNCH_ARG) {
        if args.len() != 1 {
            eprintln!("invalid native MXC launcher arguments");
            std::process::exit(2);
        }
        klaus_sandbox_win::mxc::run_main();
    }
    // A wrapper invocation never returns: it runs the sandboxed command and exits
    // with that command's status. Falling through would start an ordinary agent
    // session in a process the engine launched to be a sandbox, which means running
    // the command with no boundaries while the caller believes there are some.
    if let Some(handles) = klaus_sandbox_win::parse_wrapper_argv(&args) {
        match handles {
            Ok(handles) => {
                let code = klaus_sandbox_win::wrapper::run(handles.channel);
                std::process::exit(code as i32);
            }
            Err(error) => {
                eprintln!("klaus sandbox: {error}");
                // Exit code 2 for a malformed invocation, matching how this binary
                // rejects any other unrecognized argument.
                std::process::exit(2);
            }
        }
    }
    if args.first().map(String::as_str) == Some(klaus_sandbox_win::preparation::INSPECTION_ARG) {
        if args.len() != 1 {
            eprintln!("invalid Windows preparation inspection arguments");
            std::process::exit(2);
        }
        std::process::exit(run_windows_preparation_inspection());
    }
}

/// This internal worker accepts data over its owned pipe and only reads native
/// security state. A blocked OS read can be ended by terminating this worker,
/// without leaving a detached thread in the long-lived engine.
fn run_windows_preparation_inspection() -> i32 {
    match inspect_windows_preparation(&mut std::io::stdin().lock(), &mut std::io::stdout().lock()) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error}");
            2
        }
    }
}

fn inspect_windows_preparation(
    reader: &mut impl std::io::Read,
    writer: &mut impl std::io::Write,
) -> Result<(), String> {
    let envelope: klaus_sandbox_win::protocol::NativeSandboxRequest =
        klaus_sandbox_win::protocol::read_frame(reader)
            .map_err(|_| "invalid Windows preparation inspection request".to_string())?
            .ok_or_else(|| "missing Windows preparation inspection request".to_string())?;
    let request = &envelope.request;
    if !request.command.is_empty() || !request.env.is_empty() {
        return Err(
            "preparation inspection does not accept a target command or environment".to_string(),
        );
    }
    let mut trailing = [0u8; 1];
    if reader
        .read(&mut trailing)
        .map_err(|error| error.to_string())?
        != 0
    {
        return Err("preparation inspection accepts exactly one request".to_string());
    }

    let report = klaus_sandbox_win::preparation::inspect_native(&envelope)?;
    klaus_sandbox_win::protocol::write_frame(writer, &report)
        .map_err(|error| format!("writing the preparation inspection report failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use klaus_sandbox_win::preparation::PreparationReport;
    use klaus_sandbox_win::protocol::{read_frame, write_frame, SandboxRequest};
    use klaus_sandbox_win::{AbsPath, FilesystemBoundary, NetworkBoundary};

    use super::inspect_windows_preparation;

    fn inspection_request() -> SandboxRequest {
        SandboxRequest {
            command: Vec::new(),
            cwd: AbsPath::new(std::env::temp_dir().to_string_lossy()).unwrap(),
            env: BTreeMap::new(),
            filesystem: FilesystemBoundary::default(),
            network: NetworkBoundary::Denied,
            network_route_capability_sid: None,
            container_name: "klaus-preparation-entry-test".to_string(),
            terminal: None,
        }
    }

    #[test]
    fn preparation_worker_rejects_commands_environment_and_trailing_data() {
        for (command, environment, trailing) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let mut request = inspection_request();
            if command {
                request.command.push("DO_NOT_EXECUTE".to_string());
            }
            if environment {
                request
                    .env
                    .insert("PRIVATE".to_string(), "DO_NOT_REPORT".to_string());
            }
            let mut frame = Vec::new();
            write_frame(&mut frame, &request).unwrap();
            if trailing {
                frame.push(0);
            }
            let mut output = Vec::new();
            assert!(inspect_windows_preparation(&mut Cursor::new(frame), &mut output).is_err());
            assert!(output.is_empty());
        }
        for bytes in [Vec::new(), vec![1, 0, 0, 0]] {
            let mut output = Vec::new();
            assert!(inspect_windows_preparation(&mut Cursor::new(bytes), &mut output).is_err());
            assert!(output.is_empty());
        }
    }

    #[test]
    fn preparation_worker_writes_one_framed_read_only_report() {
        let mut input = Vec::new();
        write_frame(&mut input, &inspection_request()).unwrap();
        let mut output = Vec::new();
        inspect_windows_preparation(&mut Cursor::new(input), &mut output).unwrap();
        let length = output.len();
        let mut reader = Cursor::new(output);
        let report: PreparationReport = read_frame(&mut reader).unwrap().unwrap();
        assert_eq!(report.schema_version, 1);
        assert!(report.read_only);
        assert_eq!(reader.position(), length as u64);
    }
}
