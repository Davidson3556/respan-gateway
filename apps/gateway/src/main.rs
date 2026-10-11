use std::process::ExitCode;

fn main() -> ExitCode {
    ExitCode::from(respan_gateway::execute(std::env::args_os()))
}
