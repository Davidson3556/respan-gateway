use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    ExitCode::from(respan_gateway::run(std::env::args_os()).await)
}
