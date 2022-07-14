use anyhow::Result;
use clap::Parser;
use fern::Dispatch;
use log::info;
use segment::server;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Args {
    /// Specify the server port
    #[clap(long, default_value_t = 9890)]
    port: u16,

    /// Specify the max memory limit in megabytes
    #[clap(long, default_value_t = 1024)]
    max_memory: u64,

    /// Start the server in debug mode
    #[clap(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logger(args.debug)?;
    info!("Starting server on 127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    server::start(listener, args.max_memory).await?;
    Ok(())
}

fn setup_logger(debug: bool) -> Result<()> {
    Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {}",
                chrono::Local::now().format("%d %b %Y %H:%M:%S%.3f"),
                record.level(),
                message
            ))
        })
        .level(if debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .chain(std::io::stdout())
        .apply()?;

    Ok(())
}
