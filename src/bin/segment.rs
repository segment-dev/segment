use anyhow::Result;
use clap::Parser;
use fern::Dispatch;
use segment::config;
use segment::server;

/// An in-memory key-value database with dynamic keyspaces
#[derive(Debug, Parser)]
struct Args {
    /// Specify the server port
    #[clap(long)]
    port: Option<u16>,

    /// Specify the config file path
    #[clap(long)]
    config: Option<String>,

    /// Specify the max memory limit in megabytes
    #[clap(long)]
    max_memory: Option<u64>,

    /// Start the server in debug mode
    #[clap(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logger(args.debug)?;
    let config = config::resolve(args.port, args.max_memory, args.config)?;
    server::start(config).await?;

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
