use std::path::{Path, PathBuf};

use clap::Parser;
use mimalloc::MiMalloc;
use std::sync::{Arc, mpsc};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
mod actors;
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    single_thread: bool,
    #[arg(short, long, default_value_t = 25)]
    threads: usize,
    #[arg(value_name = "DIRECTORY")]
    directory_entry_point: Option<PathBuf>,
    /// File extensions to limit operation to.
    #[arg(short, long)]
    included_extensions: Option<Vec<String>>,
    /// File extensions to explicitly disinclude
    /// Not sensible to combine with desired extensions, but should work just fine.
    #[arg(short, long)]
    excluded_extensions: Option<Vec<String>>,
    /// Exclude files below a specified number of bytes.
    ///
    /// Defaults to 1, specify zero to include zero length files, which can be dangerous
    /// as they are sometimes used as program flags or program specific scratch space.
    ///
    /// In general, it is best to set this to at least a multiple of the filesystem's block size
    /// if you are seeking to deduplicate larger files
    #[arg(short, long, default_value_t = 1)]
    minimum_size: u64,
}
fn main() {
    let args = Args::parse();
    let runtime = if args.single_thread {
        tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(10)
            .build()
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(25)
            .max_blocking_threads(100)
            .build()
    }
    .unwrap();
    runtime.block_on(async move {
        use actors::Actor;

        let mut js = actors::run_actors(&&args);
        while !js.is_empty() {
            js.join_next().await;
        }
    })
}
