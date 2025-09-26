use std::path::PathBuf;

use clap::error::ErrorKind;
use clap::{Parser, command};
use mimalloc::MiMalloc;
mod actor_types;
mod actors;
mod constants;
mod file_discovery_actors;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    single_thread: bool,
    #[arg(short, long, default_value_t = 25)]
    threads: usize,
    #[arg(value_name = "DIRECTORY")]
    directory_entry_point: Option<Vec<PathBuf>>,
    /// File extensions to limit operation to.
    #[arg(short, long)]
    included_extensions: Option<Vec<String>>,
    /// File extensions to explicitly disinclude
    /// Not sensible to combine with desired extensions, but should work just fine.
    #[arg(short, long)]
    excluded_extensions: Option<Vec<String>>,
    /// Exclude specific paths
    #[arg(short, long)]
    excluded_paths: Vec<PathBuf>,
    /// Exclude files below a specified number of bytes.
    ///
    /// Defaults to 1, specify zero to include zero length files, which can be dangerous
    /// as they are sometimes used as program flags or program specific scratch space.
    ///
    /// In general, it is best to set this to at least a multiple of the filesystem's block size
    /// if you are seeking to reclaim disk space.
    ///
    /// Supports suffixes T,G,M,K as well as Tib,Gib,Mib,Kib
    #[arg(short, long, default_value_t = 1,value_parser=crate::parse_size)]
    minimum_size: u64,
    /// Replace duplicates with hardlinks.
    /// This is dangerous fun if you aren't careful.
    ///
    /// Run with Confirm-mode if you want to approve it
    #[arg(short, long)]
    hard_link: bool,
    /// Confirm if you want to relink the files.
    #[arg(short, default_value_t = false)]
    confirm_actions: bool,
    /// Create a symbolic link whenever it isn't possible to create a hard link, such as when the
    /// files are across devices
    #[arg(short, long, default_value_t = false)]
    fallback_to_symbolic: bool,
    /// Write duplicates to a file.
    #[arg(short, long)]
    json_dump: Option<PathBuf>,
}
#[cfg(test)]
mod arg_tests {
    use clap::error::ErrorKind;

    #[test]
    fn test_size_parsing() {
        use super::*;
        let input = ["10gb", "1m", "1kb", "1k", "1kib", "1000"];
        let expected: [u64; _] = [
            10_000_000_000u64,
            1_000_000u64,
            1_000u64,
            1_000u64,
            1_024u64,
            1000u64,
        ];
        for i in input.iter().zip(expected.iter()) {
            assert_eq!(parse_size(i.0).unwrap(), *i.1);
        }
        assert!(parse_size("kib12").is_err());
    }
}
fn parse_size(item: &str) -> Result<u64, clap::error::Error> {
    let suffix = item.trim_start_matches(&['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']);
    let number: u64 = if let Ok(num) = item.trim_end_matches(suffix).parse() {
        num
    } else {
        return Err(clap::Error::raw(
            ErrorKind::ValueValidation,
            "Invalid number supplied for minimum size",
        ));
    };
    let multiplier = match suffix {
        "M" | "m" | "mb" | "Mb" => 1_000_000,
        "Mib" | "mib" => 1024 * 1024,
        "K" | "k" | "kb" | "Kb" => 1_000,
        "Kib" | "kib" => 1024,
        "G" | "g" | "gb" | "Gb" => 1_000_000_000,
        "Gib" | "gib" => 1024 * 1024 * 1024,
        "Tb" | "tb" | "t" | "T" => 1_000_000_000_000,
        "tib" | "Tib" => 1024 * 1024 * 1024 * 1024,
        "" => 1,
        _ => {
            return Err(clap::error::Error::raw(
                ErrorKind::ValueValidation,
                "Invalid suffix",
            ));
        }
    };
    Ok(number * multiplier)
}
fn main() {
    let args = Args::parse();
    if !args.hard_link && args.json_dump.is_none() {
        println!(
            "Specify -h or -j to make this program carry out an action. This will just print duplicates"
        );
    }
    let runtime = if args.single_thread {
        tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(10)
            .enable_time()
            .build()
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(25)
            .max_blocking_threads(100)
            .enable_time()
            .build()
    }
    .unwrap();
    runtime.block_on(async move {
        let mut js = actors::run_actors(&&args);
        while !js.is_empty() {
            js.join_next().await;
        }
    })
}
