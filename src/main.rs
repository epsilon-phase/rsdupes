use std::path::{Path, PathBuf};

use clap::Parser;
use mimalloc::MiMalloc;
use std::sync::{Arc, mpsc};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
mod actors;
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

use crate::file_selector::{
    FilePartialHashSelector, FileSizeSelector, FullHashSelector, hasher_pipeline,
};
mod file_selector;
async fn go_inside(
    directory: PathBuf,
    sender: tokio::sync::mpsc::Sender<std::path::PathBuf>,
) -> () {
    if let Ok(mut directory) = tokio::fs::read_dir(&directory).await {
        while let Ok(Some(file)) = directory.next_entry().await {
            match file.file_type().await {
                Ok(filetype) => {
                    if filetype.is_dir() {
                        let path = Arc::new(file.path().clone());
                        let send = sender.clone();
                        tokio::spawn(async move {
                            go_inside(path.to_path_buf(), send).await;
                        });
                    } else if filetype.is_file() {
                        // println!("{}", file.path().to_string_lossy());
                        sender.send(file.path()).await.unwrap();
                    }
                }
                Err(_e) => {}
            }
        }
    }
}
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
    #[arg(short,long)]
    included_extensions: Option<Vec<String>>,
    /// File extensions to explicitly disinclude
    /// Not sensible to combine with desired extensions, but should work just fine.
    #[arg(short,long)]
    excluded_extensions: Option<Vec<String>>
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
    // runtime.block_on(async {
    //     let mut channel = tokio::sync::mpsc::channel(100);
    //     runtime.spawn(go_inside(args.directory_entry_point.unwrap_or(PathBuf::from(".")), channel.0));

    //     let mut thing = file_selector::FileSizeSelector::default();
    //     thing.run_collection(&mut channel.1).await;
    //     let mut size = 0;
    //     for (k, v) in thing.by_sizes.iter() {
    //         println!("{k}\n\t{}", v.len());
    //         size += v.len();
    //     }
    //     println!("Total files under consideration {size}");
    //     let mut full_collisions = 0;
    //     let mut max_bucket_size = 0;
    //     let mut full_collision_buckets = hasher_pipeline(&thing.by_sizes).await;
    //     full_collision_buckets.sort_by_key(|x|x.0);
    //     for i in full_collision_buckets.iter() {
    //         for (_, bucket) in i.1.by_hash.iter() {
    //             max_bucket_size = max_bucket_size.max(bucket.len());
    //             full_collisions += bucket.len();
    //         }
    //     }
    //     println!("Found {full_collisions} full sha256 collisions, largest bucket contains {max_bucket_size} items");
    //     let file = tokio::fs::File::create("collisions.json").await;
    //     if let Ok(file) = file{
    //         serde_json::to_writer_pretty(file.into_std().await, &full_collision_buckets).unwrap();
    //     }else{
    //         println!("Error! {}", file.err().unwrap());
    //     }
    //     // let mut potential_collisions = 0;
    //     // for mut i in partial_hashes.join_all().await {
    //     //     for (hash, bucket) in i.by_hash.drain() {
    //     //         potential_collisions += bucket.len();
    //     //         if bucket.len() > 0 {
    //     //             println!(
    //     //                 "First item of a non-1 bucket {}",
    //     //                 bucket[0].to_string_lossy()
    //     //             );
    //     //         }
    //     //         full_hashes.spawn(FullHashSelector::from_path_bucket(bucket));
    //     //     }
    //     // }

    //     // println!("There are {potential_collisions} partial hash collisions");
    //     // let mut collisions = 0;
    //     // for mut i in full_hashes.join_all().await.drain(..) {
    //     //     for (k, v) in i.by_hash.drain() {
    //     //         collisions += v.len();
    //     //     }
    //     // }
    //     // println!("There are still {collisions}");
    // });
}
