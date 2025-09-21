use core::error;
use sha2::{
    Digest, Sha224,
    digest::{
        FixedOutput,
        generic_array::{ArrayLength, GenericArray},
        typenum,
    },
};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::Arc,
    task::Poll,
};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{RwLock, mpsc},
    task::JoinSet,
};
#[derive(Default)]
pub struct FileSizeSelector {
    pub by_sizes: HashMap<u64, Vec<PathBuf>>,
}
impl FileSizeSelector {
    pub async fn run_collection(&mut self, receiver: &mut tokio::sync::mpsc::Receiver<PathBuf>) {
        let mut seen_files: HashSet<(u64, u64)> = HashSet::new();
        while let Some(path) = receiver.recv().await {
            if let Ok(info) = tokio::fs::metadata(&path).await {
                let fuid = (info.dev(), info.ino());
                if info.size() == 0 || !seen_files.insert(fuid) {
                    // Empty files are all duplicates so they aren't worth dealing with and *might* be dangerous to touch
                    // It's also worthwhile to ignore all hardlinks... Maybe it should just use the .links attribute?
                    continue;
                }
                let thing = self.by_sizes.entry(info.len()).or_default();
                thing.push(path);
            } else {
                eprintln!("Could not inspect {}", &path.to_string_lossy());
            }
        }
    }
}

#[derive(Default)]
pub struct FilePartialHashSelector {
    // This might be better as just a vector
    pub by_hash: HashMap<GenericArray<u8, typenum::U32>, Vec<PathBuf>>,
}
impl FilePartialHashSelector {
    pub async fn from_path_bucket(
        paths: Vec<PathBuf>,
        partial_size: usize,
    ) -> FilePartialHashSelector {
        let mut ret = FilePartialHashSelector::default();
        let mut task_set: JoinSet<(PathBuf, GenericArray<u8, typenum::U32>)> =
            tokio::task::JoinSet::new();
        for i in paths {
            let c = i.clone();
            if let Ok(mut file) = tokio::fs::File::open(&i).await {
                task_set.spawn(async move {
                    let mut remaining_size = partial_size;
                    // My hearo, not that I had expected this syntax to work given my
                    // experience with rust's type system
                    let mut buffer = [0; 4096];
                    let mut hasher = sha2::Sha256::new();
                    while remaining_size > 0 {
                        let bytes_to_read = 4096.min(remaining_size);
                        let bytes_read = file.read(&mut buffer[..bytes_to_read]).await.unwrap();
                        hasher.write_all(&buffer[..bytes_read]).unwrap();
                        if bytes_read == 0 {
                            break;
                        }
                        remaining_size -= bytes_read;
                    }
                    (c, hasher.finalize_fixed())
                });
            }
        }
        for (path, hash) in task_set.join_all().await.drain(..) {
            ret.by_hash.entry(hash).or_default().push(path);
        }
        ret.by_hash = ret
            .by_hash
            .drain()
            .filter(|(_, paths)| paths.len() > 1)
            .collect();
        ret
    }
}
use serde::{Serialize, ser::SerializeMap};
#[derive(Default, Clone)]
pub struct FullHashSelector {
    pub by_hash: HashMap<GenericArray<u8, typenum::U32>, Vec<PathBuf>>,
}
impl Serialize for FullHashSelector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use std::io::Cursor;
        let mut map = serializer.serialize_map(Some(self.by_hash.len())).unwrap();
        let mut buf = [0u8; 64];
        for (key, paths) in self.by_hash.iter() {
            let mut cursor = Cursor::new(&mut buf[..]);
            write!(cursor, "{:x}", key).unwrap();
            map.serialize_entry(str::from_utf8(&buf).unwrap(), paths)
                .unwrap();
        }
        map.end()
    }
}
impl FullHashSelector {
    pub async fn from_path_bucket(paths: Vec<PathBuf>) -> FullHashSelector {
        let mut ret = FullHashSelector::default();
        let mut task_set: JoinSet<(PathBuf, GenericArray<u8, typenum::U32>)> =
            tokio::task::JoinSet::new();
        for i in paths {
            let c = i.clone();
            if let Ok(mut file) = tokio::fs::File::open(&i).await {
                task_set.spawn(async move {
                    // Tbh, might as well find the 'correct' size to read chunks of. The fact it was originally correct
                    // for my own filesystem doesn't really mean too much
                    let block_size = file.metadata().await.unwrap().blksize();
                    // My hearo, not that I had expected this syntax to work given my
                    // experience with rust's type system
                    let mut vec = vec![0; block_size as usize];
                    let mut hasher = sha2::Sha256::new();
                    loop {
                        let bytes_read = file.read(&mut vec[..]).await.unwrap();
                        hasher.write_all(&vec[..bytes_read]).unwrap();
                        if bytes_read == 0 {
                            break;
                        }
                    }
                    (c, hasher.finalize_fixed())
                });
            }
        }
        for (path, hash) in task_set.join_all().await.drain(..) {
            ret.by_hash.entry(hash).or_default().push(path);
        }
        ret.by_hash = ret
            .by_hash
            .drain()
            .filter(|(_, paths)| paths.len() > 1)
            .collect();
        ret
    }
}
pub async fn hasher_pipeline(buckets: &HashMap<u64, Vec<PathBuf>>) -> Vec<(u64, FullHashSelector)> {
    let (sender, mut reciever) = mpsc::channel(1000);
    let mut partial_hasher_set: JoinSet<()> = JoinSet::new();
    let mut full_hasher_set: JoinSet<(u64, FullHashSelector)> = JoinSet::new();
    let mut ret = Vec::new();

    for (size, paths) in buckets.iter() {
        let mut sc = sender.clone();
        let bucket = paths.clone();
        let size = *size;
        partial_hasher_set.spawn(async move {
            let fphs = FilePartialHashSelector::from_path_bucket(bucket, 4096).await;
            sc.send((size, fphs)).await.unwrap();
        });
    }

    while partial_hasher_set.join_next().await.is_some() {
        print!(
            "\r{: >9} partial tasks left and {: >9} full hashing tasks left",
            partial_hasher_set.len(),
            full_hasher_set.len()
        );
        while let Some(x) = full_hasher_set.try_join_next() {
            ret.push(x.unwrap());
        }
        while let Ok(item) = reciever.try_recv() {
            let (size, fphs) = item;
            for (hash, bucket) in fphs.by_hash.iter() {
                let bucket = bucket.clone();
                full_hasher_set.spawn(async move {
                    (
                        size,
                        FullHashSelector::from_path_bucket(bucket.clone()).await,
                    )
                });
            }
        }
    }
    println!("---");
    while let Some(Ok((size, bucket))) = full_hasher_set.join_next().await {
        ret.push((size, bucket));
        print!("\r{: >9} tasks left", full_hasher_set.len());
    }
    println!("");
    ret
}
