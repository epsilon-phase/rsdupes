use fmmap::tokio::AsyncMmapFileExt;
use generic_array::GenericArray;
use generic_array::typenum;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::Metadata;
use std::io;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::io::stdin;
use std::io::stdout;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::*;
use tokio::task::JoinSet;
pub trait Actor {
    async fn operate(&mut self);
}
type ActorSender<T> = UnboundedSender<T>;
type ActorReceiver<T> = UnboundedReceiver<T>;
struct FileRecursionActor {
    queue: VecDeque<PathBuf>,
    sender: ActorSender<PathBuf>,
}

impl Actor for FileRecursionActor {
    async fn operate(&mut self) {
        while let Some(x) = self.queue.pop_front() {
            if let Ok(mut dir) = read_dir(x).await {
                while let Ok(Some(entry)) = dir.next_entry().await {
                    // println!("{} - {}", entry.path().to_string_lossy(), count);
                    if let Ok(info) = entry.file_type().await {
                        if info.is_file() {
                            // println!("{}", entry.path().to_string_lossy());
                            self.sender.send(entry.path()).unwrap();
                        } else if info.is_dir() {
                            self.queue.push_back(entry.path());
                        }
                    }
                }
            } else {
                println!("???");
            }
        }
        println!("Crawling finished");
    }
}
struct INodeFilter {
    seen: HashSet<(u64, u64)>,
    reciever: ActorReceiver<PathBuf>,
    sender: ActorSender<(PathBuf, Metadata)>,
}
impl Actor for INodeFilter {
    async fn operate(&mut self) {
        while let Some(x) = self.reciever.recv().await {
            if let Ok(meta) = metadata(&x).await {
                let fuid = (meta.dev(), meta.ino());
                if self.seen.insert(fuid) {
                    self.sender.send((x, meta)).unwrap();
                }
            }
        }
        println!("Finished inodefilter");
    }
}
struct FileSizeMessage {
    path: PathBuf,
    size: u64,
}
struct FileSizeFilter {
    seen: HashMap<u64, Option<PathBuf>>,
    receiver: ActorReceiver<(PathBuf, Metadata)>,
    sender: ActorSender<FileSizeMessage>,
    minimum_size: u64,
}
impl Actor for FileSizeFilter {
    async fn operate(&mut self) {
        while let Some((path, meta)) = self.receiver.recv().await {
            if meta.size() < self.minimum_size {
                continue;
            }
            match self.seen.get_mut(&meta.size()) {
                Some(x) => {
                    if x.is_some() {
                        self.sender
                            .send(FileSizeMessage {
                                path: x.take().unwrap(),
                                size: meta.size(),
                            })
                            .unwrap();
                    }
                    self.sender
                        .send(FileSizeMessage {
                            path,
                            size: meta.size(),
                        })
                        .unwrap();
                    self.seen.insert(meta.size(), None);
                }

                None => {
                    self.seen.insert(meta.size(), Some(path));
                }
            }
        }
        println!("Finished file size filter");
    }
}
// A lot of filesystems use this as their default size.
const BLOCK_SIZE: usize = 4096;
const MAX_HASHING_TASKS: usize = 300;
#[derive(Clone)]
struct PartialHashMessage {
    bytes_read: usize,
    file_size: u64,
    hasher: Sha256,
    path: PathBuf,
}
struct PartialHasher {
    read_size: usize,
    reciever: ActorReceiver<FileSizeMessage>,
    sender: ActorSender<PartialHashMessage>,
    joinset: JoinSet<Option<PartialHashMessage>>,
}
impl Actor for PartialHasher {
    async fn operate(&mut self) {
        let mut counter = 0;
        while let Some(msg) = self.reciever.recv().await {
            // println!(
            //     "{} partial hashes remaining, joinset has {} items",
            //     self.reciever.len(),
            //     self.joinset.len()
            // );
            let read_size = self.read_size;
            self.joinset.spawn(async move {
                let mut ret = PartialHashMessage {
                    bytes_read: read_size,
                    file_size: msg.size,
                    hasher: Sha256::default(),
                    path: msg.path,
                };
                let mut read_remaining = read_size;
                let mut buffer: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
                if let Ok(mut file) = File::open(&ret.path).await {
                    while read_remaining > 0 {
                        let b_size = read_remaining.min(BLOCK_SIZE);
                        let bytes_read = file.read(&mut buffer[..b_size]).await.unwrap();
                        read_remaining -= bytes_read;
                        ret.hasher.write_all(&buffer[..bytes_read]).unwrap();
                        if bytes_read == 0 {
                            // I don't know if this is necessary
                            break;
                        }
                    }
                    Some(ret)
                } else {
                    println!("Failed to read file");
                    None
                }
            });
            if self.joinset.len() >= MAX_HASHING_TASKS {
                while !self.joinset.is_empty() {
                    let result = self.joinset.join_next().await;
                    if let Some(Ok(Some(x))) = result {
                        self.sender.send(x).unwrap()
                    } else if let Some(Err(x)) = result {
                        eprintln!("Error! {}", x);
                    }
                    counter += 1;
                }
            } else {
                // println!("Under task limit");
                let former_len = self.joinset.len();
                while let Some(Ok(Some(item))) = self.joinset.try_join_next() {
                    self.sender.send(item).unwrap();
                    print!("\x1b[1F\x1b[K{counter}\n");
                    io::stdout().flush();
                    assert!(self.joinset.len() < former_len);
                    counter += 1;
                }
            }
        }
        println!("Partial hashing finished");
    }
}

struct PartialHashFilter {
    seen: HashMap<(u64, GenericArray<u8, typenum::U32>), Option<PartialHashMessage>>,
    receiver: ActorReceiver<PartialHashMessage>,
    sender: ActorSender<PartialHashMessage>,
}
impl Actor for PartialHashFilter {
    async fn operate(&mut self) {
        let mut temp_hasher = Sha256::default();
        while let Some(msg) = self.receiver.recv().await {
            msg.hasher.clone_into(&mut temp_hasher);
            let hash = temp_hasher.finalize_reset();
            if let Some(entry) = self.seen.get_mut(&(msg.file_size, hash)) {
                if entry.is_some() {
                    self.sender.send(entry.take().unwrap()).unwrap();
                }
                self.sender.send(msg).unwrap();
            } else {
                self.seen.insert((msg.file_size, hash), Some(msg));
            }
        }
    }
}

struct FullHashMessage {
    file_size: u64,
    hash: GenericArray<u8, typenum::U32>,
    path: PathBuf,
}
impl FullHashMessage {
    async fn from_partial_hash(mut msg: PartialHashMessage) -> Self {
        if msg.bytes_read == msg.file_size as usize {
            return FullHashMessage {
                file_size: msg.file_size,
                hash: msg.hasher.finalize(),
                path: msg.path,
            };
        }
        let fmap = fmmap::tokio::AsyncMmapFile::open(&msg.path).await;
        match fmap{
            Ok(mapped)=>{
                // Oh mmap, how did I ever live without you? <3
                let data = std::io::IoSlice::new(&mapped.as_slice()[msg.bytes_read..]);
                let written = msg.hasher.write_vectored(&[data]).unwrap();
                assert_eq!(written,mapped.len()-msg.bytes_read);
                FullHashMessage{
                    file_size:msg.file_size,
                    hash:msg.hasher.finalize(),
                    path: msg.path
                }
            },
            Err(error)=>{
                panic!("{}", error);
            }
        }
        // let file = File::open(&msg.path).await;
        // match file {
        //     Ok(mut file) => {
        //         file.seek(SeekFrom::Start(msg.bytes_read as u64))
        //             .await
        //             .unwrap();
        //         let mut buffer: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
        //         while let Ok(bytes_read) = file.read(&mut buffer).await {
        //             if bytes_read == 0 {
        //                 break;
        //             }
        //             msg.hasher.write_all(&buffer[..bytes_read]).unwrap();
        //         }
        //         FullHashMessage {
        //             file_size: msg.file_size,
        //             hash: msg.hasher.finalize(),
        //             path: msg.path,
        //         }
        //     }
        //     Err(error) => {
        //         panic!("{}", error);
        //     }
        // }
    }
}
struct FullHasher {
    receiver: ActorReceiver<PartialHashMessage>,
    sender: ActorSender<FullHashMessage>,
    joinset: JoinSet<FullHashMessage>,
}
impl Actor for FullHasher {
    async fn operate(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.joinset.spawn(FullHashMessage::from_partial_hash(msg));
            if self.joinset.len() > MAX_HASHING_TASKS {
                self.sender
                    .send(self.joinset.join_next().await.unwrap().unwrap())
                    .unwrap();
            } else {
                while let Some(Ok(item)) = self.joinset.try_join_next() {
                    self.sender.send(item).unwrap();
                }
            }
        }
    }
}
// ngl a bloom filter would work nicely here.
struct FullHashFilter {
    seen: HashMap<u64, HashMap<GenericArray<u8, typenum::U32>, Option<PathBuf>>>,
    receiver: ActorReceiver<FullHashMessage>,
    sender: ActorSender<FullHashMessage>,
}
impl Actor for FullHashFilter {
    async fn operate(&mut self) {
        while !self.receiver.is_closed() {
            let msg = self.receiver.recv();
            let msg = msg.await;
            if msg.is_none() {
                break;
            }
            let msg = msg.unwrap();

            let c = self.seen.entry(msg.file_size).or_default();
            if let Some(entry) = c.get_mut(&msg.hash) {
                if entry.is_some() {
                    self.sender
                        .send(FullHashMessage {
                            file_size: msg.file_size,
                            hash: msg.hash,
                            path: entry.take().unwrap(),
                        })
                        .unwrap();
                }
                self.sender.send(msg).unwrap();
            } else {
                c.insert(msg.hash, Some(msg.path));
            }
        }
    }
}
pub struct TestCollector {
    reciever: ActorReceiver<FullHashMessage>,
    map: BTreeMap<u64, BTreeMap<GenericArray<u8, typenum::U32>, Vec<PathBuf>>>,
}
impl TestCollector {
    fn write_to_file(&self, output_path: &str) {
        let mut map: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        for (key, value) in self.map.iter() {
            let key = key.to_string();
            let entry = map.entry(key).or_default();
            for (hash, paths) in value.iter() {
                let key = format!("{:x}", hash);
                entry.entry(key).or_insert(
                    paths
                        .iter()
                        .map(|x| x.to_string_lossy().to_string())
                        .collect(),
                );
            }
        }
        serde_json::to_writer_pretty(std::fs::File::create(output_path).unwrap(), &map).unwrap();
    }
}
impl Actor for TestCollector {
    async fn operate(&mut self) {
        let mut collected = 0;
        println!("Starting collector");
        while let Some(x) = self.reciever.recv().await {
            collected += 1;
            self.map
                .entry(x.file_size)
                .or_default()
                .entry(x.hash)
                .or_default()
                .push(x.path);
            print!(
                "\r{} Items collected in {} size buckets consisting of {} different hashes",
                collected,
                self.map.len(),
                self.map.values().fold(0, |acc, map| acc + map.len())
            );
            io::stdout().flush().unwrap();
        }
        self.write_to_file("collisions.json");
    }
}

struct FileExclusionFilter {
    reciever: ActorReceiver<PathBuf>,
    sender: ActorSender<PathBuf>,
    excluded_extensions: Option<Vec<String>>,
    included_extensions: Option<Vec<String>>,
}
impl Actor for FileExclusionFilter {
    async fn operate(&mut self) {
        let mut vec = Vec::new();
        loop {
            let read = self.reciever.recv_many(&mut vec, 100).await;
            if read == 0 {
                break;
            }
            for x in vec.drain(..read) {
                if self.excluded_extensions.is_some()
                    && self
                        .excluded_extensions
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|ext| x.extension().is_some_and(|x| *ext == *x.to_string_lossy()))
                {
                    continue;
                }
                if self.included_extensions.is_some()
                    && !self
                        .included_extensions
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|ext| x.extension().is_some_and(|x| *x.to_string_lossy() == *ext))
                {
                    continue;
                }
                self.sender.send(x).unwrap();
            }
        }
    }
}
#[derive(Clone, Debug)]
struct DuplicateMessage {
    original: PathBuf,
    duplicate: PathBuf,
}
struct BytewiseFileComparator {
    // =S
    map: BTreeMap<u64, BTreeMap<GenericArray<u8, typenum::U32>, BTreeSet<PathBuf>>>,
    receiver: ActorReceiver<FullHashMessage>,
    // A broadcast, just in case I figure out why to send it to more than one actor.
    sender: tokio::sync::broadcast::Sender<DuplicateMessage>,
}

impl Actor for BytewiseFileComparator {
    async fn operate(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            let b = fmmap::tokio::AsyncMmapFile::open(&msg.path).await;
            if b.is_err() {
                continue;
            }
            let b = b.unwrap();
            let top = self.map.entry(msg.file_size).or_default();
            let hash = top.entry(msg.hash).or_default();
            let mut found_match = None;
            for key in hash.iter() {
                let a = fmmap::tokio::AsyncMmapFile::open(&key).await.unwrap();
                if a.as_slice() == b.as_slice() {
                    found_match = Some(key);
                    break;
                }
            }
            if found_match.is_some() {
                println!(
                    "{} === {}",
                    msg.path.to_string_lossy(),
                    found_match.unwrap().to_string_lossy()
                );
                // This is necessary to permit just printing the results
                if self.sender.receiver_count() > 0 {
                    self.sender
                        .send(DuplicateMessage {
                            original: found_match.cloned().unwrap(),
                            duplicate: msg.path,
                        })
                        .unwrap();
                }
            } else {
                hash.insert(msg.path);
            }
        }
    }
}

struct HardLinker {
    reciever: tokio::sync::broadcast::Receiver<DuplicateMessage>,
    confirm_actions: bool,
}
impl Actor for HardLinker {
    async fn operate(&mut self) {
        let term = console::Term::stdout();
        loop {
            let msg = self.reciever.recv().await;
            if let Err(RecvError::Closed) = msg {
                break;
            }
            let msg = msg.unwrap();
            if self.confirm_actions {
                println!(
                    "Do you want to link {} to {}?",
                    msg.duplicate.to_string_lossy(),
                    msg.original.to_string_lossy()
                );
                let response = term.read_char().unwrap();
                if response != 'y' {
                    continue;
                }
            }
            let mut backup = msg.duplicate.clone();
            let mut ext = backup
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            ext.push_str(".bak");
            backup.set_extension(ext);
            rename(&msg.duplicate, &backup).await.unwrap();
            if let Err(e) = hard_link(&msg.original, &msg.duplicate).await {
                eprintln!("Link error {}", e);
                rename(&backup, &msg.duplicate).await;
            } else {
                remove_file(&backup).await;
                println!(
                    "Linked {} to {}",
                    msg.duplicate.to_string_lossy(),
                    msg.original.to_string_lossy()
                )
            }
        }
    }
}
struct JsonDumper {
    reciever: tokio::sync::broadcast::Receiver<DuplicateMessage>,
    map: BTreeMap<String, Vec<String>>,
    output: PathBuf,
}
impl Actor for JsonDumper {
    async fn operate(&mut self) {
        use tokio::sync::broadcast;
        //We want to panic early in this because otherwise it's all for naught
        let file = std::fs::File::create(&self.output).unwrap();
        loop {
            let msg = self.reciever.recv().await;
            if let Err(RecvError::Closed) = msg {
                break;
            }
            let msg = msg.unwrap();
            let list = self
                .map
                .entry(msg.original.to_string_lossy().to_string())
                .or_default();
            list.push(msg.duplicate.to_string_lossy().to_string());
        }
        serde_json::to_writer_pretty(file, &self.map);
    }
}
pub fn run_actors(args: &crate::Args) -> JoinSet<()> {
    let (file_sender, file_recv) = unbounded_channel();
    let (file_ext_sender, file_ext_recv) = unbounded_channel();
    let (size_sender, size_recv) = unbounded_channel();
    let (size_filter_sender, size_filter_recv) = unbounded_channel();
    let (partial_hash_sender, partial_filter_recv) = unbounded_channel();
    let (full_hash_sender, full_hash_recv) = unbounded_channel();
    let (full_filter_sender, full_filter_recv) = unbounded_channel();
    let (collector_sender, collector_reciever) = unbounded_channel();
    let (duplicate_sender, duplicate_recv) = tokio::sync::broadcast::channel(100);
    let mut js = JoinSet::new();
    let path = args
        .directory_entry_point
        .clone()
        .unwrap_or(PathBuf::from("."));
    {
        let needs_filter = args.excluded_extensions.is_some() || args.included_extensions.is_some();
        js.spawn(async move {
            let mut file_actor = FileRecursionActor {
                queue: VecDeque::new(),
                sender: file_sender,
            };
            file_actor.queue.push_back(path);
            file_actor.operate().await
        });
        if needs_filter {
            let exc_ext = args.excluded_extensions.clone();
            let inc_ext = args.included_extensions.clone();
            js.spawn(async move {
                let mut ext_filter = FileExclusionFilter {
                    excluded_extensions: exc_ext,
                    included_extensions: inc_ext,
                    sender: file_ext_sender,
                    reciever: file_recv,
                };
                ext_filter.operate().await;
            });
            js.spawn(async move {
                let mut size = INodeFilter {
                    reciever: file_ext_recv,
                    seen: HashSet::default(),
                    sender: size_sender,
                };
                size.operate().await;
            });
        } else {
            js.spawn(async move {
                let mut size = INodeFilter {
                    reciever: if needs_filter {
                        file_ext_recv
                    } else {
                        file_recv
                    },
                    seen: HashSet::default(),
                    sender: size_sender,
                };
                size.operate().await;
            });
        }
    }
    {
        // Ahh, the necessities of async programming.
        // At least I can't blame it all on rust
        let min_size = args.minimum_size;
        println!("Minimimum_size {min_size}");
        js.spawn(async move {
            let mut file_filter = FileSizeFilter {
                receiver: size_recv,
                sender: size_filter_sender,
                seen: HashMap::default(),
                minimum_size: min_size,
            };
            file_filter.operate().await;
        });
    }
    js.spawn(async move {
        let mut partial_hasher = PartialHasher {
            reciever: size_filter_recv,
            sender: partial_hash_sender,
            joinset: JoinSet::new(),
            read_size: 4096,
        };
        partial_hasher.operate().await;
    });
    js.spawn(async move {
        let mut hash_filter = PartialHashFilter {
            receiver: partial_filter_recv,
            sender: full_hash_sender,
            seen: HashMap::default(),
        };
        hash_filter.operate().await;
    });
    js.spawn(async move {
        let mut full_hasher = FullHasher {
            joinset: JoinSet::new(),
            receiver: full_hash_recv,
            sender: full_filter_sender,
        };
        full_hasher.operate().await;
    });
    js.spawn(async move {
        let mut full_filter = FullHashFilter {
            receiver: full_filter_recv,
            sender: collector_sender,
            seen: HashMap::new(),
        };
        println!("Starting full hash filter");
        full_filter.operate().await;
        println!("Full filter ended?");
    });
    {
        let duplicate_sender = duplicate_sender.clone();
        js.spawn(async move {
            let mut tc = BytewiseFileComparator {
                map: BTreeMap::new(),
                receiver: collector_reciever,
                sender: duplicate_sender,
            };
            tc.operate().await;
        });
    }
    if args.hard_link {
        let confirm = args.confirm_actions;
        println!("Starting hard linker");
        js.spawn(async move {
            let mut tc = HardLinker {
                confirm_actions: confirm,
                reciever: duplicate_recv,
            };
            tc.operate().await;
        });
    }
    if let Some(path) = args.json_dump.as_ref() {
        let path = path.clone();
        let recv = duplicate_sender.subscribe();
        js.spawn(async move {
            let mut tc = JsonDumper {
                map: BTreeMap::new(),
                output: path,
                reciever: recv,
            };
            tc.operate().await;
        });
    }

    js
}
