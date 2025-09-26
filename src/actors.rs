use crate::actor_types::{ActorReceiver, ActorSender};
use crate::actors::StatusUpdate::PartialHash;
use crate::constants::{BLOCK_SIZE, MAX_HASHING_TASKS};
use crate::file_discovery_actors::{
    FileExclusionFilter, FileRecursionActor, FileSizeFilter, FileSizeMessage, INodeFilter,
};
use fmmap::tokio::AsyncMmapFileExt;
use generic_array::GenericArray;
use generic_array::typenum;
use sha2::Digest;
use sha2::Sha256;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use tokio::fs::*;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::*;
use tokio::task::JoinSet;

pub trait Actor {
    async fn operate(&mut self);
}

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
    display_channel: ActorSender<StatusUpdate>,
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
            let last_count = counter;
            let read_size = self.read_size;
            self.joinset.spawn(async move {
                let mut ret = PartialHashMessage {
                    bytes_read: 0,
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
                        ret.bytes_read += bytes_read;
                        read_remaining -= bytes_read;
                        ret.hasher.write_all(&buffer[..bytes_read]).unwrap();
                        if bytes_read == 0 {
                            // I don't know if this is necessary
                            break;
                        }
                    }
                    Some(ret)
                } else {
                    // println!("Failed to read file");
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
                    // print!("\x1b[1F\x1b[K{counter}\n");
                    // io::stdout().flush();
                    assert!(self.joinset.len() < former_len);
                    counter += 1;
                }
            }
            self.display_channel
                .send(PartialHash(counter - last_count))
                .unwrap();
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
        match fmap {
            Ok(mapped) => {
                // Oh mmap, how did I ever live without you? <3
                let data = std::io::IoSlice::new(&mapped.as_slice()[msg.bytes_read..]);
                let written = msg.hasher.write_vectored(&[data]).unwrap();
                assert_eq!(written, mapped.len() - msg.bytes_read);
                FullHashMessage {
                    file_size: msg.file_size,
                    hash: msg.hasher.finalize(),
                    path: msg.path,
                }
            }
            Err(error) => {
                panic!("{}", error);
            }
        }
    }
}
struct FullHasher {
    receiver: ActorReceiver<PartialHashMessage>,
    sender: ActorSender<FullHashMessage>,
    joinset: JoinSet<FullHashMessage>,
    display_sender: ActorSender<StatusUpdate>,
}
impl Actor for FullHasher {
    async fn operate(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.joinset.spawn(FullHashMessage::from_partial_hash(msg));
            self.display_sender.send(StatusUpdate::FullHash(1)).unwrap();
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
    sender: ActorSender<DuplicateMessage>,
    display_sender: ActorSender<StatusUpdate>,
}

impl Actor for BytewiseFileComparator {
    async fn operate(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            let b = fmmap::tokio::AsyncMmapFile::open(&msg.path).await;
            // Doing the permissions check here seems smart, but it's easy to imagine that this
            // could end up being the wrong place.
            //
            // The other issue here is that it isn't suitable for windows, although I am not
            // a frequent user of that system, I would rather support it properly.
            let b_perms = metadata(&msg.path).await.unwrap().permissions();
            if b.is_err() {
                continue;
            }
            let b = b.unwrap();
            let top = self.map.entry(msg.file_size).or_default();
            let hash = top.entry(msg.hash).or_default();
            let mut found_match = None;
            for key in hash.iter() {
                let a_perms = metadata(&key).await.unwrap().permissions();
                if a_perms != b_perms {
                    continue;
                }
                let a = fmmap::tokio::AsyncMmapFile::open(&key).await.unwrap();
                if a.as_slice() == b.as_slice() {
                    found_match = Some(key);
                    break;
                }
            }
            if found_match.is_some() {
                // This is necessary to permit just printing the results

                if !self.sender.is_closed() {
                    self.sender
                        .send(DuplicateMessage {
                            original: found_match.cloned().unwrap(),
                            duplicate: msg.path,
                        })
                        .unwrap();
                }
                self.display_sender.send(StatusUpdate::Duplicates(1));
            } else {
                hash.insert(msg.path);
            }
        }
    }
}

struct HardLinker {
    receiver: ActorReceiver<DuplicateMessage>,
    confirm_actions: bool,
    forwarder: Option<ActorSender<DuplicateMessage>>,
    display: ActorSender<StatusUpdate>,
    fallback_to_symbolic: bool,
}
impl Actor for HardLinker {
    async fn operate(&mut self) {
        let term = console::Term::stdout();
        loop {
            let msg = self.receiver.recv().await;
            if msg.is_none() {
                break;
            }
            let msg = msg.unwrap();
            self.forwarder
                .as_mut()
                .inspect(|x| x.send(msg.clone()).unwrap());
            // .. I don't think this is useful. It's impossible to read
            // It needs to turn off the other printing to remain visible on the screen
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
            // It should check here that both files are on the same device, as otherwise
            // hard linking is impossible, an additional flag may then control whether or not
            // a symbolic link is created instead.
            let original_meta = metadata(&msg.original).await.unwrap();
            let duplicate_meta = metadata(&msg.duplicate).await.unwrap();
            let mut hard_linking = true;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if original_meta.dev() != duplicate_meta.dev() {
                    if self.fallback_to_symbolic {
                        hard_linking = false;
                    } else {
                        continue;
                    }
                }
            }
            rename(&msg.duplicate, &backup).await.unwrap();
            let (link_result, update) = if hard_linking {
                (
                    hard_link(&msg.original, &msg.duplicate).await,
                    StatusUpdate::HardLinks(1),
                )
            } else {
                (
                    symlink(&msg.original, &msg.duplicate).await,
                    StatusUpdate::SymbolicLinks(1),
                )
            };
            if let Err(e) = link_result {
                eprintln!("Link error {}", e);
                rename(&backup, &msg.duplicate).await.unwrap();
            } else {
                remove_file(&backup).await.unwrap();
                self.display.send(update).unwrap();
            }
        }
    }
}
struct JsonDumper {
    reciever: ActorReceiver<DuplicateMessage>,
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
            if msg.is_none() {
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
    // TODO figure out how to make this less odious and difficult to manage.
    let (file_sender, file_recv) = unbounded_channel();
    let (file_ext_sender, file_ext_recv) = unbounded_channel();
    let (size_sender, size_recv) = unbounded_channel();
    let (size_filter_sender, size_filter_recv) = unbounded_channel();
    let (partial_hash_sender, partial_filter_recv) = unbounded_channel();
    let (full_hash_sender, full_hash_recv) = unbounded_channel();
    let (full_filter_sender, full_filter_recv) = unbounded_channel();
    let (collector_sender, collector_reciever) = unbounded_channel();
    let (display_sender, display_receiver) = unbounded_channel();
    let mut js = JoinSet::new();
    let path = args
        .directory_entry_point
        .clone()
        .unwrap_or(vec![PathBuf::from(".")]);
    {
        let needs_filter = args.excluded_extensions.is_some() || args.included_extensions.is_some();
        let excluded_paths = args.excluded_paths.iter().cloned().collect();
        js.spawn(async move {
            let mut file_actor = FileRecursionActor {
                queue: VecDeque::new(),
                sender: file_sender,
                excluded_paths
            };
            file_actor.queue.extend(path);
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
    {
        let display_sender = display_sender.clone();
        js.spawn(async move {
            let mut partial_hasher = PartialHasher {
                reciever: size_filter_recv,
                sender: partial_hash_sender,
                joinset: JoinSet::new(),
                read_size: 4096,
                display_channel: display_sender,
            };
            partial_hasher.operate().await;
        });
    }
    js.spawn(async move {
        let mut hash_filter = PartialHashFilter {
            receiver: partial_filter_recv,
            sender: full_hash_sender,
            seen: HashMap::default(),
        };
        hash_filter.operate().await;
    });
    {
        let display_sender = display_sender.clone();
        js.spawn(async move {
            let mut full_hasher = FullHasher {
                joinset: JoinSet::new(),
                receiver: full_hash_recv,
                sender: full_filter_sender,
                 display_sender,
            };
            full_hasher.operate().await;
        });
    }
    js.spawn(async move {
        let mut full_filter = FullHashFilter {
            receiver: full_filter_recv,
            sender: collector_sender,
            seen: HashMap::new(),
        };
        full_filter.operate().await;
    });
    let (duplicate_sender, duplicate_recv) = unbounded_channel();
    {
        let display_sender = display_sender.clone();
        js.spawn(async move {
            let mut tc = BytewiseFileComparator {
                map: BTreeMap::new(),
                receiver: collector_reciever,
                sender: duplicate_sender,
                display_sender,
            };
            tc.operate().await;
        });
    }
    let duplicate_recv = if args.hard_link {
        let display_sender = display_sender.clone();
        let json_dump = args.json_dump.is_some();
        let confirm = args.confirm_actions;
        let (forwarder_sender, forwarder_recv) = unbounded_channel();
        let fallback_to_symbolic = args.fallback_to_symbolic;
        js.spawn(async move {
            let mut tc = HardLinker {
                confirm_actions: confirm,
                receiver: duplicate_recv,
                forwarder: if json_dump {
                    Some(forwarder_sender)
                } else {
                    None
                },
                display: display_sender,
                fallback_to_symbolic
            };
            tc.operate().await;
        });
        forwarder_recv
    } else {
        duplicate_recv
    };
    if let Some(path) = args.json_dump.as_ref() {
        let path = path.clone();

        js.spawn(async move {
            let mut tc = JsonDumper {
                map: BTreeMap::new(),
                output: path,
                reciever: duplicate_recv,
            };
            tc.operate().await;
        });
    }
    js.spawn(async move {
        StatusDisplay {
            status: StatusData::default(),
            receiver: display_receiver,
        }
        .operate()
        .await;
    });

    js
}
enum StatusUpdate {
    PartialHash(usize),
    FullHash(usize),
    Duplicates(usize),
    HardLinks(usize),
    SymbolicLinks(usize),
}
#[derive(Default)]
struct StatusData {
    partial_hashes: usize,
    full_hashes: usize,
    duplicates: usize,
    hardlinks: usize,
    symbolic_links: usize,
}
impl StatusData {
    fn merge_status(&mut self, data: StatusUpdate) {
        match data {
            StatusUpdate::Duplicates(n) => self.duplicates += n,
            StatusUpdate::HardLinks(n) => self.hardlinks += n,
            StatusUpdate::PartialHash(n) => self.partial_hashes += n,
            StatusUpdate::FullHash(n) => self.full_hashes += n,
            StatusUpdate::SymbolicLinks(n) => self.symbolic_links += n,
        }
    }
}
struct StatusDisplay {
    receiver: ActorReceiver<StatusUpdate>,
    status: StatusData,
}
impl Actor for StatusDisplay {
    async fn operate(&mut self) {
        let term = console::Term::stdout();
        let mut buffer = Vec::with_capacity(100);
        let partial_style = console::style("Partial hashes").bold();
        let full_style = console::style("Full hashes").bold().green();
        let duplicates_style = console::style("Duplicate files").bold().red();
        let hardlinks_style = console::style("Hard link count").bold().white();
        let symlinks_style = console::style("Symbolic link count").bold().white();
        // If the terminal is dumb, then we should just not expect it to respond reasonably,
        // just ignore it and be silent.
        let is_dumb = std::env::var("TERM").is_ok_and(|x|x=="dumb");
        'outer: loop {
            let mut read_this_loop = 0;
            while !self.receiver.is_empty() {
                let read = self.receiver.recv_many(&mut buffer, 100).await;
                if read == 0 {
                    break 'outer;
                }

                buffer.drain(..).for_each(|data| {
                    self.status.merge_status(data);
                });
                read_this_loop += read;
                if read_this_loop > 1000 {
                    break;
                }
            }
            if !term.is_term() || is_dumb{
                continue;
            }
            if self.receiver.is_closed() {
                return;
            }
            term.move_cursor_to(0, 0).unwrap();
            term.clear_line().unwrap();
            println!(
                "{}\t{: >9}\t{}\t{}",
                partial_style, self.status.partial_hashes, full_style, self.status.full_hashes
            );
            term.clear_line().unwrap();
            println!(
                "{}\t{: >9}\t{}\t{: >9}",
                duplicates_style, self.status.duplicates, hardlinks_style, self.status.hardlinks
            );
            println!("{}\t{: >9}", symlinks_style, self.status.symbolic_links);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
