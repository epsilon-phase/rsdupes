use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use tokio::fs::{metadata, read_dir};
use crate::actor_types::{ActorReceiver, ActorSender};
use crate::actors::{Actor};

pub struct FileRecursionActor {
    pub queue: VecDeque<PathBuf>,
    pub excluded_paths:Vec<PathBuf>,
    pub sender: ActorSender<PathBuf>,
}

impl Actor for FileRecursionActor {
    async fn operate(&mut self) {
        while let Some(x) = self.queue.pop_front() {
            if let Ok(mut dir) = read_dir(x).await {
                while let Ok(Some(entry)) = dir.next_entry().await {
                    if self.excluded_paths.iter().any(|excluding|entry.path().starts_with(excluding)) {
                        continue;
                    }
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

pub struct INodeFilter {
    pub seen: HashSet<(u64, u64)>,
    pub reciever: ActorReceiver<PathBuf>,
    pub sender: ActorSender<(PathBuf, Metadata)>,
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

pub struct FileSizeFilter {
    pub seen: HashMap<u64, Option<PathBuf>>,
    pub receiver: ActorReceiver<(PathBuf, Metadata)>,
    pub sender: ActorSender<FileSizeMessage>,
    pub minimum_size: u64,
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

pub struct FileExclusionFilter {
    pub reciever: ActorReceiver<PathBuf>,
    pub sender: ActorSender<PathBuf>,
    pub excluded_extensions: Option<Vec<String>>,
    pub included_extensions: Option<Vec<String>>,
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

pub struct FileSizeMessage {
    pub path: PathBuf,
    pub size: u64,
}