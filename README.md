Will, one day, provide a way to run an commands on duplicates of a file.

Current functionality:
* Invoke on a single directory
  1. Filters files based on size into buckets
  2. Filters those buckets into partial hashes(SHA256)
  3. Filters those partial hashed buckets into completely hashed buckets
  4. Prints those collisions into a big json file.

Future functionality:
* Invoke on a single directory
  1. As above
  2. Filter them into bytewise compared files
  3. Execute specified command on each group of duplicates

     Probably something like
     `rsdupes ~ -exec <COMMAND> %OLDEST %-OLDEST`
     1. Provide Oldest File

        `%OLDEST`
     2. Full file group modulo the oldest file

        Probably something like `%-OLDEST`
     3. Full file group

        Something like `%GROUP`
  4. Linking like jsdupes focuses on
     1. Hardlinking
     2. Symbolic links
     3. Reflinks
* Specify partial hash size
* Specify thread pool sizes
* Provide some way to finish filtering current tasks when there are dozens before spawning new ones.
  (Memory usage can get quite high at this point)
* Possibly provide a few different options for hash algorithms.

  Honestly SHA256 is probably fine, but BLAKE3 looks like it might be a better fit for this particular
  usecase
* Use memory mapped files somehow. It would reduce copying substantially which would likely reduce the
  overall system load, even if this program can saturate IO.
* Operate exclusively on files that match or do not match a given pattern

# Current JSON Structure

```json
{
  "<file_size>": {
    "<HASH>": [
      "PATH",...
    ],
    ...
  }
}
```

## Current Actor Model
```
┌────────────────────────────┐
│ Filesystem recursion actor │
└─────────────┬──────────────┘    ┌───────────────┐    
              ├───────────────────┤FileFilterActor│
              │ (File Paths)      └───────┬───────┘
              │                           │
    ┌─────────┴────────┐                  │
    │Inode Deduplicator├──────────────────┘
    └─────────┬────────┘
              │ (File Paths)
    ┌─────────┴────────────┐
    │ Size Duplicate buffer│
    └─────────┬────────────┘
              │ (File Paths)
     ┌────────┴───────┐
     │ Partial hasher │
     └────────┬───────┘
              │
              │  (Filesize,PartialHash,Path)
              │
┌─────────────┴───────────────┐
│Partial Hash duplicate buffer│
└─────────────┬───────────────┘
              │
        ┌─────┴─────┐
        │Full Hasher│
        └─────┬─────┘
              │ (Size, Hash, Path)
      ┌───────┴────────┐
      │Full Hash Filter│
      └───────┬────────┘
              │ (Size, Hash, Path)
       ┌──────┴──────┐
       │TestCollector│
       └────┬─┬──────┘
            │ │
            │ └────→ (collisions.json)
            ├─────────────────────┐
            │                     │
      ┌─────┴─────────────────┐   │
      │Byte-by-byte comparison│   │
      └─────┬─────────────────┘   │
            │                     │
            ├─────────────────────┘
            │
      ┌─────┴──────────┐
      │Command Executor│
      └────────────────┘
```