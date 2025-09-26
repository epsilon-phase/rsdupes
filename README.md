Fast duplicate detection and remediation

# Usage

```sh
rsdupes -i <extension-to-include> -e <extension-to-exclude> -m <minimum-size> <Directory>
```

## Detecting Duplicates Without Action

To write a listing of duplicates, this is useful for figuring out what might be duplicated
without doing anything risky.

```sh
rsdupes -j duplicates.json ~
```

## Playing with Water on Steam

I have a lot of mods on some games, and did you know that some of them use duplicate assets?
For example, for Rimworld, this should help slightly.

```sh
rsdupes -i jpg -i png -i xml -i dds -h ~/.local/share/Steam
```

## Playing with Fire on Steam
For example, steam tends to install a lot of copies of mono for various games, this can consume **several** megabytes,
and can be hard-linked for minor reduction of disk usage.

This may have deleterious effects should the game's copy of Unity move to a newer version. This will also deduplicate
the various Visual C++ redistributables, which might be... less hazardous.

```sh
# Don't run this probably
rsdupes -i exe -i dll -i xml -h ~/.local/share/Steam
```

## Media File deduplication

You may have a lot of media stored, I sure do. This might be helpful, and it should be safe.

```sh
rsdupes -i png -i jpg -i gif -i webp -i webm -i mp4 -i mkv -h <MEDIA-PATH>
```

# Current Functionality

Current functionality:
* Invoke on any number of directories
  1. Filters files based on size into buckets
  2. Filters those buckets into partial hashes(SHA256)
  3. Filters those partial hashed buckets into completely hashed buckets
  4. Filters those fully hashed files and compares them byte by byte, merging duplicates.
     into buckets. This is also where permissions are compared. Currently, permissions
     checks are only effective on unixes, but likely incomplete there.
  5. (Optionally) links duplicates together
  5. (Optionally) writes a json file

# Current JSON Structure

```json
{
  "<file_size>": {
    "<HASH>": [
      "PATH":[
        "DUPLICATE",
        "DUPLICATE2"
      ],...
    ],
    ...
  }
}
```

## Current Actor Model
```
┌────────────────────────────┐
│ Filesystem recursion actor │
└─────────────┬──────────────┘    ┌───────────────────┐
              ├───────────────────┤FileExtensionFilter│
              │ (File Paths)      └───────┬───────────┘
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
     │ Partial hasher ├───────────────────────┐
     └────────┬───────┘                       │
              │                      ┌────────┴─────────┐
              │  (Filesize,          │StatusDisplayActor│
              │   PartialHash,       └┬──┬─┬─┬──────────┘
              │   Path)               ↑  ↑ │ │
┌─────────────┴───────────────┐       │  │ │ │
│Partial Hash duplicate buffer│       │  │ │ │
└─────────────┬───────────────┘       │  │ │ │
              │                       │  │ │ │
        ┌─────┴─────┐                 │  │ │ │
        │Full Hasher├─────────────────┘  │ │ │
        └─────┬─────┘                    │ │ │
              │ (Size, Hash, Path)       │ │ │
      ┌───────┴────────┐                 │ │ │
      │Full Hash Filter├─────────────────┘ │ │
      └───────┬────────┘                   │ │
              │ (Size, Hash, Path)         │ │
              ├                            │ │
              │                            │ │
        ┌─────┴─────────────────┐          │ │
        │Byte-by-byte comparison├──────────┘ │
        └───┬───────────────────┘            │
            │                                │
            ├────────┐                       │
            │        │                       │
      ┌─────┴────┐   │                       │
      │HardLinker├───│───────────────────────┘
      └────┬─────┘   │
           ├─────────┘
      ┌────┴─────┐
      │JsonDumper│
      └──────────┘
```
