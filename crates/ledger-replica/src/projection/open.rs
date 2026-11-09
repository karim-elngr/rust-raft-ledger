use super::{FAMILIES, ReplicaError, invalid};
use ledger_core::LedgerId;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Options};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const INCOMING: &str = "checkpoint-install-v1";

pub(super) fn options(block_cache_bytes: usize) -> Options {
    let mut options = Options::default();
    options.set_atomic_flush(true);
    let mut table = BlockBasedOptions::default();
    if block_cache_bytes == 0 {
        table.disable_cache();
    } else {
        table.set_block_cache(&Cache::new_lru_cache(block_cache_bytes));
    }
    options.set_block_based_table_factory(&table);
    options
}

pub(super) fn families(opts: &Options) -> [ColumnFamilyDescriptor; 6] {
    FAMILIES.map(|name| ColumnFamilyDescriptor::new(name, opts.clone()))
}

pub(super) fn check_families(path: &Path, options: &Options) -> Result<(), ReplicaError> {
    let mut actual = DB::list_cf(options, path)?;
    let mut expected = FAMILIES;
    actual.sort_unstable();
    expected.sort_unstable();
    if actual.iter().map(String::as_str).ne(expected) {
        return Err(invalid("unsupported projection column families"));
    }
    Ok(())
}

fn decimal(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) && s.parse::<u64>().is_ok()
}

pub(super) fn immutable(name: &str) -> bool {
    name.rsplit_once('.')
        .is_some_and(|(n, extension)| decimal(n) && matches!(extension, "sst" | "blob" | "ldb"))
}

// Fixed projection layout: numeric RocksDB files plus its current metadata.
// Validate every entry before asking RocksDB to delete any recognized file.
fn db_filename(name: &str) -> bool {
    matches!(name, "IDENTITY" | "CURRENT" | "LOCK" | "LOG" | "LOG.old")
        || name.strip_prefix("LOG.old.").is_some_and(decimal)
        || name.strip_prefix("MANIFEST-").is_some_and(decimal)
        || name
            .strip_prefix("OPTIONS-")
            .is_some_and(|n| decimal(n.strip_suffix(".dbtmp").unwrap_or(n)))
        || immutable(name)
        || name
            .rsplit_once('.')
            .is_some_and(|(n, extension)| decimal(n) && matches!(extension, "log" | "dbtmp"))
}

pub(super) fn check_directory(path: &Path, private_metadata: bool) -> Result<(), ReplicaError> {
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(invalid("invalid projection data directory"));
    }
    for item in fs::read_dir(path)? {
        let item = item?;
        let name = item.file_name();
        let name = name
            .to_str()
            .filter(|s| db_filename(s))
            .ok_or_else(|| invalid("unrecognized file in projection database"))?;
        if !item.file_type()?.is_file() {
            return Err(invalid(
                "unexpected directory or symlink in projection data",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if private_metadata && !immutable(name) && item.metadata()?.nlink() > 1 {
                return Err(invalid(
                    "writable projection metadata must have private inodes",
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = (name, private_metadata);
    }
    Ok(())
}

pub(super) struct Working {
    pub data: PathBuf,
    pub incoming: PathBuf,
}

impl Working {
    // The caller holds the Raft store lock throughout installation or reset.
    pub fn prepare(path: &Path, id: LedgerId) -> Result<Self, ReplicaError> {
        match fs::symlink_metadata(path) {
            Ok(meta) if !meta.is_dir() => return Err(invalid("invalid projection directory")),
            Ok(_) => (),
            Err(err) if err.kind() == ErrorKind::NotFound => fs::create_dir(path)?,
            Err(err) => return Err(err.into()),
        }
        let marker = format!(".ledger-projection-v1-{:032x}", id.0);
        let mut owned = false;
        let mut has_contents = false;
        for item in fs::read_dir(path)? {
            let item = item?;
            let name = item.file_name();
            if name == OsStr::new(&marker)
                && item.file_type()?.is_file()
                && item.metadata()?.len() == 0
            {
                owned = true;
            } else if name == "data" || name == INCOMING {
                has_contents = true;
                check_directory(&item.path(), false)?;
            } else {
                return Err(invalid("unknown contents in projection directory"));
            }
        }
        if !owned {
            if has_contents {
                return Err(invalid("missing projection ownership marker"));
            }
            File::create_new(path.join(marker))?.sync_all()?;
            File::open(path)?.sync_all()?;
        }
        let layout = Self {
            data: path.join("data"),
            incoming: path.join(INCOMING),
        };
        // An interrupted install can leave a partial or complete copy here.
        // It has never been opened writable; unlinking a staged hard link leaves
        // the source inode untouched. The layout was checked above.
        if layout.incoming.try_exists()? {
            for item in fs::read_dir(&layout.incoming)? {
                fs::remove_file(item?.path())?;
            }
            fs::remove_dir(&layout.incoming)?;
        }
        Ok(layout)
    }

    pub fn discard_data(&self) -> Result<(), ReplicaError> {
        if self.data.try_exists()? {
            DB::destroy(&Options::default(), &self.data)?;
            if self.data.try_exists()? {
                fs::remove_dir(&self.data)?;
            }
        }
        Ok(())
    }
}

pub(super) fn open(
    path: &Path,
    id: LedgerId,
    block_cache_bytes: usize,
) -> Result<DB, ReplicaError> {
    let layout = Working::prepare(path, id)?;
    layout.discard_data()?;
    let mut options = options(block_cache_bytes);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_error_if_exists(true);
    Ok(DB::open_cf_descriptors(
        &options,
        &layout.data,
        families(&options),
    )?)
}
