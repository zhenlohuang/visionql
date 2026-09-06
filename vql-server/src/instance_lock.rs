use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use fs2::FileExt;

#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    pub fn acquire(vql_home: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(vql_home)?;
        let path = vql_home.join("vqld.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock_exclusive().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot acquire vqld instance lock '{}': {error}",
                    path.display()
                ),
            )
        })?;
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_instance_can_lock_one_home() {
        let temp = tempfile::tempdir().unwrap();
        let _first = InstanceLock::acquire(temp.path()).unwrap();
        assert!(InstanceLock::acquire(temp.path()).is_err());
    }
}
