//! Windows implementation of the private, generation-bound snapshot store.
use super::windows_private::{VerifiedDirectory, protect, unprotect};
use super::{Binding, Envelope, Marker, Value};
use hmac::{Hmac, Mac};
use serde::{Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::{
    fs::File,
    io::{self, Read, Write},
    os::windows::ffi::OsStrExt,
    path::Path,
};

const MAX_BYTES: u64 = 8 * 1024 * 1024;

pub struct Store {
    directory: VerifiedDirectory,
    anchors: VerifiedDirectory,
    key: [u8; 32],
}
pub struct Generation<'a> {
    store: &'a Store,
    lock: File,
    name: String,
    marker: String,
    generation: uuid::Uuid,
    binding: Binding,
}

fn digest<T: Serialize>(key: &[u8; 32], domain: &str, value: &T) -> io::Result<[u8; 32]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32 bytes");
    mac.update(domain.as_bytes());
    mac.update(&[0]);
    mac.update(&serde_json::to_vec(value)?);
    Ok(mac.finalize().into_bytes().into())
}
fn name_for(key: &[u8; 32], path: &Path, id: uuid::Uuid, suffix: &str) -> io::Result<String> {
    let path: Vec<u16> = path.as_os_str().encode_wide().collect();
    let digest = digest(key, "share-session-windows-v1", &(path, id))?;
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("{hex}.{suffix}"))
}
fn directory(path: &Path, create: bool) -> io::Result<Option<VerifiedDirectory>> {
    if create {
        match VerifiedDirectory::create(path) {
            Ok(dir) => return Ok(Some(dir)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
    }
    match VerifiedDirectory::open(path) {
        Ok(dir) => Ok(Some(dir)),
        Err(e) if !create && e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn file(dir: &VerifiedDirectory, name: &str, create: bool) -> io::Result<File> {
    if create {
        match dir.create_private_file(name) {
            Ok(file) => return Ok(file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
    }
    dir.open_private_file(name)
}
fn write(dir: &VerifiedDirectory, name: &str, bytes: &[u8]) -> io::Result<()> {
    let temp = format!("tmp-{}", uuid::Uuid::new_v4());
    let result = (|| {
        let mut f = dir.create_private_file(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        dir.replace_private_file(&temp, name)
    })();
    let _ = dir.remove_private_file(&temp);
    result
}
fn read(dir: &VerifiedDirectory, name: &str, max: u64) -> io::Result<Option<Vec<u8>>> {
    let f = match dir.open_private_file(name) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if f.metadata()?.len() > max {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    f.take(max + 1).read_to_end(&mut bytes)?;
    Ok((bytes.len() as u64 <= max).then_some(bytes))
}
fn key(dir: &VerifiedDirectory, create: bool) -> io::Result<Option<[u8; 32]>> {
    let _lock = if create {
        let f = file(dir, "key-v1.lock", true)?;
        f.lock()?;
        Some(f)
    } else {
        None
    };
    match read(dir, "key-v1", 65536)? {
        Some(bytes) => {
            let bytes = unprotect(&bytes)?;
            let key = bytes.try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid share cache key")
            })?;
            Ok(Some(key))
        }
        None if create => {
            let mut key = [0; 32];
            key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
            key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
            write(dir, "key-v1", &protect(&key)?)?;
            Ok(Some(key))
        }
        None => Ok(None),
    }
}
fn write_marker(
    dir: &VerifiedDirectory,
    key: &[u8; 32],
    name: &str,
    generation: uuid::Uuid,
    clean: bool,
) -> io::Result<()> {
    let marker = Marker {
        version: 1,
        generation,
        clean,
        tag: digest(
            key,
            "share-marker-windows-v1",
            &(name, 1u32, generation, clean),
        )?,
    };
    write(dir, name, &protect(&serde_json::to_vec(&marker)?)?)
}
fn read_marker(dir: &VerifiedDirectory, key: &[u8; 32], name: &str) -> io::Result<Option<Marker>> {
    let Some(bytes) = read(dir, name, 65536)? else {
        return Ok(None);
    };
    let Ok(bytes) = unprotect(&bytes) else {
        return Ok(None);
    };
    let Ok(marker) = serde_json::from_slice::<Marker>(&bytes) else {
        return Ok(None);
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32 bytes");
    mac.update(b"share-marker-windows-v1\0");
    mac.update(&serde_json::to_vec(&(
        name,
        marker.version,
        marker.generation,
        marker.clean,
    ))?);
    if marker.version != 1 || !marker.clean || mac.verify_slice(&marker.tag).is_err() {
        return Ok(None);
    }
    Ok(Some(marker))
}
impl Store {
    pub fn open(parent: &Path) -> io::Result<Self> {
        Self::open_inner(parent, true)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "share cache missing"))
    }
    pub fn open_existing(parent: &Path) -> io::Result<Option<Self>> {
        Self::open_inner(parent, false)
    }
    fn open_inner(parent: &Path, create: bool) -> io::Result<Option<Self>> {
        let Some(directory) = directory(&parent.join("share-context-v1"), create)? else {
            return Ok(None);
        };
        let Some(anchors) = directory_fn(&parent.join("share-context-anchors-v1"), create)? else {
            return Ok(None);
        };
        let Some(key) = key(&directory, create)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            directory,
            anchors,
            key,
        }))
    }
    pub fn fingerprint<T: Serialize>(&self, domain: &str, value: &T) -> io::Result<[u8; 32]> {
        digest(&self.key, domain, value)
    }
    pub fn binding_for_session<T: Serialize>(
        &self,
        session: &pi_rs::Session,
        provenance: &T,
    ) -> io::Result<Binding> {
        Ok(Binding {
            session_id: session.session_id(),
            head: session.head(),
            journal_digest: self.fingerprint("share-journal-v1", &session.records())?,
            provenance_digest: self.fingerprint("share-provenance-v1", provenance)?,
        })
    }
    pub fn read_session<P: Serialize, C: DeserializeOwned>(
        &self,
        path: &Path,
        session: &pi_rs::Session,
        provenance: &P,
    ) -> io::Result<Option<C>> {
        Ok(self
            .read(path, &self.binding_for_session(session, provenance)?)?
            .and_then(|v| serde_json::from_value(v).ok()))
    }
    pub fn begin(&self, path: &Path, binding: Binding) -> io::Result<Generation<'_>> {
        let lock = file(
            &self.directory,
            &name_for(&self.key, path, binding.session_id, "lock")?,
            true,
        )?;
        lock.lock()?;
        let name = name_for(&self.key, path, binding.session_id, "json")?;
        let marker = name_for(&self.key, path, binding.session_id, "generation")?;
        let anchor = name_for(&self.key, path, binding.session_id, "anchor")?;
        let generation = uuid::Uuid::new_v4();
        write_marker(&self.anchors, &self.key, &anchor, generation, true)?;
        write_marker(&self.directory, &self.key, &marker, generation, false)?;
        self.directory.remove_private_file(&name)?;
        Ok(Generation {
            store: self,
            lock,
            name,
            marker,
            generation,
            binding,
        })
    }
    pub async fn begin_and_initialize<F, Fut, T>(
        &self,
        path: &Path,
        binding: Binding,
        initialize: F,
    ) -> pi_rs::Result<(Generation<'_>, T)>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = pi_rs::Result<T>>,
    {
        let generation = self.begin(path, binding)?;
        let runtime = initialize().await?;
        Ok((generation, runtime))
    }
    pub fn read(&self, path: &Path, binding: &Binding) -> io::Result<Option<Value>> {
        let lock = match file(
            &self.directory,
            &name_for(&self.key, path, binding.session_id, "lock")?,
            false,
        ) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        match lock.try_lock() {
            Ok(()) => (),
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        let Some(anchor) = read_marker(
            &self.anchors,
            &self.key,
            &name_for(&self.key, path, binding.session_id, "anchor")?,
        )?
        else {
            return Ok(None);
        };
        let Some(marker) = read_marker(
            &self.directory,
            &self.key,
            &name_for(&self.key, path, binding.session_id, "generation")?,
        )?
        else {
            return Ok(None);
        };
        if anchor.generation != marker.generation {
            return Ok(None);
        }
        let name = name_for(&self.key, path, binding.session_id, "json")?;
        let Some(bytes) = read(&self.directory, &name, MAX_BYTES * 2)? else {
            return Ok(None);
        };
        let Ok(bytes) = unprotect(&bytes) else {
            return Ok(None);
        };
        if bytes.len() as u64 > MAX_BYTES {
            return Ok(None);
        }
        let Ok(envelope) = serde_json::from_slice::<Envelope>(&bytes) else {
            return Ok(None);
        };
        if envelope.version != 2
            || envelope.generation != marker.generation
            || &envelope.binding != binding
        {
            return Ok(None);
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts 32 bytes");
        mac.update(b"share-envelope-windows-v1\0");
        mac.update(&serde_json::to_vec(&(
            &name,
            envelope.version,
            envelope.generation,
            &envelope.binding,
            &envelope.context,
        ))?);
        if mac.verify_slice(&envelope.tag).is_err() {
            return Ok(None);
        }
        Ok(Some(envelope.context))
    }
}
// Avoid shadowing the directory constructor in open_inner.
fn directory_fn(path: &Path, create: bool) -> io::Result<Option<VerifiedDirectory>> {
    directory(path, create)
}
impl Generation<'_> {
    pub fn publish_session<P: Serialize, C: Serialize>(
        self,
        session: &pi_rs::Session,
        provenance: &P,
        context: &C,
    ) -> io::Result<()> {
        let binding = self.store.binding_for_session(session, provenance)?;
        self.publish(binding, serde_json::to_value(context)?)
    }
    pub fn publish(self, binding: Binding, context: Value) -> io::Result<()> {
        if binding.session_id != self.binding.session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session identity changed",
            ));
        }
        let tag = digest(
            &self.store.key,
            "share-envelope-windows-v1",
            &(&self.name, 2u32, self.generation, &binding, &context),
        )?;
        let envelope = Envelope {
            version: 2,
            generation: self.generation,
            binding,
            context,
            tag,
        };
        let bytes = serde_json::to_vec(&envelope)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "share cache too large",
            ));
        }
        write(&self.store.directory, &self.name, &protect(&bytes)?)?;
        write_marker(
            &self.store.directory,
            &self.store.key,
            &self.marker,
            self.generation,
            true,
        )?;
        let _ = &self.lock;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshot_requires_clean_matching_generation_and_binding() {
        let root = std::env::temp_dir().join(format!("pi-share-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        assert!(Store::open_existing(&root).unwrap().is_none());
        let store = Store::open(&root).unwrap();
        let path = root.join("session.jsonl");
        let binding = Binding {
            session_id: uuid::Uuid::new_v4(),
            head: 7,
            journal_digest: [1; 32],
            provenance_digest: [2; 32],
        };
        let generation = store.begin(&path, binding.clone()).unwrap();
        assert_eq!(store.read(&path, &binding).unwrap(), None);
        generation
            .publish(binding.clone(), json!({"prompt":"private"}))
            .unwrap();
        assert_eq!(
            store.read(&path, &binding).unwrap(),
            Some(json!({"prompt":"private"}))
        );
        let mut changed = binding.clone();
        changed.provenance_digest[0] ^= 1;
        assert_eq!(store.read(&path, &changed).unwrap(), None);
        let generation = store.begin(&path, binding.clone()).unwrap();
        drop(generation);
        assert_eq!(store.read(&path, &binding).unwrap(), None);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restoring_old_snapshot_and_marker_does_not_override_new_anchor() {
        let root = std::env::temp_dir().join(format!("pi-share-replay-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let store = Store::open(&root).unwrap();
        let path = root.join("session.jsonl");
        let binding = Binding {
            session_id: uuid::Uuid::new_v4(),
            head: 1,
            journal_digest: [3; 32],
            provenance_digest: [4; 32],
        };
        store
            .begin(&path, binding.clone())
            .unwrap()
            .publish(binding.clone(), json!({"prompt":"old"}))
            .unwrap();
        let name = name_for(&store.key, &path, binding.session_id, "json").unwrap();
        let marker = name_for(&store.key, &path, binding.session_id, "generation").unwrap();
        let old_data = read(&store.directory, &name, MAX_BYTES * 2)
            .unwrap()
            .unwrap();
        let old_marker = read(&store.directory, &marker, 65536).unwrap().unwrap();
        drop(store.begin(&path, binding.clone()).unwrap());
        write(&store.directory, &name, &old_data).unwrap();
        write(&store.directory, &marker, &old_marker).unwrap();
        assert_eq!(store.read(&path, &binding).unwrap(), None);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
