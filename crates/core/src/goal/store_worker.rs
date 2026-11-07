//! Worker-owned database handles and shared observation for each storage root.

use super::{open_database, sql_error, validate_database, SCHEMA_VERSION, WORK_QUEUE_CAPACITY};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot, watch};

// Observed roots retain one handle each. Unobserved roots share a bounded idle
// cache, so visiting additional homes cannot retain every database forever.
const MAX_IDLE_CONNECTIONS: usize = 8;
type Job = Box<dyn FnOnce(&mut Worker) + Send + 'static>;

#[derive(Debug, PartialEq, Eq)]
struct FileStamp {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    created: Option<SystemTime>,
    modified: Option<SystemTime>,
    length: u64,
}
impl FileStamp {
    fn read(root: &Path) -> io::Result<Self> {
        let metadata = std::fs::metadata(root.join("goals.sqlite3"))?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "goal database is not a file",
            ));
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            created: metadata.created().ok(),
            modified: metadata.modified().ok(),
            length: metadata.len(),
        })
    }
    fn same_file(&self, other: &Self) -> bool {
        #[cfg(unix)]
        if self.device != other.device || self.inode != other.inode {
            return false;
        }
        self.created == other.created
    }
}
struct Database {
    connection: Connection,
    stamp: FileStamp,
    data_version: i64,
    revision: u64,
    used: u64,
}
struct Observation {
    canonical_root: PathBuf,
    sender: watch::Sender<u64>,
    seen: Option<u64>,
    error: Option<String>,
}
#[derive(Default)]
struct Worker {
    databases: HashMap<PathBuf, Database>,
    observations: HashMap<PathBuf, Observation>,
    sequence: u64,
}
impl Worker {
    fn next_revision(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }
    fn connection(&mut self, root: &Path) -> io::Result<PathBuf> {
        std::fs::create_dir_all(root)?;
        let canonical = std::fs::canonicalize(root)?;
        let mut reopen = false;
        let mut failure = None;
        if let Some(database) = self.databases.get_mut(&canonical) {
            match FileStamp::read(&canonical) {
                Ok(stamp) => {
                    // Identity comes first: a broken old handle must never
                    // prevent an atomic on-disk replacement from recovering.
                    if !database.stamp.same_file(&stamp) {
                        reopen = true;
                    } else {
                        let check = (|| -> io::Result<(bool, i64)> {
                            let version: i64 = database
                                .connection
                                .pragma_query_value(None, "user_version", |row| row.get(0))
                                .map_err(sql_error)?;
                            let data_version: i64 = database
                                .connection
                                .pragma_query_value(None, "data_version", |row| row.get(0))
                                .map_err(sql_error)?;
                            // A direct non-SQLite rewrite requires a new handle;
                            // its old page cache must never mask corruption.
                            let reopen = version != SCHEMA_VERSION
                                || (stamp != database.stamp
                                    && data_version == database.data_version);
                            if !reopen && data_version != database.data_version {
                                validate_database(&database.connection, version)?;
                            }
                            Ok((reopen, data_version))
                        })();
                        match check {
                            Ok((must_reopen, data_version)) => {
                                reopen = must_reopen;
                                if !reopen && data_version != database.data_version {
                                    database.data_version = data_version;
                                    database.stamp = stamp;
                                    database.revision = self.sequence.wrapping_add(1);
                                    self.sequence = database.revision;
                                }
                            }
                            Err(error) => {
                                // Evict failed handles so later repair can open
                                // cleanly instead of retrying their stale cache.
                                reopen = true;
                                failure = Some(error);
                            }
                        }
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => reopen = true,
                Err(error) => return Err(error),
            }
        }
        if reopen {
            self.databases.remove(&canonical);
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if !self.databases.contains_key(&canonical) {
            let connection = open_database(&canonical)?;
            let stamp = FileStamp::read(&canonical)?;
            let data_version = connection
                .pragma_query_value(None, "data_version", |row| row.get(0))
                .map_err(sql_error)?;
            let revision = self.next_revision();
            self.databases.insert(
                canonical.clone(),
                Database {
                    connection,
                    stamp,
                    data_version,
                    revision,
                    used: revision,
                },
            );
        }
        let used = self.next_revision();
        self.databases
            .get_mut(&canonical)
            .expect("opened goal database remains cached")
            .used = used;
        Ok(canonical)
    }
    fn execute<T>(
        &mut self,
        root: &Path,
        operation: impl FnOnce(&mut Connection, &Path) -> io::Result<T>,
    ) -> io::Result<T> {
        let canonical = self.connection(root)?;
        let database = self
            .databases
            .get_mut(&canonical)
            .expect("goal database was opened");
        let before = database.connection.total_changes();
        let result = operation(&mut database.connection, &canonical);
        // total_changes can advance after a rollback. A redundant invalidation
        // is harmless; ignoring a committed mutation after a reply error is not.
        if database.connection.total_changes() != before {
            database.revision = self.sequence.wrapping_add(1);
            self.sequence = database.revision;
        }
        if let Ok(stamp) = FileStamp::read(&canonical) {
            database.stamp = stamp;
        }
        self.publish_cached(&canonical);
        self.prune();
        result
    }
    fn publish_cached(&mut self, canonical: &Path) {
        let Some(database) = self.databases.get(canonical) else {
            return;
        };
        for observation in self
            .observations
            .values_mut()
            .filter(|entry| entry.canonical_root == canonical)
        {
            if observation.seen != Some(database.revision) || observation.error.take().is_some() {
                observation.seen = Some(database.revision);
                observation
                    .sender
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
            }
        }
    }
    fn subscribe(&mut self, root: PathBuf) -> io::Result<watch::Receiver<u64>> {
        let canonical = self.connection(&root)?;
        let seen = self
            .databases
            .get(&canonical)
            .map(|database| database.revision);
        let observation = self.observations.entry(root).or_insert_with(|| {
            let (sender, _) = watch::channel(0);
            Observation {
                canonical_root: canonical.clone(),
                sender,
                seen,
                error: None,
            }
        });
        if observation.canonical_root != canonical {
            observation.canonical_root = canonical;
            observation.seen = seen;
        }
        Ok(observation.sender.subscribe())
    }
    fn poll(&mut self) {
        self.observations
            .retain(|_, entry| entry.sender.receiver_count() != 0);
        let roots: Vec<_> = self.observations.keys().cloned().collect();
        for root in roots {
            // Observation must not recreate a removed home behind its owner's
            // back. A later explicit storage operation can initialize it again.
            let result = std::fs::metadata(&root).and_then(|_| self.connection(&root));
            match result {
                Ok(canonical) => {
                    if let Some(observation) = self.observations.get_mut(&root) {
                        if observation.canonical_root != canonical {
                            observation.canonical_root = canonical.clone();
                            observation.seen = None;
                        }
                    }
                    self.publish_cached(&canonical);
                }
                Err(error) => {
                    if let Some(observation) = self.observations.get_mut(&root) {
                        let message = error.to_string();
                        if observation.error.as_ref() != Some(&message) {
                            observation.error = Some(message);
                            observation
                                .sender
                                .send_modify(|revision| *revision = revision.wrapping_add(1));
                        }
                    }
                }
            }
        }
        self.prune();
    }
    fn prune(&mut self) {
        self.observations
            .retain(|_, entry| entry.sender.receiver_count() != 0);
        let active: HashSet<_> = self
            .observations
            .values()
            .map(|entry| entry.canonical_root.clone())
            .collect();
        let mut idle: Vec<_> = self
            .databases
            .iter()
            .filter(|(root, _)| !active.contains(*root))
            .map(|(root, database)| (database.used, root.clone()))
            .collect();
        idle.sort_by_key(|(used, _)| *used);
        let excess = idle.len().saturating_sub(MAX_IDLE_CONNECTIONS);
        for (_, root) in idle.into_iter().take(excess) {
            self.databases.remove(&root);
        }
    }
}
fn sender() -> io::Result<&'static mpsc::Sender<Job>> {
    static WORKER: OnceLock<Result<mpsc::Sender<Job>, String>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().map_err(|error| error.to_string())?;
        let (sender, mut receiver) = mpsc::channel::<Job>(WORK_QUEUE_CAPACITY);
        std::thread::Builder::new().name("goal-storage".into()).spawn(move || {
            runtime.block_on(async move {
                let mut worker = Worker::default();
                let mut interval = tokio::time::interval(Duration::from_millis(250));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        job = receiver.recv() => match job { Some(job) => job(&mut worker), None => break },
                        _ = interval.tick() => worker.poll(),
                    }
                }
            });
        }).map(|_| sender).map_err(|error| error.to_string())
    }).as_ref().map_err(|message| io::Error::other(format!("cannot start goal storage worker: {message}")))
}
pub(super) async fn run<T, F>(root: PathBuf, operation: F) -> io::Result<T>
where
    F: FnOnce(&mut Connection, &Path) -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let (send, receive) = oneshot::channel();
    sender()?
        .send(Box::new(move |worker| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker.execute(&root, operation)
            }))
            .unwrap_or_else(|_| Err(io::Error::other("goal storage operation panicked")))
            .map_err(|error| {
                let message = error.to_string();
                let message = if message.starts_with("GOAL_CONFLICT: ") {
                    format!("{message} (goal storage {})", root.display())
                } else {
                    format!("goal storage {}: {message}", root.display())
                };
                io::Error::new(error.kind(), message)
            });
            let _ = send.send(result);
        }))
        .await
        .map_err(|_| io::Error::other("goal storage worker stopped"))?;
    receive
        .await
        .map_err(|_| io::Error::other("goal storage worker stopped before replying"))?
}
pub(super) async fn subscribe(root: PathBuf) -> io::Result<watch::Receiver<u64>> {
    let (send, receive) = oneshot::channel();
    sender()?
        .send(Box::new(move |worker| {
            let _ = send.send(worker.subscribe(root));
        }))
        .await
        .map_err(|_| io::Error::other("goal storage worker stopped"))?;
    receive
        .await
        .map_err(|_| io::Error::other("goal storage worker stopped before replying"))?
}
