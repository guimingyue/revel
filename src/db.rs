// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use crate::options::{Options, ReadOptions, WriteOptions};
use crate::{log_writer, Result};
use crate::dbformat::{InternalKeyComparator, LookupKey};
use crate::env::{PosixWritableFile, WritableFile};
use crate::error::Error::NotFound;
use crate::memtable::MemTable;
use crate::slice::Slice;
use crate::util::crc::value;
use crate::version_set::VersionSet;
use crate::write_batch::{append, byte_size, insert_into, WriteBatch};

pub struct DB {
    logfile: Rc<RefCell<dyn WritableFile>>,
    // Queue of writers
    writers: Mutex<VecDeque<Writer>>,

    versions: VersionSet,

    temp_batch: RefCell<WriteBatch>,

    log: log_writer::Writer,

    mem: MemTable
}

impl DB {
    pub fn open(options: &Options, str: &str) -> Result<DB> {
        let path = <Path as AsRef<Path>>::as_ref(Path::new(str));
        let mut create = true;
        if path.exists() && File::open(path)?.metadata()?.len() > 0 {
            create = false;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(path)? ;
        let logfile = Rc::new(RefCell::new(PosixWritableFile::new(str, file)));
        let internalKeyComparator = InternalKeyComparator::new(options.comparator);
        let db = DB {
            logfile: logfile.clone(),
            writers: Mutex::new(VecDeque::new()),
            versions: VersionSet::new(str),
            temp_batch: RefCell::new(WriteBatch::new()),
            log: log_writer::Writer::new(logfile.clone()),
            mem: MemTable::new(internalKeyComparator)
        };
        Ok(db)
    }

    pub fn put(&mut self, opt: &WriteOptions, key: &Slice, value: &Slice) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.put(key, value);
        self.write(opt, write_batch)
    }

    pub fn delete(&mut self, opt: &WriteOptions, key: &Slice) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.delete(key);
        self.write(opt, write_batch)
    }
    
    pub fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Vec<u8>> {
        let snapshot;
        {
            let lock = self.writers.lock();
            snapshot = self.versions.last_sequence();
            drop(lock);
        }
        let lkey = LookupKey::new(key, snapshot);
        match self.mem.get(&lkey) {
            (true, Ok(value)) => Ok(value),
            _ => Err(NotFound)
        }
    }

    pub fn write(&mut self, opt: &WriteOptions, updates: WriteBatch) -> Result<()> {
        let mut last_sequence;
        {
            let mut writers = self.writers.lock().unwrap();
            writers.push_back(Writer::new(updates, opt.sync));
            last_sequence = self.versions.last_sequence();
            self.build_batch_group(writers);
            let mut write_batch = self.temp_batch.borrow_mut();
            write_batch.set_sequence(last_sequence + 1);
            last_sequence += write_batch.count() as u64;
        }
        {
            let write_batch = self.temp_batch.borrow();
            self.log.add_record(&write_batch.contents())?;
            if opt.sync {
                self.logfile.borrow().sync()?;
            }
            insert_into(&write_batch, &mut self.mem);
        }
        {
            // clean up
            self.temp_batch.borrow_mut().clear();
            self.versions.set_last_sequence(last_sequence);
        }
        Ok(())
    }

    fn build_batch_group(&self, writers: MutexGuard<VecDeque<Writer>>) {
        let front = writers.front();
        let first = front.expect("writers should not be empty");
        let mut size = byte_size(&first.batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much
        let mut max_size = 1 << 20;
        if size <= 128 << 10 {
            max_size = size + (128 << 10);
        }

        let mut result = self.temp_batch.borrow_mut();

        let mut iter = writers.iter();
        while let Some(w) = iter.next() {
            if !first.sync && w.sync {
                // Do not include a sync write into a batch handled by a non-sync write.
                break
            }

            size += byte_size(&w.batch);
            if size > max_size {
                // Do not make batch too big
                break;
            }
            result.append(&w.batch);
        }
    }
}

struct Writer {

    batch: WriteBatch,

    sync: bool,

    done: bool

    //cv: Condvar
}

impl Writer {

    fn new(batch: WriteBatch, sync: bool) -> Self {
        Writer{
            batch,
            sync,
            done: false
        }
    }

    fn wait(&self) {
        //self.cv.wait()
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use super::*;

    #[test]
    fn test() {
        let user_comparator: fn(a: &Slice, b: &Slice) -> Ordering = |a: &Slice, b: &Slice| {
            a.data().cmp(b.data())
        };
        let options = Options {
            comparator: user_comparator
        };
        let mut db = DB::open(&options, "./text").expect("error");
        db.put(&WriteOptions::default(), &Slice::from_str("key"), &Slice::from_str("value")).expect("put error");
        let value = db.get(&ReadOptions::default(), &Slice::from_str("key")).expect("read error");
        assert_eq!("value", String::from_utf8(value).unwrap());
    }
}