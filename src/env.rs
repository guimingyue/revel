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

use std::cell::{RefCell, RefMut};
use std::cmp::min;
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::rc::Rc;
use std::sync::Mutex;
use crate::Error::IOError;
use crate::filename::{current_file_name, descriptor_file_name, temp_file_name};
use crate::Result;
use crate::slice::Slice;

/// posix env implementation

pub fn new_writable_file(filename: &str) -> Result<Rc<RefCell<dyn WritableFile>>>{
    // todo!() O_CLOEXEC flag
    let opened_file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open(filename);

    match opened_file {
        Ok(file) => Ok(Rc::new(RefCell::new(PosixWritableFile::new(filename, file)))),
        Err(err) => Err(crate::Error::from(err))
    }

}

pub fn remove_file(fname: &str) -> Result<()> {
    // todo!()
    Ok(())
}

pub fn rename_file(from: &str, to: &str) -> Result<()> {
    // todo!()
    Ok(())
}

pub fn set_current_file(dbname: &str, descriptor_number: u64) -> Result<()> {
    let manifest = descriptor_file_name(dbname, descriptor_number);
    let mut contents = Slice::from_str(manifest.as_str());
    contents.remove_prefix(dbname.len() + 1);
    let tmp = temp_file_name(dbname, descriptor_number);
    let mut data = Vec::from(contents.data());
    data.push('/' as u8);
    match write_string_to_file_sync(&Slice::from_bytes(data.as_slice()), tmp.as_str(), true) {
        Ok(_) => rename_file(tmp.as_str(), current_file_name(dbname).as_str())?,
        Err(_) => remove_file(tmp.as_str())?
    };
    Ok(())
}

pub fn write_string_to_file_sync(data: &Slice, fname: &str, should_sync: bool) -> Result<()> {
    let file = new_writable_file(fname)?;
    match file.borrow_mut().append(data) {
        Ok(_) => {
            if should_sync {
                file.borrow().sync()?
            }
        },
        Err(_) => {
            remove_file(fname)?
        }
    }
    Ok(())
}

pub fn create_dir(dirname: &str) -> Result<()> {
    match std::fs::create_dir(dirname) {
        Ok(_) => Ok(()),
        Err(e) => Err(crate::Error::from(e))
    }
}

pub trait WritableFile {

    fn append(&mut self, data: &Slice) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn close(&self) -> Result<()>;

    fn sync(&self) -> Result<()>;

}

pub trait SequentialFile {

    fn read<'a>(&'a self, scratch: &'a mut [u8]) -> Result<Slice>;

    fn skip(&self, n: u64) -> Result<()>;
}

pub trait RandomAccessFile {

    fn read<'a>(&'a self, offset: u64, scratch: &'a mut [u8]) -> Result<Slice>;

}

pub trait FileLock {

}

const kWritableFileBufferSize: usize = 65536;

pub struct PosixWritableFile {

    // buf_[0, pos_ - 1] contains data to be written to fd_.
    buf: Vec<u8>,
    pos: usize,
    file: RefCell<File>,

    // True if the file's name starts with MANIFEST.
    is_manifest: bool,
    filename: String,
    // The directory of filename_.
    dirname: String
}

fn write_unbuffered(mut file: RefMut<File>, data: &[u8], size: usize) -> Result<()> {
    let result = file.write(&data[0..size]);
    match result {
        Ok(write_result) => Ok(()),
        Err(err) => Err(crate::Error::from(err))
    }
}

impl PosixWritableFile {

    pub fn new(filename: &str, file: File) -> Self {
        PosixWritableFile {
            pos: 0,
            buf: vec![0; kWritableFileBufferSize],
            file: RefCell::new(file),
            // todo!() filename start with MANIFEST
            filename: filename.to_string(),
            is_manifest: false,
            // todo!() parse dirname from filename
            dirname: "".to_string()
        }
    }

    fn flush_buffer(&mut self) -> Result<()>{
        let result = write_unbuffered(self.file.borrow_mut(), self.buf.as_slice(), self.pos);
        self.pos = 0;
        result
    }
}

impl WritableFile for PosixWritableFile {
    fn append(&mut self, data: &Slice) -> Result<()> {
        let write_data = data.data();
        let write_size = data.size();
        let mut write_offset = 0;
        let copy_size = std::cmp::min(write_size, kWritableFileBufferSize - self.pos);
        let size = self.buf.write(&write_data[..copy_size]).expect("");
        self.pos += size;
        write_offset += size;
        if write_size <= write_offset {
            return Ok(());
        }

        // Can't fit in buffer, so need to do at least one write.
        self.flush_buffer()?;

        if write_size - write_offset < kWritableFileBufferSize {
            self.buf.write(&write_data[size..]).expect("");
            return Ok(());
        }
        write_unbuffered(self.file.borrow_mut(), write_data, write_size - write_offset)
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_buffer()
    }

    fn close(&self) -> Result<()> {
        drop(self.file.borrow_mut());
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.borrow_mut().sync_all()?;
        Ok(())
    }
}

pub struct PosixSequentialFile {

    file: RefCell<File>,

    filename: String
}

impl SequentialFile for PosixSequentialFile {
    
    fn read<'a>(&'a self, scratch: &'a mut [u8]) -> Result<Slice> {
        match self.file.borrow_mut().read( scratch) {
            Ok(size) => {
                Ok(Slice::from_bytes(&scratch[0..size]))
            },
            Err(e) => Err(crate::Error::from(e))
        }
    }

    fn skip(&self, n: u64) -> Result<()> {
        self.file.borrow_mut().seek(SeekFrom::Start(n as u64))?;
        Ok(())
    }
}

pub struct PosixRandomAccessFile {
    has_permanent_file: bool,

    file: RefCell<File>,

    // todo!() Limiter

    filename: String

}

impl RandomAccessFile for PosixRandomAccessFile {

    fn read<'a>(&'a self, offset: u64, scratch: &'a mut [u8]) -> Result<Slice> {
        if !self.has_permanent_file {
            // todo!()
        }

        self.file.borrow().read_at(scratch, offset)?;

        Ok(Slice::from_bytes(scratch))
    }
}

pub struct MemoryWritableFile {
    memory: Vec<u8>
}

impl MemoryWritableFile {
    pub fn new(memory: Vec<u8>) -> Self {
        MemoryWritableFile {
            memory
        }
    }
}

impl WritableFile for MemoryWritableFile {
    fn append(&mut self, data: &Slice) -> crate::Result<()> {
        self.memory.write_all(data.data())?;
        Ok(())
    }

    fn flush(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn close(&self) -> crate::Result<()> {
        Ok(())
    }

    fn sync(&self) -> crate::Result<()> {
        Ok(())
    }
}

pub struct MemorySequentialFile {
    memory: Rc<Vec<u8>>,
    offset: RefCell<usize>
}

impl MemorySequentialFile {

    pub fn new(memory: Rc<Vec<u8>>) -> Self{
        MemorySequentialFile{
            memory,
            offset: RefCell::new(0)
        }
    }
}

impl SequentialFile for MemorySequentialFile {
    fn read<'a>(&'a self, mut scratch: &'a mut [u8]) -> Result<Slice<'a>> {
        let len;
        let memory_end;
        {
            let memory_offset = *self.offset.borrow();
            memory_end = min(memory_offset + scratch.len(), self.memory.len());
            len = memory_end - memory_offset;
            scratch[..len].copy_from_slice(&self.memory[memory_offset..memory_end]);
        }
        self.offset.replace(memory_end);
        Ok(Slice::from_bytes(&scratch[0..len]))
    }

    fn skip(&self, n: u64) -> Result<()> {
        let memory_offset = *self.offset.borrow() + n as usize;
        self.offset.replace(memory_offset);
        Ok(())
    }
}

pub struct Env {

    locks: LockTable

}

struct LockTable {

    locked_files: Mutex<BTreeSet<String>>

}

impl LockTable {

    fn new() -> Self {
        LockTable {
            locked_files: Mutex::new(BTreeSet::new())
        }
    }

    fn insert(&mut self, fname: &str) -> bool {
        let mut guard = self.locked_files.lock().unwrap();
        guard.insert(fname.to_string())
    }

    fn remove(&mut self, fname: &str) {
        let mut guard = self.locked_files.lock().unwrap();
        guard.remove(fname);
    }
}

impl Env {

    pub fn new() -> Self{
        Env {
            locks: LockTable::new()
        }
    }

    pub fn lock_file(&mut self, fname: &str) -> Result<()> {
        // todo!()
        Ok(())
    }

    /// Returns true iff the named file exists.
    pub fn file_exists(&self, fname: &str) -> bool {
        Path::new(fname).try_exists().is_ok()
    }
}