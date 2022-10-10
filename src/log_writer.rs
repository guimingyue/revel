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
use std::fs::File;
use std::io::Write;
use crate::slice::Slice;
use crate::Result;

pub trait WritableFile {

    fn append(&mut self, data: &Slice) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn close(&self) -> Result<()>;

    fn sync(&self) -> Result<()>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}