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

use std::fs::{File, OpenOptions};
use std::io::Error;
use crate::log_writer::{PosixWritableFile, WritableFile};
use crate::Result;

/// posix env implementation

pub fn new_writable_file(filename: &str) -> Result<Box<dyn WritableFile>>{
    // todo!() O_CLOEXEC flag
    let opened_file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open(filename);

    match opened_file {
        Ok(file) => Ok(Box::new(PosixWritableFile::new(filename, file))),
        Err(err) => Err(crate::Error::from(err))
    }

}
