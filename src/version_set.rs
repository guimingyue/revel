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

use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::ptr::NonNull;
use std::rc::Rc;
use crate::env::{new_sequential_file, read_file_to_bytes};
use crate::error::Error;
use crate::filename::current_file_name;
use crate::log_reader::Reader;
use crate::options::Options;
use crate::version_edit::{FileMetaData, VersionEdit};
use crate::write_batch::append;

#[derive(Default)]
pub struct VersionSet {

    dbname: String,

    last_sequence: u64,

    next_file_number: u64,

    current: Option<NonNull<Version>>,

    dummy_versions: Option<NonNull<Version>>
}

impl VersionSet {

    pub fn new(db_name: &str) -> Self {
        VersionSet {
            dbname: db_name.to_string(),
            .. Default::default()
        }
    }

    pub fn init(&mut self) {

    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, s: u64) {
        assert!(s >= self.last_sequence);
        self.last_sequence = s;
    }

    pub fn mark_file_number_used(&mut self, number: u64) {
        if self.next_file_number < number {
            self.next_file_number = number + 1;
        }
    }

    pub fn append_version(&mut self, version: Version) {
        if let Some(mut ver) = &self.current {
            unsafe { (*ver.as_ptr()).unref() };
        }
        let mut v = Box::new(version);
        v.next = self.dummy_versions;
        v.prev = None;
        let ptr = Some(unsafe {NonNull::new_unchecked(Box::into_raw(v))});
        if let Some(head_ptr) = self.dummy_versions {
             unsafe { (*head_ptr.as_ptr()).prev = ptr};
        }
        self.dummy_versions = ptr;
        self.current = ptr;
    }

    pub fn prepare(&mut self, save_manifest: &mut bool) -> crate::Result<()> {
        let mut current = match read_file_to_bytes(current_file_name(self.dbname.as_str()).as_str()) {
            Ok(mut bytes) => {
                if bytes.is_empty() || bytes[bytes.len() - 1] != '\n' as u8 {
                    return Err(Error::Corruption);
                }
                bytes.resize(bytes.len()-1, 0);
                unsafe {String::from_utf8_unchecked(bytes)}
            },
            Err(error) => {
                return Err(error)
            }
        };

        let dscname = format!("{}/{}", self.dbname, current);

        let mut have_log_number = false;
        let mut have_prev_log_number = false;
        let mut have_next_file = false;
        let mut have_last_sequence = false;
        let mut next_file: u64 = 0;
        let mut last_sequence: u64 = 0;
        let mut log_number: u64 = 0;
        let mut prev_log_number: u64 = 0;

        let mut result = match new_sequential_file(dscname.as_str()) {
            Err(error) => {
                if error == Error::NotFound {
                    return Err(Error::Corruption);
                }
                Err(error)
            },
            Ok(file) => {
                let mut reader = Reader::new(file, true, 0);
                let mut scratch = vec![];
                let mut read_records = 0;
                loop {
                    let record = reader.read_record(&mut scratch);
                    let edit = match record {
                        Ok(slice) => {
                            read_records += 1;
                            match VersionEdit::decode_from(slice.data()) {
                                Ok(edit) => edit,
                                Err(_) => continue
                            }
                        },
                        Err(error) => {
                            break;
                        }
                    };
                    if edit.has_log_number {
                        log_number = edit.log_number;
                        have_log_number = true;
                    }
                    if edit.has_pre_log_number {
                        prev_log_number = edit.prev_log_number;
                        have_prev_log_number = true;
                    }
                    if edit.has_next_file_number {
                        next_file = edit.next_file_number;
                        have_next_file = true;
                    }
                    if edit.has_last_sequence {
                        last_sequence = edit.last_sequence;
                        have_last_sequence = true;
                    }
                }
                Ok(())
            }
        };
        if result.is_ok() {
            if !have_next_file {
                result = Err(Error::Corruption);
            } else if !have_log_number {
                result = Err(Error::Corruption);
            } else if !have_last_sequence {
                result = Err(Error::Corruption);
            }

            if !have_prev_log_number {
                prev_log_number = 0;
            }
            self.mark_file_number_used(prev_log_number);
            self.mark_file_number_used(log_number);
        }

        if result.is_ok() {

        }

        result
    }
}

#[derive(Default)]
pub struct Version {
    vset: Rc<RefCell<VersionSet>>,
    next: Option<NonNull<Version>>,
    prev: Option<NonNull<Version>>,
    refs: u32,
    files: Vec<FileMetaData>,
    file_to_compact: Option<FileMetaData>,
    file_to_compact_level: u32,
    compaction_score: f64,
    compaction_level: u32
}

impl Version {

    pub fn new(vset: Rc<RefCell<VersionSet>>) -> Self {
        Version {
            vset,
            .. Default::default()
        }
    }

    pub fn ref_(&mut self) {
        self.refs += 1;
    }

    pub fn unref(&mut self) {
        self.refs -= 1;
    }
}