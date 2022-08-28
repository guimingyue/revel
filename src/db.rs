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
use std::path::Path;
use crate::Result;

pub struct DB {
    file: File,
}

pub struct Options {
    
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
        let db = DB {file};
        Ok(db)
    }

    pub fn put(key: &[u8], value: &[u8]) -> bool {
        true
    }
    
    pub fn get(key: &[u8]) -> bool {
        true
    }
}