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

extern crate core;

use crate::error::Error;

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

pub mod db;
pub mod error;
pub mod slice;
pub mod write_batch;
pub mod comparator;
pub mod log_writer;
pub mod options;

mod memtable;
mod log;
mod fs;
mod filename;
mod skiplist;
mod dbformat;
mod coding;
mod random;
mod env;
mod util;
mod log_format;
mod log_reader;
mod version_set;
mod version_edit;