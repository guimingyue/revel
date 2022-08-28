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

use crate::Result;

enum FileType {
    kLogFile,
    kDBLockFile,
    kTableFile,
    KDescriptorFile,
    kCurrentFile,
    kTempFile,
    kInfoLogFile
}

fn write_string_to_file_sync() -> Result<bool> {
    Ok(true)
}

fn make_file_name(path: &str, number: u64, suffix: &str) -> Box<String> {
    Box::new(format!("{}/{:06}.{}", path, number, suffix))
}

pub fn log_file_name(path: &str, number: u64) -> Box<String> {
    assert!(number > 0);
    make_file_name(path, number, "log")
}

#[test]
fn test() {
    assert_eq!("testdb/000192.log", make_file_name("testdb", 192, "log").as_str());
    assert_eq!("testdb/192345.log", make_file_name("testdb", 192345, "log").as_str());
    assert_eq!("testdb/1923457.log", make_file_name("testdb", 1923457, "log").as_str());
}