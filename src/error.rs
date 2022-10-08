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

use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug, PartialEq)]
pub enum Error {
    NotFound = 1,
    Corruption = 2,
    NotSupport = 3,
    InvalidArgument = 4,
    IOError = 5
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Error::IOError
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => {
                panic!("object not found error")
            },
            Error::Corruption => {
                panic!("file corrupted")
            },
            Error::NotSupport => {
                panic!("not support")
            },
            Error::InvalidArgument => {
                panic!("invalid argument")
            },
            Error::IOError => {
                panic!("io error")
            },
            _ => {
                panic!("unknown error")
            }
        }
    }
}

impl std::error::Error for Error {

}