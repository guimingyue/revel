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

use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Slice<'a> {
    data: &'a [u8],
    size: usize
}

impl<'a> Slice<'a> {
    
    pub fn from_empty() -> Self {
        Self::from_bytes("".as_bytes())
    }
    
    pub fn from_bytes(d: &'a[u8]) -> Self {
        Slice {
            data: d,
            size: d.len()
        }
    }
    
    pub fn size(&self) -> usize {
        self.data.len()
    }
    
    pub fn data(&self) -> &[u8]{
        self.data
    }
}

#[test]
fn test() {
    let slice1 = Slice::from_empty();
    let slice2 = Slice::from_bytes("".as_bytes());
    assert_eq!(slice1, slice2);
    assert_eq!(slice1.cmp(&slice2), Ordering::Equal);
    let slice3 = Slice::from_bytes("123".as_bytes());
    let slice4 = Slice::from_bytes("123".as_bytes());
    assert_eq!(slice3, slice4);
    let slice5 = Slice::from_bytes("124".as_bytes());
    assert_eq!(slice3.cmp(&slice5), Ordering::Less);
}