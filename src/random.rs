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

pub struct Random {
    seed: u32
}

impl Random {

    pub fn new(s: u32) -> Self {
        let mut seed = s & 0x7fffffff;
        if seed == 0 || seed == 2147483647 {
            seed = 1
        }
        Random {seed}
    }

    pub fn next(&mut self) -> u32 {
        const M: u32 = 2147483647;  // 2^31-1
        const A: u64 = 16807;   // bits 14, 8, 7, 5, 2, 1, 0
        // We are computing
        //       seed_ = (seed_ * A) % M,    where M = 2^31-1
        //
        // seed_ must not be zero or M, or else all subsequent computed values
        // will be zero or M respectively.  For all other values, seed_ will end
        // up cycling through every number in [1,M-1]
        let product = self.seed as u64 * A;

        // Compute (product % M) using the fact that ((x << 31) % M) == x.
        let mut seed = ((product >> 31) as u64 + (product & M as u64)) as u32;
        // The first reduction may overflow by 1 bit, so we may need to
        // repeat.  mod == M is not possible; using > allows the faster
        // sign-bit-based test.
        if seed > M {
            seed -= M;
        }
        self.seed = seed;
        return self.seed;
    }

    /// Returns a uniformly distributed value in the range [0..n-1]
    /// REQUIRES: n > 0
    fn uniform(&mut self, n: i32) -> u32{
        self.next() % n as u32
    }

    /// Randomly returns true ~"1/n" of the time, and false otherwise.
    /// REQUIRES: n > 0
    pub(crate) fn one_in(&mut self, n: i32) -> bool {
        self.next() % n as u32 == 0
    }

    /// Skewed: pick "base" uniformly from range [0,max_log] and then
    /// return "base" random bits.  The effect is to pick a number in the
    /// range [0,2^max_log-1] with exponential bias towards smaller numbers.
    fn skewed(&mut self, max_log: i32) -> u32 {
        let v: u32;
        {
            v = self.uniform(max_log + 1)
        }
        self.uniform(1 << v)
    }
}