/*
 * Copyright 2026 EntDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

fn registry() -> &'static Mutex<HashMap<String, usize>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_failpoint(name: &str, hits_before_fail: usize) {
    let mut reg = registry().lock().expect("failpoint lock");
    reg.insert(name.to_string(), hits_before_fail);
}

pub fn clear_failpoint(name: &str) {
    let mut reg = registry().lock().expect("failpoint lock");
    reg.remove(name);
}

pub fn clear_all_failpoints() {
    let mut reg = registry().lock().expect("failpoint lock");
    reg.clear();
}

pub fn should_fail(name: &str) -> bool {
    let mut reg = registry().lock().expect("failpoint lock");
    let Some(remaining) = reg.get_mut(name) else {
        return false;
    };

    if *remaining == 0 {
        reg.remove(name);
        return true;
    }

    *remaining -= 1;
    false
}
