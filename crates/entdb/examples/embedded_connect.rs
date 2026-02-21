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

use entdb::{EntDb, QueryOutput};

fn main() -> entdb::Result<()> {
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let table_name = format!("users_{run_id}");

    let db = EntDb::connect("./entdb_data")?;
    db.execute(&format!("CREATE TABLE {table_name} (id INT, name TEXT)"))?;
    db.execute(&format!(
        "INSERT INTO {table_name} VALUES (1, 'alice'), (2, 'bob')"
    ))?;

    let out = db.execute(&format!("SELECT id, name FROM {table_name} ORDER BY id"))?;
    if let QueryOutput::Rows { columns, rows } = &out[0] {
        println!("{columns:?}");
        for row in rows {
            println!("{row:?}");
        }
    }

    db.execute(&format!("DROP TABLE {table_name}"))?;
    db.close()?;
    Ok(())
}
