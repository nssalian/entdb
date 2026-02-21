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

use entdb::types::Value;
use entdb::{EntDb, QueryOutput};
use tempfile::tempdir;

#[test]
fn connect_open_close_and_basic_query_round_trip() {
    let dir = tempdir().expect("tempdir");
    let db_root = dir.path().join("embedded_connect");

    {
        let db = EntDb::connect(&db_root).expect("connect");
        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .expect("create table");
        db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
            .expect("insert rows");
        let out = db
            .execute("SELECT id, name FROM users ORDER BY id")
            .expect("select");
        match &out[0] {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![
                        vec![Value::Int32(1), Value::Text("alice".to_string())],
                        vec![Value::Int32(2), Value::Text("bob".to_string())]
                    ]
                );
            }
            _ => panic!("expected row output"),
        }
        db.close().expect("close");
    }

    let reopened = EntDb::connect(&db_root).expect("reconnect");
    let out = reopened
        .execute("SELECT id, name FROM users ORDER BY id")
        .expect("select after reconnect");
    match &out[0] {
        QueryOutput::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Int32(1));
            assert_eq!(rows[1][0], Value::Int32(2));
        }
        _ => panic!("expected row output"),
    }
}
