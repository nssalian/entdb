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

use crate::error::{EntDbError, Result};
use polyglot_sql::transpile_by_name;
use tracing::debug;

#[derive(Debug, Clone, Copy)]
pub struct PolyglotOptions {
    pub enabled: bool,
}

impl Default for PolyglotOptions {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranspileOutput {
    pub original_sql: String,
    pub transpiled_sql: String,
    pub changed: bool,
}

pub fn transpile(sql: &str, opts: PolyglotOptions) -> Result<String> {
    Ok(transpile_with_meta(sql, opts)?.transpiled_sql)
}

pub fn transpile_with_meta(sql: &str, opts: PolyglotOptions) -> Result<TranspileOutput> {
    if !opts.enabled {
        return Ok(TranspileOutput {
            original_sql: sql.to_string(),
            transpiled_sql: sql.to_string(),
            changed: false,
        });
    }

    validate_input(sql)?;
    if !should_attempt_transpile(sql) {
        return Ok(TranspileOutput {
            original_sql: sql.to_string(),
            transpiled_sql: sql.to_string(),
            changed: false,
        });
    }
    let out = transpile_with_external_or_fallback(sql)?;
    let out = align_terminal_semicolon(sql, out);
    validate_output(&out)?;

    let changed = out != sql;
    if changed {
        debug!(
            original = sql,
            transpiled = out,
            "polyglot transpilation applied"
        );
    }
    Ok(TranspileOutput {
        original_sql: sql.to_string(),
        transpiled_sql: out,
        changed,
    })
}

fn align_terminal_semicolon(original: &str, transpiled: String) -> String {
    let original_has_semicolon = original.trim_end().ends_with(';');
    let transpiled_has_semicolon = transpiled.trim_end().ends_with(';');
    if original_has_semicolon && !transpiled_has_semicolon {
        format!("{transpiled};")
    } else {
        transpiled
    }
}

fn should_attempt_transpile(sql: &str) -> bool {
    sql.contains('`') || has_mysql_limit_comma_shape(sql)
}

fn has_mysql_limit_comma_shape(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    let Some(limit_idx) = upper.rfind(" LIMIT ") else {
        return false;
    };
    sql[limit_idx + 7..].contains(',')
}

fn transpile_with_external_or_fallback(sql: &str) -> Result<String> {
    // Fast path for the currently supported cross-dialect surface:
    // normalize backticks and MySQL LIMIT offset,count without invoking the
    // external transpiler on every statement.
    let mut rewritten = rewrite_backtick_identifiers(sql)?;
    rewritten = rewrite_mysql_limit_syntax(&rewritten)?;
    if rewritten != sql {
        return Ok(rewritten);
    }

    const TARGET_DIALECT: &str = "postgresql";
    // Try common ingress dialects first; fallback path below preserves prior behavior.
    const SOURCE_DIALECTS: &[&str] = &[
        "mysql",
        "postgresql",
        "sqlite",
        "duckdb",
        "snowflake",
        "clickhouse",
        "bigquery",
        "redshift",
        "trino",
        "ansi",
        "generic",
    ];

    for source in SOURCE_DIALECTS {
        if let Ok(candidates) = transpile_by_name(sql, source, TARGET_DIALECT) {
            let normalized = normalize_transpiled_statements(candidates);
            if !normalized.is_empty() {
                let preserved = preserve_non_numeric_mysql_limit_shape(sql, &normalized)?;
                return Ok(preserved);
            }
        }
    }

    Ok(sql.to_string())
}

fn normalize_transpiled_statements(statements: Vec<String>) -> String {
    let cleaned: Vec<String> = statements
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    cleaned.join("; ")
}

fn preserve_non_numeric_mysql_limit_shape(original: &str, candidate: &str) -> Result<String> {
    if has_non_numeric_mysql_limit(original) {
        // Preserve previous contract for unsupported LIMIT rewrite shapes.
        return rewrite_backtick_identifiers(original);
    }
    rewrite_mysql_limit_syntax(candidate)
}

fn has_non_numeric_mysql_limit(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    let Some(limit_idx) = upper.rfind(" LIMIT ") else {
        return false;
    };
    let tail = sql[limit_idx + 7..].trim();
    let Some(comma_idx) = tail.find(',') else {
        return false;
    };
    let (left, right) = tail.split_at(comma_idx);
    let offset = left.trim();
    let count_raw = right.trim_start_matches(',').trim();
    let count = count_raw.strip_suffix(';').unwrap_or(count_raw).trim_end();
    if offset.is_empty() || count.is_empty() {
        return true;
    }
    !offset.chars().all(|c| c.is_ascii_digit()) || !count.chars().all(|c| c.is_ascii_digit())
}

fn rewrite_backtick_identifiers(sql: &str) -> Result<String> {
    let ticks = sql.chars().filter(|c| *c == '`').count();
    if ticks % 2 != 0 {
        return Err(EntDbError::Query(
            "POLYGLOT_E_UNBALANCED_BACKTICKS: unbalanced backtick identifiers".to_string(),
        ));
    }
    Ok(sql.replace('`', "\""))
}

fn rewrite_mysql_limit_syntax(sql: &str) -> Result<String> {
    let upper = sql.to_ascii_uppercase();
    let Some(limit_idx) = upper.rfind(" LIMIT ") else {
        return Ok(sql.to_string());
    };

    let prefix = &sql[..limit_idx + 7];
    let tail = sql[limit_idx + 7..].trim();

    let Some(comma_idx) = tail.find(',') else {
        return Ok(sql.to_string());
    };
    let (left, right) = tail.split_at(comma_idx);
    let offset = left.trim();
    let count_raw = right.trim_start_matches(',').trim();
    let (count, suffix) = if let Some(stripped) = count_raw.strip_suffix(';') {
        (stripped.trim_end(), ";")
    } else {
        (count_raw, "")
    };
    if offset.is_empty() || count.is_empty() {
        return Err(EntDbError::Query(
            "POLYGLOT_E_LIMIT_SYNTAX: invalid MySQL LIMIT clause for transpilation".to_string(),
        ));
    }
    if !offset.chars().all(|c| c.is_ascii_digit()) || !count.chars().all(|c| c.is_ascii_digit()) {
        return Ok(sql.to_string());
    }

    Ok(format!("{prefix}{count} OFFSET {offset}{suffix}"))
}

fn validate_input(sql: &str) -> Result<()> {
    let upper = sql.to_ascii_uppercase();
    if upper.contains(" DELIMITER ") || upper.starts_with("DELIMITER ") {
        return Err(EntDbError::Query(
            "POLYGLOT_E_UNSUPPORTED_SYNTAX: DELIMITER statements are unsupported".to_string(),
        ));
    }
    Ok(())
}

fn validate_output(sql: &str) -> Result<()> {
    if sql.contains(";;") {
        return Err(EntDbError::Query(
            "POLYGLOT_E_OUTPUT_INVALID: transpilation produced invalid SQL separator sequence"
                .to_string(),
        ));
    }
    Ok(())
}
