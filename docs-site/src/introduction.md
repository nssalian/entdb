# Introduction

EntDB is a Rust-based SQL database engine with PostgreSQL wire compatibility.

It runs in two modes:

- embedded library (`entdb` crate),
- server (`entdb-server`) for `psql` and pg drivers.

## Current scope

- Core DDL/DML for OLTP-style workloads.
- MVCC transactions.
- WAL + restart recovery.
- Optional polyglot SQL ingress on both server and embedded paths.
  When enabled, EntDB rewrites selected non-PostgreSQL SQL forms (for example
  MySQL backticks and numeric `LIMIT offset, count`) into PostgreSQL-compatible
  SQL before parsing.

See `quickstart.md` for usage and `architecture.md` for runtime layout.
