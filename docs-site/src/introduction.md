# Introduction

EntDB is a Rust-based SQL database engine with PostgreSQL wire compatibility.

It runs in two modes:

- embedded library (`entdb` crate),
- server (`entdb-server`) for `psql` and pg drivers.

## Current scope

- Core DDL/DML for OLTP-style workloads.
- MVCC transactions.
- WAL + restart recovery.
- Optional polyglot SQL ingress rewrites.

See `quickstart.md` for usage and `architecture.md` for runtime layout.
