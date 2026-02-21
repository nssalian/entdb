# EntDB Fuzz Targets

Requires `cargo-fuzz`:

```bash
cargo install cargo-fuzz
rustup toolchain install nightly
```

Run targets:

```bash
cargo +nightly fuzz run tuple_roundtrip
cargo +nightly fuzz run wal_decode
cargo +nightly fuzz run query_parse_bind
cargo +nightly fuzz run catalog_load
cargo +nightly fuzz run polyglot_transpile
```

Seed corpora live in:

- `fuzz/corpus/tuple_roundtrip`
- `fuzz/corpus/wal_decode`
- `fuzz/corpus/query_parse_bind`
- `fuzz/corpus/catalog_load`
- `fuzz/corpus/polyglot_transpile`
