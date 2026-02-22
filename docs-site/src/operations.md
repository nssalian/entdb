# Operating EntDB

EntDB runs as a single-node transactional SQL engine.

## Runtime boundaries

You can configure:

- max concurrent connections,
- max statement size,
- per-query timeout,
- auth policy (`md5` or `scram-sha-256`),
- optional TLS transport.

## Deployment basics

- Keep `--data-path` on durable storage.
- Use different data paths for dev/stage/prod.
- Keep auth enabled outside local development.
- Use TLS for non-local traffic.

## Lifecycle behavior

- Clean shutdown flushes dirty pages and transaction metadata.
- Restart replays recovery before serving queries.

## What to monitor

- query latency and error rate,
- transaction conflict rate,
- WAL flush latency,
- buffer pool pressure.
