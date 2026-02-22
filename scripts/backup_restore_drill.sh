#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <data-db-path> <backup-dir>"
  exit 1
fi

DB_PATH="$1"
BACKUP_DIR="$2"
mkdir -p "$BACKUP_DIR"

if [[ ! -f "$DB_PATH" ]]; then
  echo "database file not found: $DB_PATH"
  exit 1
fi

base="$(basename "$DB_PATH")"
stem="${base%.*}"
ext="${base##*.}"
if [[ "$base" == "$ext" ]]; then
  stem="$base"
  ext=""
fi

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -f "$src" ]]; then
    cp -f "$src" "$dst"
  fi
}

copy_if_exists "$DB_PATH" "$BACKUP_DIR/$base"
copy_if_exists "${DB_PATH%.*}.wal" "$BACKUP_DIR/$stem.wal"
copy_if_exists "${DB_PATH%.*}.txn.wal" "$BACKUP_DIR/$stem.txn.wal"
copy_if_exists "${DB_PATH%.*}.txn.json" "$BACKUP_DIR/$stem.txn.json"

RESTORE_DIR="$BACKUP_DIR/restore-check"
mkdir -p "$RESTORE_DIR"
copy_if_exists "$BACKUP_DIR/$base" "$RESTORE_DIR/$base"
copy_if_exists "$BACKUP_DIR/$stem.wal" "$RESTORE_DIR/$stem.wal"
copy_if_exists "$BACKUP_DIR/$stem.txn.wal" "$RESTORE_DIR/$stem.txn.wal"
copy_if_exists "$BACKUP_DIR/$stem.txn.json" "$RESTORE_DIR/$stem.txn.json"

echo "Backup created at: $BACKUP_DIR"
echo "Restore drill snapshot at: $RESTORE_DIR"
echo "Next: point entdb-server --data to '$RESTORE_DIR/$base' and run a smoke query."
