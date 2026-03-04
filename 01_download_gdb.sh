#!/bin/bash
# Download CROME GDB files for 2017-2019 (years without WFS endpoints)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/crome_config.sh"

mkdir -p "$GDB_DIR"

failed=()

for year in "${YEARS_GDB[@]}"; do
  url="$(gdb_url "$year")"
  filename="$(gdb_file "$year")"
  outfile="$GDB_DIR/$filename"
  min_size="$(gdb_min_size "$year")"

  # Skip if already downloaded and verified
  if [ -f "$outfile" ]; then
    actual_size=$(stat -f%z "$outfile" 2>/dev/null || stat -c%s "$outfile" 2>/dev/null)
    if [ "$actual_size" -ge "$min_size" ]; then
      log "SKIP $year — already downloaded ($filename, $(numfmt --to=iec "$actual_size" 2>/dev/null || echo "${actual_size} bytes"))"
      continue
    fi
    log "$year — existing file too small (${actual_size} < ${min_size}), resuming download"
  fi

  log "DOWNLOAD $year — $filename"

  for attempt in 1 2 3 4 5; do
    # Server doesn't support byte-range resume; download fresh each attempt
    rm -f "$outfile"
    curl -L \
      --retry 3 \
      --retry-delay 10 \
      --max-time 3600 \
      --speed-limit 10000 \
      --speed-time 120 \
      -o "$outfile" \
      "$url"
    curl_rc=$?

    if [ $curl_rc -eq 0 ]; then
      actual_size=$(stat -f%z "$outfile" 2>/dev/null || stat -c%s "$outfile" 2>/dev/null)
      if [ "$actual_size" -ge "$min_size" ]; then
        log "VERIFY $year — checking zip integrity"
        if unzip -t "$outfile" > /dev/null 2>&1; then
          log "OK $year — $filename verified ($(numfmt --to=iec "$actual_size" 2>/dev/null || echo "${actual_size} bytes"))"
          break
        else
          log_err "$year — zip integrity check failed, retrying"
          rm -f "$outfile"
        fi
      else
        log_err "$year — file too small after download (${actual_size} < ${min_size})"
      fi
    else
      log_err "$year — curl failed (exit $curl_rc), attempt $attempt/5"
    fi

    if [ $attempt -lt 5 ]; then
      sleep $((attempt * 15))
    else
      failed+=("$year")
    fi
  done
done

if [ ${#failed[@]} -gt 0 ]; then
  log_err "FAILED years: ${failed[*]}"
  exit 1
fi

log "All GDB downloads complete"
ls -lh "$GDB_DIR"/*.zip
