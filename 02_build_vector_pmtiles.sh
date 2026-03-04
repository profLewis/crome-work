#!/bin/bash
# Build combined vector PMTiles for all CROME years (2017-2024)
#
# Pipeline per year:
#   1. Convert to per-county GeoJSONSeq files (from GDB or WFS)
#   2. Run tippecanoe to create per-year PMTiles
#   3. Clean up intermediate GeoJSONSeq
#
# After all years: tile-join to create combined PMTiles.
#
# Resumable: skips completed counties and completed years.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/crome_config.sh"

mkdir -p "$TMP_GEOJSONSEQ" "$PMTILES_PER_YEAR"

# ── GDB year processing ─────────────────────────────────────────────

process_gdb_year() {
  local year=$1
  local gdb_zip="$GDB_DIR/$(gdb_file "$year")"
  local year_dir="$TMP_GEOJSONSEQ/$year"
  mkdir -p "$year_dir"

  if [ ! -f "$gdb_zip" ]; then
    log_err "$year — GDB file not found: $gdb_zip"
    log_err "Run 01_download_gdb.sh first"
    return 1
  fi

  # Discover layer name inside GDB
  local gdb_path="/vsizip/$gdb_zip"
  local layers
  layers=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | awk '{print $2}')

  if [ -z "$layers" ]; then
    # Fallback: try unzipping first
    log "$year — /vsizip/ failed, unzipping GDB"
    local tmp_unzip="$GDB_DIR/tmp_${year}"
    mkdir -p "$tmp_unzip"
    unzip -o -q "$gdb_zip" -d "$tmp_unzip"
    gdb_path=$(find "$tmp_unzip" -name "*.gdb" -type d | head -1)
    if [ -z "$gdb_path" ]; then
      log_err "$year — no .gdb directory found in zip"
      rm -rf "$tmp_unzip"
      return 1
    fi
    layers=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | awk '{print $2}')
  fi

  if [ -z "$layers" ]; then
    log_err "$year — no layers found in GDB"
    return 1
  fi

  log "$year — GDB layers: $layers"

  local out_file="$year_dir/all.geojsonseq"
  if [ -f "$out_file" ]; then
    local count
    count=$(wc -l < "$out_file" | tr -d ' ')
    log "SKIP $year GDB extraction — already done ($count features)"
    return 0
  fi

  local tmp_file="$out_file.tmp"
  : > "$tmp_file"

  for layer in $layers; do
    log "$year — extracting layer $layer"

    # Check if lucode field exists; some layers may name it differently
    local fields
    fields=$(ogrinfo -ro -so "$gdb_path" "$layer" 2>/dev/null | grep -i lucode | head -1)
    local lucode_field="lucode"
    if [ -z "$fields" ]; then
      # Try uppercase
      fields=$(ogrinfo -ro -so "$gdb_path" "$layer" 2>/dev/null | grep -i LUCODE | head -1)
      if [ -n "$fields" ]; then
        lucode_field="LUCODE"
      else
        log_err "$year/$layer — no lucode field found, skipping"
        continue
      fi
    fi

    ogr2ogr -f GeoJSONSeq /vsistdout/ \
      "$gdb_path" "$layer" \
      -sql "SELECT ${lucode_field} as lucode, ${year} as year FROM \"${layer}\"" \
      -t_srs EPSG:4326 \
      2>/dev/null >> "$tmp_file"

    if [ $? -ne 0 ]; then
      log_err "$year/$layer — ogr2ogr failed"
    fi
  done

  local count
  count=$(wc -l < "$tmp_file" | tr -d ' ')
  if [ "$count" -gt 0 ]; then
    mv "$tmp_file" "$out_file"
    log "OK $year — $count features extracted from GDB"
  else
    log_err "$year — 0 features extracted"
    rm -f "$tmp_file"
    return 1
  fi

  # Clean up unzipped GDB if we created one
  [ -d "$GDB_DIR/tmp_${year}" ] && rm -rf "$GDB_DIR/tmp_${year}"
  return 0
}

# ── WFS year processing ──────────────────────────────────────────────

process_wfs_year() {
  local year=$1
  local wfs_url
  wfs_url="$(wfs_url_for_year "$year")"
  local year_dir="$TMP_GEOJSONSEQ/$year"
  mkdir -p "$year_dir"

  # Discover actual layer names from GetCapabilities
  log "$year — discovering WFS layers"
  local layers_file="$year_dir/layers.txt"

  if [ ! -f "$layers_file" ]; then
    discover_wfs_layers "$year" > "$layers_file"
  fi

  local total
  total=$(wc -l < "$layers_file" | tr -d ' ')
  if [ "$total" -eq 0 ]; then
    log_err "$year — no WFS layers discovered"
    return 1
  fi
  log "$year — $total county layers"

  local done_count=0
  local failed_count=0

  while IFS= read -r layer; do
    [ -n "$layer" ] || continue

    local county_file="$year_dir/${layer}.geojsonseq"

    # Skip completed counties
    if [ -f "$county_file" ]; then
      done_count=$((done_count + 1))
      continue
    fi

    local tmp_file="$county_file.tmp"
    log "$year — [$((done_count + failed_count + 1))/$total] $layer"

    local ok=0
    for attempt in $(seq 1 "$WFS_MAX_RETRIES"); do
      ogr2ogr -f GeoJSONSeq "$tmp_file" \
        "WFS:$wfs_url" "$layer" \
        -t_srs EPSG:4326 \
        -select lucode \
        --config OGR_WFS_PAGE_SIZE "$OGR_WFS_PAGE_SIZE" \
        -lco RS=YES \
        2>/dev/null

      if [ $? -eq 0 ] && [ -f "$tmp_file" ] && [ -s "$tmp_file" ]; then
        ok=1
        break
      fi

      log_err "$year/$layer — attempt $attempt/$WFS_MAX_RETRIES failed"
      rm -f "$tmp_file"
      sleep $((attempt * 10))
    done

    if [ "$ok" -eq 1 ]; then
      # Add year attribute to each feature using jq
      local with_year="$tmp_file.year"
      jq -c ".properties.year = $year" "$tmp_file" > "$with_year" 2>/dev/null
      if [ $? -eq 0 ] && [ -s "$with_year" ]; then
        mv "$with_year" "$county_file"
        rm -f "$tmp_file"
      else
        # jq failed — try sed as fallback
        sed "s/\"properties\":{/\"properties\":{\"year\":$year,/" "$tmp_file" > "$county_file"
        rm -f "$tmp_file" "$with_year"
      fi

      local count
      count=$(wc -l < "$county_file" | tr -d ' ')
      log "  OK $layer — $count features"
      done_count=$((done_count + 1))
    else
      failed_count=$((failed_count + 1))
      log_err "  FAIL $layer — retries exhausted"
    fi

  done < "$layers_file"

  log "$year — WFS done: $done_count OK, $failed_count failed out of $total"
  [ "$failed_count" -eq 0 ]
}

# ── Tippecanoe per year ──────────────────────────────────────────────

build_year_pmtiles() {
  local year=$1
  local pmtiles_out="$PMTILES_PER_YEAR/crome_${year}.pmtiles"

  if [ -f "$pmtiles_out" ]; then
    log "SKIP $year PMTiles — already exists"
    return 0
  fi

  local year_dir="$TMP_GEOJSONSEQ/$year"
  local geojsonseq_files
  geojsonseq_files=$(find "$year_dir" -name '*.geojsonseq' -type f 2>/dev/null)

  if [ -z "$geojsonseq_files" ]; then
    log_err "$year — no GeoJSONSeq files found"
    return 1
  fi

  local total_features
  total_features=$(cat "$year_dir"/*.geojsonseq | wc -l | tr -d ' ')
  log "$year — building PMTiles from $total_features features"

  cat "$year_dir"/*.geojsonseq | tippecanoe \
    -o "$pmtiles_out" \
    "${TIPPECANOE_OPTS[@]}"

  local rc=$?
  if [ $rc -eq 0 ]; then
    local size
    size=$(ls -lh "$pmtiles_out" | awk '{print $5}')
    log "OK $year PMTiles — $size"
  else
    log_err "$year — tippecanoe failed (exit $rc)"
    rm -f "$pmtiles_out"
    return 1
  fi
}

# ── Main ─────────────────────────────────────────────────────────────

log "=== CROME Vector PMTiles Pipeline ==="
log "Years: ${ALL_YEARS[*]}"
log ""

year_failures=()

for year in "${ALL_YEARS[@]}"; do
  pmtiles_out="$PMTILES_PER_YEAR/crome_${year}.pmtiles"

  if [ -f "$pmtiles_out" ]; then
    log "SKIP $year — PMTiles already complete"
    continue
  fi

  log "=== Processing year $year ==="

  # Phase 1: get GeoJSONSeq
  case $year in
    2017|2018|2019)
      process_gdb_year "$year" || { year_failures+=("$year"); continue; }
      ;;
    *)
      process_wfs_year "$year" || { year_failures+=("$year"); continue; }
      ;;
  esac

  # Phase 2: tippecanoe
  build_year_pmtiles "$year" || { year_failures+=("$year"); continue; }

  # Phase 3: cleanup intermediate files
  log "$year — cleaning up GeoJSONSeq"
  rm -rf "$TMP_GEOJSONSEQ/$year"
done

# ── Combine all years ────────────────────────────────────────────────

log ""
log "=== Combining per-year PMTiles ==="

per_year_files=()
for year in "${ALL_YEARS[@]}"; do
  f="$PMTILES_PER_YEAR/crome_${year}.pmtiles"
  if [ -f "$f" ]; then
    per_year_files+=("$f")
  else
    log_err "Missing PMTiles for $year"
  fi
done

if [ ${#per_year_files[@]} -eq 0 ]; then
  log_err "No per-year PMTiles to combine"
  exit 1
fi

log "Combining ${#per_year_files[@]} files into $FINAL_VECTOR"

tile-join -f \
  -o "$FINAL_VECTOR" \
  "${per_year_files[@]}"

if [ $? -eq 0 ]; then
  log "=== DONE ==="
  ls -lh "$FINAL_VECTOR"
else
  log_err "tile-join failed"
  exit 1
fi

if [ ${#year_failures[@]} -gt 0 ]; then
  log_err "WARNING: some years had failures: ${year_failures[*]}"
  log_err "The combined PMTiles is missing data for these years."
  log_err "Re-run this script to retry failed years."
fi
