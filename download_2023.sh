#!/bin/bash
# Download 2023 GDB with resume support, then build PMTiles
set -uo pipefail

ROOT="/Users/plewis/Documents/GitHub/crome-work"
GDB_DIR="$ROOT/gdb_downloads"
TMP_DIR="$ROOT/tmp_geojsonseq"
PMTILES_DIR="$ROOT/pmtiles_per_year"
TIPPECANOE="/opt/homebrew/bin/tippecanoe"

mkdir -p "$GDB_DIR" "$TMP_DIR" "$PMTILES_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

year=2023
id="4f8fbd8e-ccec-4fbb-b346-575b58afb38a"
gdb_file="$GDB_DIR/Crop_Map_of_England_CROME_2023.gdb.zip"
url="https://environment.data.gov.uk/api/file/download?fileDataSetId=${id}&fileName=Crop_Map_of_England_CROME_2023.gdb.zip"

# Step 1: Download with resume support
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  log "Download attempt $attempt (with resume)..."

  curl -L -C - --retry 5 --retry-delay 10 --retry-connrefused \
    --max-time 7200 --connect-timeout 30 \
    --speed-limit 5000 --speed-time 180 \
    -o "$gdb_file" "$url" 2>&1 | tail -5

  if [ -f "$gdb_file" ] && [ -s "$gdb_file" ]; then
    size=$(ls -lh "$gdb_file" | awk '{print $5}')
    log "Downloaded $size — testing zip integrity..."

    if unzip -t "$gdb_file" > /dev/null 2>&1; then
      log "ZIP OK — proceeding to extract"
      break
    else
      log "ZIP corrupt — deleting and retrying from scratch"
      rm -f "$gdb_file"
    fi
  else
    log "Download incomplete, retrying..."
  fi

  sleep $((attempt * 10))
done

if [ ! -f "$gdb_file" ]; then
  log "ERROR: All download attempts failed"
  exit 1
fi

# Step 2: Open GDB
log "Opening GDB..."
gdb_path="/vsizip/$gdb_file"
if ! ogrinfo -ro -so "$gdb_path" >/dev/null 2>&1; then
  # Extract
  tmp_dir="$GDB_DIR/tmp_extract_2023"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"
  unzip -o -q "$gdb_file" -d "$tmp_dir"

  gdb_dir=$(find "$tmp_dir" -name "*.gdb" -type d 2>/dev/null | head -1)
  if [ -n "$gdb_dir" ]; then
    gdb_path="$gdb_dir"
  else
    renamed="$GDB_DIR/CROME_2023.gdb"
    rm -rf "$renamed"
    mv "$tmp_dir" "$renamed"
    gdb_path="$renamed"
  fi
fi

# Find layer
layers=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | awk '{print $2}')
log "Layers: $(echo $layers | wc -w | tr -d ' ') found"

national_layer=""
for l in $layers; do
  if echo "$l" | grep -qE "^Crop_Map_[Oo]f_England_2023$"; then
    national_layer="$l"
    break
  fi
done
[ -z "$national_layer" ] && national_layer=$(echo "$layers" | head -1)
log "Using layer: $national_layer"

# Step 3: Extract to GeoJSONSeq
geojsonseq="$TMP_DIR/2023_all.geojsonseq"
log "Extracting to GeoJSONSeq..."
ogr2ogr -f GeoJSONSeq "$geojsonseq.tmp" \
  "$gdb_path" "$national_layer" \
  -sql "SELECT lucode, 2023 as year FROM \"${national_layer}\"" \
  -t_srs EPSG:4326 \
  2>/dev/null

if [ $? -eq 0 ] && [ -s "$geojsonseq.tmp" ]; then
  mv "$geojsonseq.tmp" "$geojsonseq"
  count=$(wc -l < "$geojsonseq" | tr -d ' ')
  size=$(ls -lh "$geojsonseq" | awk '{print $5}')
  log "Extracted $count features ($size)"
else
  log "ERROR: extraction failed"
  exit 1
fi

# Step 4: Build PMTiles
pmtiles_out="$PMTILES_DIR/crome_2023.pmtiles"
log "Building PMTiles..."
cat "$geojsonseq" | "$TIPPECANOE" \
  -o "$pmtiles_out" \
  -l crome -z12 \
  --coalesce-densest-as-needed --drop-densest-as-needed \
  --drop-rate=1 --hilbert \
  -y lucode -y year \
  --simplification=10 --force

if [ $? -eq 0 ] && [ -f "$pmtiles_out" ]; then
  size=$(ls -lh "$pmtiles_out" | awk '{print $5}')
  log "OK 2023 — PMTiles complete ($size)"
else
  log "ERROR: tippecanoe failed"
  exit 1
fi

# Cleanup
rm -f "$geojsonseq"
rm -f "$gdb_file"
rm -rf "$GDB_DIR/tmp_extract_2023" "$GDB_DIR/CROME_2023.gdb"
log "Done! Cleaned up temp files."
