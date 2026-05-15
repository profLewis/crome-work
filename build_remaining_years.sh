#!/bin/bash
# Download GDB bulk files for 2020-2024 and build vector PMTiles
set -uo pipefail

ROOT="/Users/plewis/Documents/GitHub/crome-work"
GDB_DIR="$ROOT/gdb_downloads"
TMP_DIR="$ROOT/tmp_geojsonseq"
PMTILES_DIR="$ROOT/pmtiles_per_year"
TIPPECANOE="/opt/homebrew/bin/tippecanoe"

mkdir -p "$GDB_DIR" "$TMP_DIR" "$PMTILES_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

gdb_id() {
  case $1 in
    2020) echo "41adeeb3-89f1-4a86-8982-c2d27b5f3e6f" ;;
    2021) echo "f447e4c3-4783-4e83-b9e4-61694f439d82" ;;
    2022) echo "9eff03a0-55ce-44fd-b29b-9735f8d71e8d" ;;
    2023) echo "4f8fbd8e-ccec-4fbb-b346-575b58afb38a" ;;
    2024) echo "2c6afc6a-ece4-4656-ac30-0ebaeb0459a3" ;;
  esac
}

open_gdb() {
  # Given a gdb zip, return a path usable by ogrinfo/ogr2ogr
  local gdb_zip=$1
  local year=$2
  
  # Try /vsizip/ first
  local gdb_path="/vsizip/$gdb_zip"
  if ogrinfo -ro -so "$gdb_path" >/dev/null 2>&1; then
    echo "$gdb_path"
    return 0
  fi
  
  # Unzip and find .gdb directory
  local tmp_dir="$GDB_DIR/tmp_extract_${year}"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"
  unzip -o -q "$gdb_zip" -d "$tmp_dir"
  
  # Look for .gdb directory
  local gdb_dir
  gdb_dir=$(find "$tmp_dir" -name "*.gdb" -type d 2>/dev/null | head -1)
  
  if [ -n "$gdb_dir" ]; then
    echo "$gdb_dir"
    return 0
  fi
  
  # No .gdb dir — files are at root level. Rename the extract dir to .gdb
  local renamed="$GDB_DIR/CROME_${year}.gdb"
  rm -rf "$renamed"
  mv "$tmp_dir" "$renamed"
  
  if ogrinfo -ro -so "$renamed" >/dev/null 2>&1; then
    echo "$renamed"
    return 0
  fi
  
  log "ERROR: Cannot open GDB for $year"
  return 1
}

for year in 2020 2021 2022 2023 2024; do
  pmtiles_out="$PMTILES_DIR/crome_${year}.pmtiles"
  
  if [ -f "$pmtiles_out" ]; then
    log "SKIP $year — PMTiles already exists"
    continue
  fi

  log "=== $year ==="

  geojsonseq="$TMP_DIR/${year}_all.geojsonseq"
  
  # Skip if GeoJSONSeq already exists (e.g. 2020 being extracted separately)
  if [ -f "$geojsonseq" ]; then
    log "$year — GeoJSONSeq already exists"
  else
    # Step 1: Download GDB
    gdb_file="$GDB_DIR/Crop_Map_of_England_CROME_${year}.gdb.zip"
    if [ ! -f "$gdb_file" ]; then
      id=$(gdb_id "$year")
      url="https://environment.data.gov.uk/api/file/download?fileDataSetId=${id}&fileName=Crop_Map_of_England_CROME_${year}.gdb.zip"
      log "$year — downloading GDB..."
      
      for attempt in 1 2 3 4 5; do
        rm -f "$gdb_file"
        curl -L --retry 3 --retry-delay 10 --max-time 3600 \
          --speed-limit 10000 --speed-time 120 \
          -o "$gdb_file" "$url" 2>&1 | tail -1
        
        if [ $? -eq 0 ] && [ -f "$gdb_file" ] && [ -s "$gdb_file" ]; then
          if unzip -t "$gdb_file" > /dev/null 2>&1; then
            size=$(ls -lh "$gdb_file" | awk '{print $5}')
            log "$year — GDB downloaded OK ($size)"
            break
          else
            log "ERROR $year — zip integrity failed, attempt $attempt/5"
            rm -f "$gdb_file"
          fi
        else
          log "ERROR $year — download failed, attempt $attempt/5"
          rm -f "$gdb_file"
        fi
        sleep $((attempt * 15))
      done
      
      if [ ! -f "$gdb_file" ]; then
        log "ERROR $year — all download attempts failed, skipping"
        continue
      fi
    fi

    # Step 2: Open GDB and find national layer
    gdb_path=$(open_gdb "$gdb_file" "$year")
    if [ -z "$gdb_path" ]; then
      log "ERROR $year — cannot open GDB"
      continue
    fi
    
    layers=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | awk '{print $2}')
    log "$year — layers: $(echo $layers | wc -w | tr -d ' ') found"
    
    # Find national layer
    national_layer=""
    for l in $layers; do
      if echo "$l" | grep -qE "^Crop_Map_[Oo]f_England_${year}$"; then
        national_layer="$l"
        break
      fi
    done
    
    if [ -z "$national_layer" ]; then
      national_layer=$(echo "$layers" | head -1)
      log "$year — no national layer, using: $national_layer"
    else
      log "$year — national layer: $national_layer"
    fi
    
    # Step 3: Extract to GeoJSONSeq
    log "$year — extracting to GeoJSONSeq..."
    ogr2ogr -f GeoJSONSeq "$geojsonseq.tmp" \
      "$gdb_path" "$national_layer" \
      -sql "SELECT lucode, ${year} as year FROM \"${national_layer}\"" \
      -t_srs EPSG:4326 \
      2>/dev/null
    
    if [ $? -eq 0 ] && [ -s "$geojsonseq.tmp" ]; then
      mv "$geojsonseq.tmp" "$geojsonseq"
      count=$(wc -l < "$geojsonseq" | tr -d ' ')
      size=$(ls -lh "$geojsonseq" | awk '{print $5}')
      log "$year — extracted $count features ($size)"
    else
      log "ERROR $year — extraction failed"
      rm -f "$geojsonseq.tmp"
      continue
    fi
    
    # Clean up GDB files
    rm -f "$gdb_file"
    rm -rf "$GDB_DIR/tmp_extract_${year}" "$GDB_DIR/CROME_${year}.gdb"
  fi

  # Step 4: Build PMTiles
  log "$year — building PMTiles..."
  cat "$geojsonseq" | "$TIPPECANOE" \
    -o "$pmtiles_out" \
    -l crome -z12 \
    --coalesce-densest-as-needed --drop-densest-as-needed \
    --drop-rate=1 --hilbert \
    -y lucode -y year \
    --simplification=10 --force
  
  if [ $? -eq 0 ] && [ -f "$pmtiles_out" ]; then
    size=$(ls -lh "$pmtiles_out" | awk '{print $5}')
    log "OK $year — PMTiles complete ($size)"
  else
    log "ERROR $year — tippecanoe failed"
    rm -f "$pmtiles_out"
    continue
  fi

  # Step 5: Clean up
  rm -f "$geojsonseq"
  log "$year — cleaned up"
  df -h "$ROOT" | tail -1 | awk '{print "  Free: " $4}'
done

log ""
log "=== Done ==="
ls -lh "$PMTILES_DIR"/crome_*.pmtiles
