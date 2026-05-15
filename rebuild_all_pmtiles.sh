#!/bin/bash
# Rebuild all PMTiles with better tippecanoe settings
set -uo pipefail

ROOT="/Users/plewis/Documents/GitHub/crome-work"
TMP_DIR="$ROOT/tmp_geojsonseq"
PMTILES_DIR="$ROOT/pmtiles_per_year"
TIPPECANOE="/opt/homebrew/bin/tippecanoe"
GDB_DIR="$ROOT/gdb_downloads"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

tippecanoe_build() {
  local year=$1
  local input=$2
  local out="$PMTILES_DIR/crome_${year}.pmtiles"

  log "$year — building PMTiles from $input..."
  cat "$input" | "$TIPPECANOE" \
    -o "$out" \
    -l crome -z12 \
    --no-feature-limit --no-tile-size-limit \
    --drop-smallest-as-needed \
    --drop-rate=1 --hilbert \
    -y lucode -y year \
    --simplification=10 --force 2>&1 | tail -3

  if [ $? -eq 0 ] && [ -f "$out" ]; then
    size=$(ls -lh "$out" | awk '{print $5}')
    log "OK $year — $size"
  else
    log "ERROR $year — tippecanoe failed"
  fi
}

# --- 2020, 2021, 2022: extract from GDB tables then build ---
for year in 2020 2021 2022; do
  geojsonseq="$TMP_DIR/${year}_all.geojsonseq"
  gdb_path="$GDB_DIR/tmp_${year}"

  if [ ! -f "$geojsonseq" ]; then
    log "$year — extracting GDB to GeoJSONSeq..."

    # Find layer name
    layer=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE 'Crop_Map_[Oo]f_England_[0-9]+' | head -1)
    if [ -z "$layer" ]; then
      layer=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | head -1 | awk '{print $2}')
    fi
    log "$year — layer: $layer"

    ogr2ogr -f GeoJSONSeq "$geojsonseq.tmp" \
      "$gdb_path" "$layer" \
      -sql "SELECT lucode, ${year} as year FROM \"${layer}\"" \
      -t_srs EPSG:4326 2>/dev/null

    if [ $? -eq 0 ] && [ -s "$geojsonseq.tmp" ]; then
      mv "$geojsonseq.tmp" "$geojsonseq"
      count=$(wc -l < "$geojsonseq" | tr -d ' ')
      log "$year — extracted $count features"
    else
      log "ERROR $year — extraction failed"
      rm -f "$geojsonseq.tmp"
      continue
    fi
  fi

  tippecanoe_build "$year" "$geojsonseq"
done

# --- 2024: already have geojsonseq ---
if [ -f "$TMP_DIR/2024_all.geojsonseq" ]; then
  tippecanoe_build 2024 "$TMP_DIR/2024_all.geojsonseq"
else
  log "2024 — no source data, skipping"
fi

# --- 2017, 2018, 2019: download via WFS then build ---
for year in 2017 2018 2019; do
  geojsonseq="$TMP_DIR/${year}_all.geojsonseq"
  county_dir="$ROOT/counties_wfs_${year}"

  if [ -f "$geojsonseq" ] && [ -s "$geojsonseq" ]; then
    log "$year — GeoJSONSeq already exists"
    tippecanoe_build "$year" "$geojsonseq"
    continue
  fi

  # Download via WFS
  WFS_HTTP="https://environment.data.gov.uk/spatialdata/crop-map-of-england-${year}/wfs"
  mkdir -p "$county_dir"

  log "$year — fetching WFS layer list..."
  caps=$(curl -sS --max-time 60 "${WFS_HTTP}?service=WFS&version=2.0.0&request=GetCapabilities")
  layers=$(echo "$caps" | grep -oE "Crop_Map_of_England_${year}_[A-Za-z_]*" | sort -u)
  total=$(echo "$layers" | wc -l | tr -d ' ')

  if [ "$total" -eq 0 ]; then
    log "ERROR $year — no WFS layers found"
    continue
  fi
  log "$year — found $total county layers"

  for layer in $layers; do
    out="$county_dir/${layer}.geojsonl"
    if [ -f "$out" ]; then
      continue
    fi

    tmp="$out.tmp"
    [ -f "$tmp" ] || : > "$tmp"
    start=$(wc -l < "$tmp" | tr -d ' ')

    log "$year — FETCH $layer (startIndex=$start)"

    county_ok=1
    while true; do
      url="${WFS_HTTP}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${layer}&outputFormat=application/json&srsName=EPSG:4326&count=5000&startIndex=${start}"

      got_json=0; n=0
      for attempt in 1 2 3 4 5; do
        resp=$(mktemp)
        curl -sS --max-time 120 "$url" -o "$resp"
        if jq -e '.features and (.features|type=="array")' "$resp" >/dev/null 2>&1; then
          n=$(jq '.features | length' "$resp")
          got_json=1; break
        fi
        rm -f "$resp"
        sleep $((attempt * 2))
      done

      [ "$got_json" -eq 0 ] && { county_ok=0; break; }
      [ "$n" -eq 0 ] && { rm -f "$resp"; break; }

      jq -c '.features[]' "$resp" >> "$tmp"
      rm -f "$resp"
      start=$((start + n))
      [ "$n" -lt 5000 ] && break
    done

    if [ "$county_ok" -eq 1 ]; then
      mv "$tmp" "$out"
    fi
  done

  # Combine and build
  log "$year — combining county files..."
  cat "$county_dir"/*.geojsonl > "$geojsonseq"
  count=$(wc -l < "$geojsonseq" | tr -d ' ')
  log "$year — $count features total"

  tippecanoe_build "$year" "$geojsonseq"
done

log ""
log "=== All done ==="
ls -lh "$PMTILES_DIR"/crome_*.pmtiles
