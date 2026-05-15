#!/bin/bash
# Fix missing counties in 2021 and 2022 CROME PMTiles
# Also rebuild 2017-2019 with improved tippecanoe settings
set -uo pipefail

ROOT="/Users/plewis/Documents/GitHub/crome-work"
TMP_DIR="$ROOT/tmp_geojsonseq"
PMTILES_DIR="$ROOT/pmtiles_per_year"
TIPPECANOE="/opt/homebrew/bin/tippecanoe"
FIX_DIR="/tmp/crome_fix"
mkdir -p "$FIX_DIR" "$PMTILES_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# Download a single county via WFS, outputting GeoJSONSeq lines to stdout
download_county() {
  local year=$1
  local layer=$2
  local wfs="https://environment.data.gov.uk/spatialdata/crop-map-of-england-${year}/wfs"
  local start=0
  local total=0

  while true; do
    local url="${wfs}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${layer}&outputFormat=application/json&srsName=EPSG:4326&count=5000&startIndex=${start}"
    local resp=$(mktemp)
    local ok=0

    for attempt in 1 2 3; do
      curl -sS --max-time 120 "$url" -o "$resp" 2>/dev/null
      if jq -e '.features and (.features|type=="array")' "$resp" >/dev/null 2>&1; then
        ok=1; break
      fi
      sleep $((attempt * 2))
    done

    if [ "$ok" -eq 0 ]; then
      log "  ERROR: failed to fetch $layer at startIndex=$start"
      rm -f "$resp"
      return 1
    fi

    local n=$(jq '.features | length' "$resp")
    if [ "$n" -eq 0 ]; then
      rm -f "$resp"
      break
    fi

    # Output features as GeoJSONSeq, keeping only lucode property
    jq -c '.features[] | {type: .type, geometry: .geometry, properties: {lucode: .properties.lucode}}' "$resp"
    rm -f "$resp"

    total=$((total + n))
    start=$((start + n))
    [ "$n" -lt 5000 ] && break
  done

  log "  $layer: $total features" >&2
}

tippecanoe_build() {
  local year=$1
  local input=$2
  local out="$PMTILES_DIR/crome_${year}.pmtiles"

  local count=$(wc -l < "$input" | tr -d ' ')
  log "$year — building PMTiles from $count features..."
  cat "$input" | "$TIPPECANOE" \
    -o "$out" \
    -l crome -z12 \
    --no-feature-limit --no-tile-size-limit \
    --drop-smallest-as-needed \
    --drop-rate=1 --hilbert \
    -y lucode \
    --simplification=10 --force 2>&1 | tail -3

  if [ $? -eq 0 ] && [ -f "$out" ]; then
    size=$(ls -lh "$out" | awk '{print $5}')
    log "OK $year — $size ($count features)"
  else
    log "ERROR $year — tippecanoe failed"
    return 1
  fi
}

# ===== FIX 2021: missing East Sussex, Greater Manchester, Warwickshire =====
log ""
log "===== Fixing 2021 (3 missing counties) ====="
MISSING_2021="Crop_Map_of_England_2021_East_Sussex Crop_Map_of_England_2021_Greater_Manchester Crop_Map_of_England_2021_Warwickshire"
FIX_2021="$FIX_DIR/2021_missing.geojsonseq"
> "$FIX_2021"

for layer in $MISSING_2021; do
  log "  Downloading $layer..."
  download_county 2021 "$layer" >> "$FIX_2021"
done

added=$(wc -l < "$FIX_2021" | tr -d ' ')
log "2021 — downloaded $added missing features"

# Append to existing and rebuild
FULL_2021="$TMP_DIR/2021_all.geojsonseq"
cp "$FULL_2021" "$FIX_DIR/2021_backup.geojsonseq"
cat "$FIX_2021" >> "$FULL_2021"
new_total=$(wc -l < "$FULL_2021" | tr -d ' ')
log "2021 — new total: $new_total features"

tippecanoe_build 2021 "$FULL_2021"

# ===== FIX 2022: missing Merseyside, Surrey, West Yorkshire =====
log ""
log "===== Fixing 2022 (3 missing counties) ====="
MISSING_2022="Crop_Map_of_England_2022_Merseyside Crop_Map_of_England_2022_Surrey Crop_Map_of_England_2022_West_Yorkshire"
FIX_2022="$FIX_DIR/2022_missing.geojsonseq"
> "$FIX_2022"

for layer in $MISSING_2022; do
  log "  Downloading $layer..."
  download_county 2022 "$layer" >> "$FIX_2022"
done

added=$(wc -l < "$FIX_2022" | tr -d ' ')
log "2022 — downloaded $added missing features"

FULL_2022="$TMP_DIR/2022_all.geojsonseq"
cp "$FULL_2022" "$FIX_DIR/2022_backup.geojsonseq"
cat "$FIX_2022" >> "$FULL_2022"
new_total=$(wc -l < "$FULL_2022" | tr -d ' ')
log "2022 — new total: $new_total features"

tippecanoe_build 2022 "$FULL_2022"

# ===== REBUILD 2017-2019 with better tippecanoe settings =====
for year in 2017 2018 2019; do
  log ""
  log "===== Rebuilding $year (better tippecanoe settings) ====="
  geojsonseq="$TMP_DIR/${year}_all.geojsonseq"
  if [ -f "$geojsonseq" ] && [ -s "$geojsonseq" ]; then
    tippecanoe_build "$year" "$geojsonseq"
  else
    log "$year — no GeoJSONSeq found, skipping"
  fi
done

log ""
log "===== Summary ====="
ls -lh "$PMTILES_DIR"/crome_*.pmtiles

log ""
log "To deploy, copy to crome-maps:"
log "  cp $PMTILES_DIR/crome_{2017,2018,2019,2021,2022}.pmtiles /Users/plewis/Documents/GitHub/crome-maps/"
log ""
log "Done!"
