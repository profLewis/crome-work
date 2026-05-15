#!/bin/bash
# Download CROME 2023 via WFS county-by-county, then build PMTiles
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="$ROOT/counties_wfs_2023"
WFS_HTTP="https://environment.data.gov.uk/spatialdata/crop-map-of-england-2023/wfs"
GEOJSONSEQ="$ROOT/tmp_geojsonseq/2023_all.geojsonseq"
PMTILES_OUT="$ROOT/pmtiles_per_year/crome_2023.pmtiles"
TIPPECANOE="/opt/homebrew/bin/tippecanoe"
PAGE_SIZE=5000
MAX_RETRIES=5

mkdir -p "$OUTDIR" "$ROOT/tmp_geojsonseq"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

if [ -f "$PMTILES_OUT" ]; then
  log "PMTiles already exists: $PMTILES_OUT"
  exit 0
fi

# Get layer list
log "Fetching layer list..."
caps=$(curl -sS --max-time 60 "${WFS_HTTP}?service=WFS&version=2.0.0&request=GetCapabilities")
layers=$(echo "$caps" | grep -o 'Crop_Map_of_England_2023_[A-Za-z_]*' | sort -u)
total=$(echo "$layers" | wc -l | tr -d ' ')
log "Found $total county layers"

done_count=0
failed_count=0

for layer in $layers; do
  out="$OUTDIR/${layer}.geojsonl"
  tmp="$out.tmp"

  if [ -f "$out" ]; then
    lines=$(wc -l < "$out" | tr -d ' ')
    log "SKIP $layer ($lines features)"
    done_count=$((done_count + 1))
    continue
  fi

  [ -f "$tmp" ] || : > "$tmp"
  start=$(wc -l < "$tmp" | tr -d ' ')

  log "FETCH $layer (startIndex=$start)"

  county_ok=1
  while true; do
    url="${WFS_HTTP}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${layer}&outputFormat=application/json&srsName=EPSG:4326&count=${PAGE_SIZE}&startIndex=${start}"

    got_json=0
    n=0
    for attempt in $(seq 1 "$MAX_RETRIES"); do
      resp=$(mktemp)
      curl -sS --max-time 120 "$url" -o "$resp"

      if jq -e '.features and (.features|type=="array")' "$resp" >/dev/null 2>&1; then
        n=$(jq '.features | length' "$resp")
        got_json=1
        break
      fi

      rm -f "$resp"
      log "  page startIndex=$start attempt=$attempt failed; retrying"
      sleep $((attempt * 2))
    done

    if [ "$got_json" -eq 0 ]; then
      log "  FAIL startIndex=$start (retries exhausted)"
      county_ok=0
      break
    fi

    if [ "$n" -eq 0 ]; then
      rm -f "$resp"
      break
    fi

    jq -c '.features[]' "$resp" >> "$tmp"
    rm -f "$resp"

    start=$((start + n))
    log "  fetched=$n total=$start"

    if [ "$n" -lt "$PAGE_SIZE" ]; then
      break
    fi
  done

  if [ "$county_ok" -eq 1 ]; then
    mv "$tmp" "$out"
    lines=$(wc -l < "$out" | tr -d ' ')
    size=$(ls -lh "$out" | awk '{print $5}')
    log "DONE $layer features=$lines size=$size"
    done_count=$((done_count + 1))
  else
    failed_count=$((failed_count + 1))
    log "INCOMPLETE $layer (can resume later)"
  fi
done

log "Download summary: done=$done_count failed=$failed_count total=$total"

if [ "$failed_count" -gt 0 ]; then
  log "Some counties failed — re-run this script to resume"
  exit 1
fi

# Combine all county files into one GeoJSONSeq
log "Combining county files..."
> "$GEOJSONSEQ"
for f in "$OUTDIR"/*.geojsonl; do
  # Extract just lucode, add year, output as GeoJSONSeq
  while IFS= read -r line; do
    echo "$line" | jq -c '{type:.type, geometry:.geometry, properties:{lucode:.properties.lucode, year:2023}}'
  done < "$f" >> "$GEOJSONSEQ"
done

count=$(wc -l < "$GEOJSONSEQ" | tr -d ' ')
size=$(ls -lh "$GEOJSONSEQ" | awk '{print $5}')
log "Combined: $count features ($size)"

# Build PMTiles
log "Building PMTiles..."
cat "$GEOJSONSEQ" | "$TIPPECANOE" \
  -o "$PMTILES_OUT" \
  -l crome -z12 \
  --coalesce-densest-as-needed --drop-densest-as-needed \
  --drop-rate=1 --hilbert \
  -y lucode -y year \
  --simplification=10 --force

if [ $? -eq 0 ] && [ -f "$PMTILES_OUT" ]; then
  size=$(ls -lh "$PMTILES_OUT" | awk '{print $5}')
  log "OK — PMTiles complete ($size)"
else
  log "ERROR: tippecanoe failed"
  exit 1
fi

# Cleanup
rm -f "$GEOJSONSEQ"
log "Done!"
