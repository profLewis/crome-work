#!/bin/bash
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="$ROOT/counties_wfs_geojsonseq"
WFS_HTTP="https://environment.data.gov.uk/spatialdata/crop-map-of-england-2020/wfs"
CAPS_URL="${WFS_HTTP}?service=WFS&version=2.0.0&request=GetCapabilities"
PAGE_SIZE="${PAGE_SIZE:-5000}"
MAX_RETRIES="${MAX_RETRIES:-5}"
CURL_MAX_TIME="${CURL_MAX_TIME:-120}"

mkdir -p "$OUTDIR"

caps=$(curl -sS --max-time 60 "$CAPS_URL") || {
  echo "ERROR: failed to fetch GetCapabilities" >&2
  exit 1
}

echo "$caps" | grep -o 'Crop_Map_of_England_2020_[A-Za-z_]*' | sort -u > /tmp/crome_2020_layers.txt
TOTAL=$(wc -l < /tmp/crome_2020_layers.txt | tr -d ' ')

if [ "$TOTAL" -eq 0 ]; then
  echo "ERROR: no county layers discovered" >&2
  exit 1
fi

echo "Discovered layers: $TOTAL"

done_count=0
failed_count=0

while IFS= read -r layer; do
  [ -n "$layer" ] || continue

  out="$OUTDIR/${layer}.geojsonl"
  tmp="$out.tmp"

  if [ -f "$out" ]; then
    lines=$(wc -l < "$out" | tr -d ' ')
    echo "SKIP $layer (already complete: $lines features)"
    done_count=$((done_count + 1))
    continue
  fi

  [ -f "$tmp" ] || : > "$tmp"
  start=$(wc -l < "$tmp" | tr -d ' ')
  page=$((start / PAGE_SIZE))

  echo "FETCH $layer (resume startIndex=$start)"

  county_ok=1
  while true; do
    page=$((page + 1))
    url="${WFS_HTTP}?service=WFS&version=2.0.0&request=GetFeature&typeNames=${layer}&outputFormat=application/json&srsName=EPSG:4326&count=${PAGE_SIZE}&startIndex=${start}"

    got_json=0
    n=0
    for attempt in $(seq 1 "$MAX_RETRIES"); do
      resp=$(mktemp)
      curl -sS --max-time "$CURL_MAX_TIME" "$url" -o "$resp"

      if jq -e '.features and (.features|type=="array")' "$resp" >/dev/null 2>&1; then
        n=$(jq '.features | length' "$resp")
        got_json=1
        break
      fi

      rm -f "$resp"
      echo "  page=$page attempt=$attempt invalid response; retrying" >&2
      sleep 2
    done

    if [ "$got_json" -eq 0 ]; then
      echo "  FAIL page=$page startIndex=$start (retries exhausted)" >&2
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
    echo "  page=$page fetched=$n total=$start"

    if [ "$n" -lt "$PAGE_SIZE" ]; then
      break
    fi
  done

  if [ "$county_ok" -eq 1 ]; then
    mv "$tmp" "$out"
    lines=$(wc -l < "$out" | tr -d ' ')
    size=$(ls -lh "$out" | awk '{print $5}')
    echo "DONE $layer features=$lines size=$size"
    done_count=$((done_count + 1))
  else
    failed_count=$((failed_count + 1))
    echo "INCOMPLETE $layer (can resume later from $tmp)"
  fi

done < /tmp/crome_2020_layers.txt

echo "SUMMARY done=$done_count failed=$failed_count total=$TOTAL"
