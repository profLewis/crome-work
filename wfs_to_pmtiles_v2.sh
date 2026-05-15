#!/bin/bash
set -uo pipefail

WFS="WFS:https://environment.data.gov.uk/spatialdata/crop-map-of-england-2020/wfs"
OUTFILE="/Users/plewis/Downloads/crome-maps/crome2020_new.pmtiles"
FIFO="/Users/plewis/Downloads/crome-work/geojson_fifo"

COUNTIES=(
  Crop_Map_of_England_2020_Bedfordshire
  Crop_Map_of_England_2020_Berkshire
  Crop_Map_of_England_2020_Bristol_and_Somerset
  Crop_Map_of_England_2020_Buckinghamshire
  Crop_Map_of_England_2020_Cambridgeshire
  Crop_Map_of_England_2020_Cheshire
  Crop_Map_of_England_2020_City_and_Greater_London
  Crop_Map_of_England_2020_Cornwall
  Crop_Map_of_England_2020_Cumbria
  Crop_Map_of_England_2020_Derbyshire
  Crop_Map_of_England_2020_Devon
  Crop_Map_of_England_2020_Dorset
  Crop_Map_of_England_2020_Durham
  Crop_Map_of_England_2020_East_Riding_of_Yorkshire
  Crop_Map_of_England_2020_East_Sussex
  Crop_Map_of_England_2020_Essex
  Crop_Map_of_England_2020_Gloucestershire
  Crop_Map_of_England_2020_Greater_Manchester
  Crop_Map_of_England_2020_Hampshire
  Crop_Map_of_England_2020_Herefordshire
  Crop_Map_of_England_2020_Hertfordshire
  Crop_Map_of_England_2020_Isle_of_Wight
  Crop_Map_of_England_2020_Kent
  Crop_Map_of_England_2020_Lancashire
  Crop_Map_of_England_2020_Leicestershire
  Crop_Map_of_England_2020_Lincolnshire
  Crop_Map_of_England_2020_Merseyside
  Crop_Map_of_England_2020_Norfolk
  Crop_Map_of_England_2020_North_Yorkshire
  Crop_Map_of_England_2020_Northamptonshire
  Crop_Map_of_England_2020_Northumberland
  Crop_Map_of_England_2020_Nottinghamshire
  Crop_Map_of_England_2020_Oxfordshire
  Crop_Map_of_England_2020_Rutland
  Crop_Map_of_England_2020_Shropshire
  Crop_Map_of_England_2020_South_Yorkshire
  Crop_Map_of_England_2020_Staffordshire
  Crop_Map_of_England_2020_Suffolk
  Crop_Map_of_England_2020_Surrey
  Crop_Map_of_England_2020_Tyne_and_Wear
  Crop_Map_of_England_2020_Warwickshire
  Crop_Map_of_England_2020_West_Midlands
  Crop_Map_of_England_2020_West_Sussex
  Crop_Map_of_England_2020_West_Yorkshire
  Crop_Map_of_England_2020_Wiltshire
  Crop_Map_of_England_2020_Worcestershire
)

rm -f "$FIFO"
mkfifo "$FIFO"

# Tippecanoe reads from the FIFO in background
# Key changes from v1:
#   --coalesce-densest-as-needed: merges adjacent same-type polygons per tile
#   --simplification=10: reduces vertex count on hex boundaries
tippecanoe \
  -o "$OUTFILE" \
  -l crome2020 \
  -z12 \
  --coalesce-densest-as-needed \
  --drop-densest-as-needed \
  --drop-rate=1 \
  --hilbert \
  -y lucode \
  --simplification=10 \
  --force \
  < "$FIFO" &
TIPPECANOE_PID=$!

MAX_RETRIES=3

# Stream all counties into the FIFO
# Individual county failures are logged but don't abort the pipeline
(
  TOTAL=${#COUNTIES[@]}
  COUNT=0
  FAILED=0
  for county in "${COUNTIES[@]}"; do
    COUNT=$((COUNT + 1))
    echo "[$COUNT/$TOTAL] Fetching $county..." >&2
    ok=0
    for attempt in $(seq 1 $MAX_RETRIES); do
      if ogr2ogr -f GeoJSONSeq /vsistdout/ "$WFS" "$county" \
        -t_srs EPSG:4326 -select lucode 2>/dev/null; then
        ok=1
        break
      fi
      echo "  Attempt $attempt/$MAX_RETRIES failed, retrying in 5s..." >&2
      sleep 5
    done
    if [ $ok -eq 0 ]; then
      echo "  WARNING: $county FAILED after $MAX_RETRIES attempts, skipping" >&2
      FAILED=$((FAILED + 1))
    fi
  done
  echo "=== Streaming complete: $COUNT counties attempted, $FAILED failed ===" >&2
) > "$FIFO"

echo "Waiting for tippecanoe to finish..."
wait $TIPPECANOE_PID
STATUS=$?

rm -f "$FIFO"

if [ $STATUS -eq 0 ]; then
  echo "=== DONE ==="
  ls -lh "$OUTFILE"
else
  echo "=== TIPPECANOE FAILED with exit code $STATUS ==="
fi
