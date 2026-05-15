#!/bin/bash
# Download CROME 2017-2019 GDB files with resume support
set -uo pipefail

declare -A URLS
URLS[2017]="https://environment.data.gov.uk/api/file/download?fileDataSetId=63826b94-669a-4094-a206-470d399df3c8&fileName=Crop_Map_of_England_CROME_2017_Complete.gdb.zip"
URLS[2018]="https://environment.data.gov.uk/api/file/download?fileDataSetId=c369347b-53d4-4f92-821e-273089d773de&fileName=Crop_Map_of_England_CROME_2018.gdb.zip"
URLS[2019]="https://environment.data.gov.uk/api/file/download?fileDataSetId=1aaba8cc-9d93-480e-a29a-7a92324cc5b8&fileName=Crop_Map_of_England_CROME_2019.gdb.zip"

declare -A SIZES
SIZES[2017]=1367892300
SIZES[2018]=2300000000
SIZES[2019]=2430000000

for year in 2017 2018 2019; do
  out="gdb_downloads/crome_${year}.gdb.zip"
  url="${URLS[$year]}"
  echo "=== Downloading $year ==="

  for attempt in $(seq 1 20); do
    echo "  Attempt $attempt..."
    curl -L -C - --max-time 300 --retry 3 --retry-delay 5 -o "$out" "$url" 2>&1 | tail -1

    if [ -f "$out" ]; then
      actual=$(stat -f%z "$out" 2>/dev/null || stat -c%s "$out" 2>/dev/null)
      echo "  Downloaded: ${actual} bytes"
      # Check if zip is valid
      if unzip -t "$out" >/dev/null 2>&1; then
        echo "  $year: ZIP valid!"
        break
      fi
    fi
    sleep 2
  done
done

echo "=== Download complete ==="
ls -lh gdb_downloads/crome_201*.gdb.zip
