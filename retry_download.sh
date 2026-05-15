#!/bin/bash
# Retry CROME 2020 GDB download every 10 minutes until success
OUTFILE="/Users/plewis/Downloads/crome-work/Crop_Map_of_England_CROME_2020.gdb.zip"
URL="https://environment.data.gov.uk/api/file/download?fileDataSetId=41adeeb3-89f1-4a86-8982-c2d27b5f3e6f&fileName=Crop_Map_of_England_CROME_2020.gdb.zip"
EXPECTED_MIN=2700000000  # ~2.6GB minimum

for attempt in $(seq 1 12); do
    echo "=== Attempt $attempt/12 at $(date) ==="
    rm -f "$OUTFILE"
    curl -L -o "$OUTFILE" --max-time 540 --speed-limit 5000 --speed-time 60 "$URL" 2>&1
    
    if [ -f "$OUTFILE" ]; then
        SIZE=$(stat -f%z "$OUTFILE" 2>/dev/null || stat -c%s "$OUTFILE" 2>/dev/null)
        echo "Downloaded: $SIZE bytes"
        if [ "$SIZE" -gt "$EXPECTED_MIN" ]; then
            echo "SUCCESS - download complete ($SIZE bytes)"
            exit 0
        else
            echo "File too small ($SIZE < $EXPECTED_MIN), will retry..."
        fi
    else
        echo "No file created, will retry..."
    fi
    
    if [ "$attempt" -lt 12 ]; then
        echo "Waiting 10 minutes before next attempt..."
        sleep 600
    fi
done

echo "FAILED after 12 attempts (2 hours)"
exit 1
