#!/usr/bin/env python3
"""Download CROME 2020 per-county FGDB files and combine into a single GeoJSONSeq stream."""
import subprocess, sys, os, glob, tempfile

COUNTIES = [
    "BED","BER","BSO","BUC","CAM","CHE","CMB","COR","DER","DEV","DOR","DUR",
    "ERY","ESS","ESX","GLO","GMN","HAM","HER","HRT","IOW","KEN","LAN","LEI",
    "LIN","LON","MER","NOR","NRM","NRT","NOT","NYO","OXF","RUT","SHR","STF",
    "SUF","SUR","SYO","TAW","WAR","WIL","WMD","WOR","WSX","WYR"
]

BASE = "https://environment.data.gov.uk/api/file/download?fileDataSetId=41adeeb3-89f1-4a86-8982-c2d27b5f3e6f&fileName="
WORKDIR = "/Users/plewis/Downloads/crome-work/counties"
os.makedirs(WORKDIR, exist_ok=True)

failed = []
for i, code in enumerate(COUNTIES):
    fname = f"CropMapOfEngland2020{code}-FGDB.zip"
    outpath = os.path.join(WORKDIR, fname)
    if os.path.exists(outpath) and os.path.getsize(outpath) > 1000:
        print(f"[{i+1}/{len(COUNTIES)}] {code}: already downloaded", flush=True)
        continue
    url = BASE + fname
    print(f"[{i+1}/{len(COUNTIES)}] Downloading {code}...", end=" ", flush=True)
    for attempt in range(3):
        r = subprocess.run(
            ["curl", "-sL", "--retry", "3", "--retry-delay", "5",
             "--max-time", "300", "-o", outpath, url],
            capture_output=True
        )
        if r.returncode == 0 and os.path.exists(outpath) and os.path.getsize(outpath) > 1000:
            sz = os.path.getsize(outpath)
            print(f"OK ({sz/1024/1024:.1f} MB)", flush=True)
            break
        print(f"retry {attempt+1}...", end=" ", flush=True)
    else:
        print(f"FAILED", flush=True)
        failed.append(code)

if failed:
    print(f"\nFailed counties: {failed}")
    sys.exit(1)
else:
    print(f"\nAll {len(COUNTIES)} counties downloaded successfully")
