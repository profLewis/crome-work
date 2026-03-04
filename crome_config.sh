#!/bin/bash
# Shared configuration for CROME processing pipeline

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Output directories ──────────────────────────────────────────────
GDB_DIR="$ROOT/gdb_downloads"
TMP_GEOJSONSEQ="$ROOT/tmp_geojsonseq"
PMTILES_PER_YEAR="$ROOT/pmtiles_per_year"
RASTER_DIR="$ROOT/raster_pmtiles"
FINAL_VECTOR="$ROOT/crome_2017_2024.pmtiles"

# ── GDB bulk downloads (2017-2019) ──────────────────────────────────
# Functions instead of associative arrays (bash 3.2 compat)

gdb_url() {
  case $1 in
    2017) echo "https://environment.data.gov.uk/api/file/download?fileDataSetId=63826b94-669a-4094-a206-470d399df3c8&fileName=Crop_Map_of_England_CROME_2017_Complete.gdb.zip" ;;
    2018) echo "https://environment.data.gov.uk/api/file/download?fileDataSetId=c369347b-53d4-4f92-821e-273089d773de&fileName=Crop_Map_of_England_CROME_2018.gdb.zip" ;;
    2019) echo "https://environment.data.gov.uk/api/file/download?fileDataSetId=1aaba8cc-9d93-480e-a29a-7a92324cc5b8&fileName=Crop_Map_of_England_CROME_2019.gdb.zip" ;;
  esac
}

gdb_file() {
  case $1 in
    2017) echo "Crop_Map_of_England_CROME_2017_Complete.gdb.zip" ;;
    2018) echo "Crop_Map_of_England_CROME_2018.gdb.zip" ;;
    2019) echo "Crop_Map_of_England_CROME_2019.gdb.zip" ;;
  esac
}

gdb_min_size() {
  case $1 in
    2017) echo 1300000000 ;;  # ~1.37 GB
    2018) echo 2400000000 ;;  # ~2.45 GB
    2019) echo 2400000000 ;;  # ~2.43 GB
  esac
}

YEARS_GDB=(2017 2018 2019)
YEARS_WFS=(2020 2021 2022 2023 2024)
ALL_YEARS=(2017 2018 2019 2020 2021 2022 2023 2024)

# ── WFS configuration ───────────────────────────────────────────────
WFS_BASE="https://environment.data.gov.uk/spatialdata/crop-map-of-england"
OGR_WFS_PAGE_SIZE=5000
WFS_MAX_RETRIES=3

# Canonical county names (lowercase connectors — works for all years except 2023)
COUNTIES=(
  Bedfordshire
  Berkshire
  Bristol_and_Somerset
  Buckinghamshire
  Cambridgeshire
  Cheshire
  City_and_Greater_London
  Cornwall
  Cumbria
  Derbyshire
  Devon
  Dorset
  Durham
  East_Riding_of_Yorkshire
  East_Sussex
  Essex
  Gloucestershire
  Greater_Manchester
  Hampshire
  Herefordshire
  Hertfordshire
  Isle_of_Wight
  Kent
  Lancashire
  Leicestershire
  Lincolnshire
  Merseyside
  Norfolk
  North_Yorkshire
  Northamptonshire
  Northumberland
  Nottinghamshire
  Oxfordshire
  Rutland
  Shropshire
  South_Yorkshire
  Staffordshire
  Suffolk
  Surrey
  Tyne_and_Wear
  Warwickshire
  West_Midlands
  West_Sussex
  West_Yorkshire
  Wiltshire
  Worcestershire
)

# ── Tippecanoe settings ─────────────────────────────────────────────
TIPPECANOE_OPTS=(
  -l crome
  -z12
  --coalesce-densest-as-needed
  --drop-densest-as-needed
  --drop-rate=1
  --hilbert
  -y lucode -y year
  --simplification=10
  --force
)

# ── Functions ────────────────────────────────────────────────────────

wfs_url_for_year() {
  local year=$1
  echo "${WFS_BASE}-${year}/wfs"
}

# Discover actual WFS layer names for a given year from GetCapabilities.
# Writes layer names to stdout, one per line.
discover_wfs_layers() {
  local year=$1
  local wfs_url
  wfs_url="$(wfs_url_for_year "$year")"
  local caps_url="${wfs_url}?service=WFS&version=2.0.0&request=GetCapabilities"

  local caps
  caps=$(curl -sS --max-time 60 "$caps_url") || {
    echo "ERROR: failed to fetch GetCapabilities for $year" >&2
    return 1
  }

  # Extract county-level layers (skip national layer which has no county suffix)
  echo "$caps" | grep -oE "Crop_Map_[Oo]f_England_${year}_[A-Za-z_]+" | sort -u
}

# Get the WFS layer name for a specific year+county.
# Uses GetCapabilities cache if available, otherwise constructs the name.
get_layer_name() {
  local year=$1
  local county=$2  # canonical name with lowercase connectors

  # For 2023, the actual layer names have title-case connectors for some counties.
  # Rather than hardcoding exceptions, try the canonical name first and fall back.
  echo "Crop_Map_of_England_${year}_${county}"
}

log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

log_err() {
  echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2
}
