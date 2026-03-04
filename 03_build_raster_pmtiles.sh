#!/bin/bash
# Build per-year raster PMTiles for CROME data
#
# Rasterizes vector CROME data to byte-indexed GeoTIFF (lucode category),
# then creates web-ready raster PMTiles via gdal2tiles + pmtiles convert.
#
# Requires: gdal_rasterize, gdalwarp, gdal_translate, gdal2tiles.py, pmtiles
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/crome_config.sh"

mkdir -p "$RASTER_DIR"

PIXEL_SIZE=20  # 20m resolution in BNG

# England extent in EPSG:27700 (British National Grid)
TE_XMIN=82000
TE_YMIN=5000
TE_XMAX=660000
TE_YMAX=660000

# ── Lucode category mapping ─────────────────────────────────────────
# Maps 2-char lucode prefix to a byte value (1-15) for rasterization.
# CROME lucode format: 2-letter category + 2-digit number (e.g., AC67, WO12)
# Special cases like "HEAT" are mapped by first 2 chars → HE.

# Category colors (R,G,B) for the RGBA colormap
# Based on standard UK land use classification colours
cat > "$RASTER_DIR/colormap.txt" <<'COLORTABLE'
0 0 0 0 0
1 255 255 0 255
2 255 170 0 255
3 180 120 60 255
4 200 200 100 255
5 170 255 0 255
6 0 168 0 255
7 255 85 255 255
8 140 70 20 255
9 0 100 0 255
10 0 112 255 255
11 190 190 190 255
12 255 0 0 255
13 128 128 0 255
14 224 224 224 255
15 100 0 100 255
COLORTABLE

# Category legend
cat > "$RASTER_DIR/lucode_categories.csv" <<'LEGEND'
id,prefix,description,color
1,AC,Arable Crops,#FFFF00
2,FA,Fallow Land,#FFAA00
3,HE,Heather,#B4783C
4,LG,Leguminous/Grass Mix,#C8C864
5,PG,Permanent Grassland,#AAFF00
6,WO,Woodland,#00A800
7,SR,Short Rotation Coppice,#FF55FF
8,TC,Trees/Crops,#8C4614
9,NA,Non-Agricultural,#006400
10,WA,Water,#0070FF
11,UR,Urban/Built-up,#BEBEBE
12,EN,Energy Crops,#FF0000
13,SH,Shrubland,#808000
14,UN,Unknown/Other,#E0E0E0
15,PE,Peat/Bog,#640064
LEGEND

# SQL CASE expression to map lucode prefix to category integer
LUCODE_SQL_CASE="CASE substr(lucode,1,2)
  WHEN 'AC' THEN 1
  WHEN 'FA' THEN 2
  WHEN 'HE' THEN 3
  WHEN 'LG' THEN 4
  WHEN 'PG' THEN 5
  WHEN 'WO' THEN 6
  WHEN 'SR' THEN 7
  WHEN 'TC' THEN 8
  WHEN 'NA' THEN 9
  WHEN 'WA' THEN 10
  WHEN 'UR' THEN 11
  WHEN 'EN' THEN 12
  WHEN 'SH' THEN 13
  WHEN 'UN' THEN 14
  WHEN 'PE' THEN 15
  ELSE 14
END"

# ── Raster processing per year ───────────────────────────────────────

process_raster_year() {
  local year=$1
  local pmtiles_out="$RASTER_DIR/crome_${year}.pmtiles"

  if [ -f "$pmtiles_out" ]; then
    log "SKIP raster $year — already exists"
    return 0
  fi

  local raw_tif="$RASTER_DIR/crome_${year}_raw.tif"
  local web_tif="$RASTER_DIR/crome_${year}_3857.tif"
  local rgba_tif="$RASTER_DIR/crome_${year}_rgba.tif"
  local mbtiles="$RASTER_DIR/crome_${year}.mbtiles"

  # ── Step 1: Rasterize vector data to byte GeoTIFF ──

  if [ ! -f "$raw_tif" ]; then
    log "$year — rasterizing to byte GeoTIFF (${PIXEL_SIZE}m)"

    case $year in
      2017|2018|2019)
        # Rasterize from GDB
        local gdb_zip="$GDB_DIR/$(gdb_file "$year")"
        if [ ! -f "$gdb_zip" ]; then
          log_err "$year — GDB not found: $gdb_zip"
          return 1
        fi

        local gdb_path="/vsizip/$gdb_zip"
        local layer
        layer=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | head -1 | awk '{print $2}')

        if [ -z "$layer" ]; then
          # Fallback: unzip
          local tmp_unzip="$RASTER_DIR/tmp_gdb_${year}"
          mkdir -p "$tmp_unzip"
          unzip -o -q "$gdb_zip" -d "$tmp_unzip"
          gdb_path=$(find "$tmp_unzip" -name "*.gdb" -type d | head -1)
          layer=$(ogrinfo -ro -so "$gdb_path" 2>/dev/null | grep -oE '[0-9]+: [^ ]+' | head -1 | awk '{print $2}')
        fi

        # Create temp GeoPackage with integer category field
        local tmp_gpkg="$RASTER_DIR/tmp_${year}.gpkg"
        log "$year — converting GDB to GPKG with category field"
        ogr2ogr -f GPKG "$tmp_gpkg" "$gdb_path" "$layer" \
          -sql "SELECT *, ${LUCODE_SQL_CASE} as lucat FROM \"${layer}\"" \
          -nln crome \
          -overwrite 2>/dev/null

        gdal_rasterize -a lucat -ot Byte \
          -tr "$PIXEL_SIZE" "$PIXEL_SIZE" \
          -te "$TE_XMIN" "$TE_YMIN" "$TE_XMAX" "$TE_YMAX" \
          -a_srs EPSG:27700 \
          -init 0 \
          -co COMPRESS=LZW \
          -co TILED=YES \
          -l crome \
          "$tmp_gpkg" "$raw_tif"

        rm -f "$tmp_gpkg"
        [ -d "$RASTER_DIR/tmp_gdb_${year}" ] && rm -rf "$RASTER_DIR/tmp_gdb_${year}"
        ;;
      *)
        # Rasterize from per-county GeoJSONSeq (if available from vector pipeline)
        local year_dir="$TMP_GEOJSONSEQ/$year"
        local geojsonseq_files
        geojsonseq_files=$(find "$year_dir" -name '*.geojsonseq' -type f 2>/dev/null | head -1)

        if [ -z "$geojsonseq_files" ]; then
          # Need to download from WFS first — create temp GPKG per county
          log "$year — downloading from WFS for rasterization"
          local tmp_gpkg="$RASTER_DIR/tmp_${year}.gpkg"
          rm -f "$tmp_gpkg"

          local wfs_url
          wfs_url="$(wfs_url_for_year "$year")"
          local layers
          layers=$(discover_wfs_layers "$year")

          local count=0
          local total
          total=$(echo "$layers" | wc -l | tr -d ' ')

          while IFS= read -r layer; do
            [ -n "$layer" ] || continue
            count=$((count + 1))
            log "  [$count/$total] $layer"

            for attempt in 1 2 3; do
              ogr2ogr -f GPKG "$tmp_gpkg" \
                "WFS:$wfs_url" "$layer" \
                -sql "SELECT *, ${LUCODE_SQL_CASE} as lucat FROM \"${layer}\"" \
                -nln crome \
                -append \
                --config OGR_WFS_PAGE_SIZE "$OGR_WFS_PAGE_SIZE" \
                2>/dev/null && break
              sleep $((attempt * 10))
            done
          done <<< "$layers"

          gdal_rasterize -a lucat -ot Byte \
            -tr "$PIXEL_SIZE" "$PIXEL_SIZE" \
            -te "$TE_XMIN" "$TE_YMIN" "$TE_XMAX" "$TE_YMAX" \
            -a_srs EPSG:27700 \
            -init 0 \
            -co COMPRESS=LZW \
            -co TILED=YES \
            -l crome \
            "$tmp_gpkg" "$raw_tif"

          rm -f "$tmp_gpkg"
        else
          # Use existing GeoJSONSeq — merge into temp GPKG with category field
          log "$year — rasterizing from existing GeoJSONSeq"
          local tmp_gpkg="$RASTER_DIR/tmp_${year}.gpkg"
          rm -f "$tmp_gpkg"

          for f in "$year_dir"/*.geojsonseq; do
            ogr2ogr -f GPKG "$tmp_gpkg" "$f" \
              -sql "SELECT *, ${LUCODE_SQL_CASE} as lucat FROM OGRGeoJSON" \
              -nln crome \
              -append \
              -t_srs EPSG:27700 \
              2>/dev/null
          done

          gdal_rasterize -a lucat -ot Byte \
            -tr "$PIXEL_SIZE" "$PIXEL_SIZE" \
            -te "$TE_XMIN" "$TE_YMIN" "$TE_XMAX" "$TE_YMAX" \
            -a_srs EPSG:27700 \
            -init 0 \
            -co COMPRESS=LZW \
            -co TILED=YES \
            -l crome \
            "$tmp_gpkg" "$raw_tif"

          rm -f "$tmp_gpkg"
        fi
        ;;
    esac

    if [ ! -f "$raw_tif" ]; then
      log_err "$year — rasterization failed"
      return 1
    fi
    log "$year — raw GeoTIFF: $(ls -lh "$raw_tif" | awk '{print $5}')"
  fi

  # ── Step 2: Reproject to EPSG:3857 ──

  if [ ! -f "$web_tif" ]; then
    log "$year — reprojecting to EPSG:3857"
    gdalwarp -s_srs EPSG:27700 -t_srs EPSG:3857 \
      -r near \
      -co COMPRESS=LZW \
      -co TILED=YES \
      "$raw_tif" "$web_tif"
  fi

  # ── Step 3: Apply colormap → RGBA ──

  if [ ! -f "$rgba_tif" ]; then
    log "$year — applying colormap"
    gdaldem color-relief "$web_tif" "$RASTER_DIR/colormap.txt" "$rgba_tif" \
      -alpha \
      -co COMPRESS=LZW \
      -co TILED=YES
  fi

  # ── Step 4: Generate tiles as MBTiles ──

  if [ ! -f "$mbtiles" ]; then
    log "$year — generating tiles (z0-14)"
    gdal2tiles.py \
      --processes=4 \
      -z 0-14 \
      --xyz \
      -w none \
      "$rgba_tif" "$RASTER_DIR/tiles_${year}"

    # Convert tile directory to MBTiles using Python
    # (gdal2tiles doesn't output MBTiles directly in GDAL 3.6)
    python3 -c "
import sqlite3, os, sys, glob

tiles_dir = '$RASTER_DIR/tiles_${year}'
mbtiles_path = '$mbtiles'

conn = sqlite3.connect(mbtiles_path)
conn.execute('CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)')
conn.execute('CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)')
conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row)')

conn.execute(\"INSERT OR REPLACE INTO metadata VALUES ('name', 'CROME ${year}')\")
conn.execute(\"INSERT OR REPLACE INTO metadata VALUES ('format', 'png')\")
conn.execute(\"INSERT OR REPLACE INTO metadata VALUES ('type', 'overlay')\")
conn.execute(\"INSERT OR REPLACE INTO metadata VALUES ('minzoom', '0')\")
conn.execute(\"INSERT OR REPLACE INTO metadata VALUES ('maxzoom', '14')\")

count = 0
for z_dir in sorted(glob.glob(os.path.join(tiles_dir, '[0-9]*'))):
    z = int(os.path.basename(z_dir))
    for x_dir in sorted(glob.glob(os.path.join(z_dir, '[0-9]*'))):
        x = int(os.path.basename(x_dir))
        for tile_file in sorted(glob.glob(os.path.join(x_dir, '*.png'))):
            y = int(os.path.splitext(os.path.basename(tile_file))[0])
            with open(tile_file, 'rb') as f:
                data = f.read()
            # XYZ to TMS: flip y
            tms_y = (2**z - 1) - y
            conn.execute('INSERT OR REPLACE INTO tiles VALUES (?,?,?,?)', (z, x, tms_y, data))
            count += 1
            if count % 10000 == 0:
                conn.commit()
                print(f'  {count} tiles...', file=sys.stderr)

conn.commit()
conn.close()
print(f'  Done: {count} tiles', file=sys.stderr)
"
    rm -rf "$RASTER_DIR/tiles_${year}"
  fi

  # ── Step 5: Convert MBTiles to PMTiles ──

  log "$year — converting to PMTiles"
  pmtiles convert "$mbtiles" "$pmtiles_out"

  if [ -f "$pmtiles_out" ]; then
    local size
    size=$(ls -lh "$pmtiles_out" | awk '{print $5}')
    log "OK raster $year — $size"

    # Clean up intermediates
    rm -f "$raw_tif" "$web_tif" "$rgba_tif" "$mbtiles"
  else
    log_err "$year — PMTiles conversion failed"
    return 1
  fi
}

# ── Main ─────────────────────────────────────────────────────────────

log "=== CROME Raster PMTiles Pipeline ==="
log "Resolution: ${PIXEL_SIZE}m | Zoom: 0-14"
log ""

for year in "${ALL_YEARS[@]}"; do
  log "=== Raster year $year ==="
  process_raster_year "$year" || log_err "$year raster FAILED"
done

log ""
log "=== Raster pipeline complete ==="
ls -lh "$RASTER_DIR"/*.pmtiles 2>/dev/null
