#!/usr/bin/env python3
"""Extract cell measurements from geojson.gz files and aggregate into a parquet file.

Usage:
    python extract_geojson_measurements.py \
        --geojson_dir /path/to/cellmeasurement \
        --output measurements.parquet \
        --pixel_size 0.3906
"""

import argparse
import gzip
import json
import glob
import os
import sys
import time
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def polygon_centroid_np(coords_list):
    """Compute polygon centroid using numpy-vectorized shoelace formula."""
    v = np.array(coords_list, dtype=np.float64)
    x = v[:, 0]
    y = v[:, 1]
    x_next = np.roll(x, -1)
    y_next = np.roll(y, -1)
    cross = x * y_next - x_next * y
    area = cross.sum() / 2.0
    if abs(area) < 1e-10:
        return float(x.mean()), float(y.mean())
    cx = ((x + x_next) * cross).sum() / (6.0 * area)
    cy = ((y + y_next) * cross).sum() / (6.0 * area)
    return cx, cy


def rename_measurement(col):
    """Convert a QuPath measurement column name to CellTune double-underscore format."""
    parts = col.split(': ')

    # Handle Neighbor prefix
    neighbor_suffix = ''
    if parts[0] == 'Neighbors':
        neighbor_suffix = f'__Neighbor{parts[1]}'
        parts = parts[2:]

    # Morphological properties (start with "Cell:")
    if parts[0] == 'Cell':
        if len(parts) == 2:
            prop = parts[1].replace(' µm^2', '').replace(' µm', '').replace(' ', '_')
            return f"{prop}__Cell__RegionProps{neighbor_suffix}"
        elif len(parts) == 3:
            region, prop = parts[1], parts[2]
            return f"{prop}__{region}__RegionProps{neighbor_suffix}"

    # Marker-based measurements
    marker = parts[0]
    compartment = parts[1]
    stat = parts[-1]

    if len(parts) >= 4 and parts[-2] == 'Percentile':
        stat = f"Percentile_{stat}"
        extra = parts[2:-2]
    else:
        extra = parts[2:-1]

    if stat == 'Std.Dev.':
        stat = 'StdDev'

    result_parts = [marker, stat, compartment] + extra
    return '__'.join(result_parts) + neighbor_suffix


def main():
    parser = argparse.ArgumentParser(
        description="Extract cell measurements from geojson.gz files into a CellTune-format parquet"
    )
    parser.add_argument("--geojson_dir", required=True,
                        help="Directory containing *.geojson.gz files")
    parser.add_argument("--output", required=True,
                        help="Output parquet file path")
    parser.add_argument("--pixel_size", type=float, required=True,
                        help="Pixel size in µm/pixel for centroid conversion")
    args = parser.parse_args()

    geojson_files = sorted(glob.glob(os.path.join(args.geojson_dir, "*.geojson.gz")))
    if not geojson_files:
        print(f"ERROR: No *.geojson.gz files found in {args.geojson_dir}", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(geojson_files)} geojson.gz files")
    sys.stdout.flush()

    # Phase 1: Load all files and collect raw cell data
    all_images = []
    all_cx = []
    all_cy = []
    all_meas = []

    t0 = time.time()
    for i, filepath in enumerate(geojson_files):
        fname = os.path.basename(filepath)
        image_name = fname.replace(".geojson.gz", "")

        with gzip.open(filepath, "rt") as f:
            data = json.load(f)

        ncells = 0
        for feat in data["features"]:
            props = feat["properties"]
            if props.get("objectType") != "cell":
                continue

            geom = feat["geometry"]
            if geom["type"] == "Polygon":
                gx, gy = polygon_centroid_np(geom["coordinates"][0])
            elif geom["type"] == "MultiPolygon":
                largest = max(geom["coordinates"], key=lambda ring: len(ring[0]))
                gx, gy = polygon_centroid_np(largest[0])
            else:
                gx, gy = 0.0, 0.0

            all_images.append(image_name)
            # QuPath convention: Centroid X µm = geojson_y * pixel_size (swapped)
            all_cx.append(gy * args.pixel_size)
            all_cy.append(gx * args.pixel_size)
            all_meas.append(props.get("measurements", {}))
            ncells += 1

        del data
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(geojson_files)}] {fname}: {ncells} cells ({elapsed:.0f}s)")
        sys.stdout.flush()

    total = len(all_images)
    print(f"\nPhase 1 complete: {total} cells loaded in {time.time()-t0:.0f}s")
    sys.stdout.flush()

    # Phase 2: Collect all unique measurement keys and rename
    print("Collecting measurement keys...")
    all_keys = set()
    for m in all_meas:
        all_keys.update(m.keys())
    qupath_keys = sorted(all_keys)
    print(f"  {len(qupath_keys)} unique measurement columns")
    sys.stdout.flush()

    # Phase 3: Build cellID (1-based index per FOV)
    print("Computing cellID...")
    cell_ids = []
    current_fov = None
    counter = 0
    for img in all_images:
        if img != current_fov:
            current_fov = img
            counter = 1
        else:
            counter += 1
        cell_ids.append(counter)

    # Phase 4: Build columns with CellTune naming
    print("Building parquet columns...")
    sys.stdout.flush()

    columns = {
        "fov": all_images,
        "cellID": np.array(cell_ids, dtype=np.int32),
    }
    del all_images, cell_ids

    # Rename and build measurement columns
    t1 = time.time()
    renamed_cols = {}
    area_col = None
    cx_col = np.array(all_cx, dtype=np.float32)
    cy_col = np.array(all_cy, dtype=np.float32)
    del all_cx, all_cy

    for ki, key in enumerate(qupath_keys):
        new_name = rename_measurement(key)
        col = np.array([m.get(key, np.nan) for m in all_meas], dtype=np.float32)
        renamed_cols[new_name] = col
        if new_name == 'Area__Cell__RegionProps':
            area_col = new_name
        if (ki + 1) % 500 == 0:
            print(f"    {ki+1}/{len(qupath_keys)} columns built ({time.time()-t1:.0f}s)")
            sys.stdout.flush()

    del all_meas
    print(f"  All columns built in {time.time()-t1:.0f}s")
    sys.stdout.flush()

    # Assemble in CellTune order: fov, cellID, Area, CentroidX, CentroidY, then sorted measurements
    columns["Area__Cell__RegionProps"] = renamed_cols.pop("Area__Cell__RegionProps", np.full(total, np.nan, dtype=np.float32))
    columns["Centroid_X__Cell__RegionProps"] = cx_col
    columns["Centroid_Y__Cell__RegionProps"] = cy_col
    del cx_col, cy_col

    for name in sorted(renamed_cols.keys()):
        columns[name] = renamed_cols[name]
    del renamed_cols

    # Phase 5: Write parquet
    print("Writing parquet...")
    sys.stdout.flush()
    table = pa.table(columns)
    del columns
    pq.write_table(table, args.output)

    print(f"\nSaved to {args.output}")
    print(f"Shape: {table.num_rows} rows x {table.num_columns} cols")
    print(f"First 10 columns: {table.column_names[:10]}")
    print(f"File size: {os.path.getsize(args.output) / 1e6:.1f} MB")
    print(f"Total time: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
