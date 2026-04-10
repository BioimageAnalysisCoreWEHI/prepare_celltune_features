#!/usr/bin/env python3
"""Extract cell measurements from a single geojson.gz file into a parquet.

Processes one FOV at a time for parallel execution via Nextflow.
Memory bounded to a single file (~5-7K cells instead of ~375K).

Usage:
    python extract_single_fov.py \
        --geojson /path/to/fov.geojson.gz \
        --output fov.parquet \
        --pixel_size 0.3906
"""

import argparse
import gzip
import json
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

    neighbor_suffix = ''
    if parts[0] == 'Neighbors':
        neighbor_suffix = f'__Neighbor{parts[1]}'
        parts = parts[2:]

    if parts[0] == 'Cell':
        if len(parts) == 2:
            prop = parts[1].replace(' µm^2', '').replace(' µm', '').replace(' ', '_')
            return f"{prop}__Cell__RegionProps{neighbor_suffix}"
        elif len(parts) == 3:
            region, prop = parts[1], parts[2]
            return f"{prop}__{region}__RegionProps{neighbor_suffix}"

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
        description="Extract cell measurements from a single geojson.gz file"
    )
    parser.add_argument("--geojson", required=True,
                        help="Path to a single *.geojson.gz file")
    parser.add_argument("--output", required=True,
                        help="Output parquet file path")
    parser.add_argument("--pixel_size", type=float, required=True,
                        help="Pixel size in µm/pixel for centroid conversion")
    args = parser.parse_args()

    t0 = time.time()
    fname = os.path.basename(args.geojson)
    image_name = fname.replace(".geojson.gz", "")

    print(f"Processing {fname}...")
    sys.stdout.flush()

    with gzip.open(args.geojson, "rt") as f:
        data = json.load(f)

    # Single pass: extract all cell data and build columns simultaneously
    cells_cx = []
    cells_cy = []
    cells_meas = []
    cells_id = []

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

        # QuPath convention: Centroid X µm = geojson_y * pixel_size (swapped)
        cells_cx.append(gy * args.pixel_size)
        cells_cy.append(gx * args.pixel_size)
        cells_meas.append(props.get("measurements", {}))
        cells_id.append(int(props.get("id", 0)))

    del data
    ncells = len(cells_cx)
    print(f"  {ncells} cells extracted")

    if ncells == 0:
        print("  WARNING: No cells found, writing empty parquet")
        table = pa.table({"fov": [], "cellID": pa.array([], type=pa.int32())})
        pq.write_table(table, args.output)
        return

    # Collect all measurement keys from this FOV
    all_keys = set()
    for m in cells_meas:
        all_keys.update(m.keys())
    qupath_keys = sorted(all_keys)

    # Build all columns in a single pass over measurement dicts
    # Pre-allocate arrays for each column
    n_meas = len(qupath_keys)
    meas_arrays = np.full((n_meas, ncells), np.nan, dtype=np.float32)

    key_to_idx = {k: i for i, k in enumerate(qupath_keys)}
    for cell_idx, m in enumerate(cells_meas):
        for k, v in m.items():
            meas_arrays[key_to_idx[k], cell_idx] = v

    del cells_meas

    # Assemble columns in CellTune order
    columns = {
        "fov": [image_name] * ncells,
        "cellID": np.array(cells_id, dtype=np.uint16),
    }
    del cells_id

    # Rename and add measurement columns
    renamed = {}
    for ki, key in enumerate(qupath_keys):
        new_name = rename_measurement(key)
        renamed[new_name] = meas_arrays[ki]
    del meas_arrays

    # Add Area and centroids in header position
    columns["Area__Cell__RegionProps"] = renamed.pop(
        "Area__Cell__RegionProps", np.full(ncells, np.nan, dtype=np.float32)
    )
    columns["Centroid_X__Cell__RegionProps"] = np.array(cells_cx, dtype=np.float32)
    columns["Centroid_Y__Cell__RegionProps"] = np.array(cells_cy, dtype=np.float32)
    del cells_cx, cells_cy

    # Add remaining measurement columns sorted
    for name in sorted(renamed.keys()):
        columns[name] = renamed[name]
    del renamed

    # Write parquet
    table = pa.table(columns)
    del columns
    pq.write_table(table, args.output)

    elapsed = time.time() - t0
    print(f"  Saved {args.output}: {table.num_rows} rows x {table.num_columns} cols ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()
