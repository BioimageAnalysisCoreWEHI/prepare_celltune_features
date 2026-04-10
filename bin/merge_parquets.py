#!/usr/bin/env python3
"""Merge per-FOV parquet files into a single aggregated parquet.

Handles column alignment across FOVs (fills missing columns with NaN).
Uses pyarrow concat_tables for efficient zero-copy merging.

Optionally compares against a CellTune cellTable_region_props.parquet to
identify and remove cells that CellTune cannot use (e.g. edge-clipped cells
whose polygon produces no pixels when rasterized). Dropped cells are written
to dropped_cells.csv alongside the merged output.

Usage:
    python merge_parquets.py \
        --input_dir /path/to/parquets \
        --output merged.parquet \
        [--celltune_cell_table /path/to/cellTable_region_props.parquet]
"""

import argparse
import glob
import os
import sys
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Merge per-FOV parquet files into a single file"
    )
    parser.add_argument("--input_dir", required=True,
                        help="Directory containing per-FOV *.parquet files")
    parser.add_argument("--output", required=True,
                        help="Output merged parquet file path")
    parser.add_argument("--celltune_cell_table", default=None,
                        help="Optional path to CellTune cellTable_region_props.parquet. "
                             "Cells absent from the reference are removed and recorded "
                             "in dropped_cells.csv.")
    args = parser.parse_args()

    parquet_files = sorted(glob.glob(os.path.join(args.input_dir, "*.parquet")))
    if not parquet_files:
        print(f"ERROR: No *.parquet files found in {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(parquet_files)} parquet files to merge")
    sys.stdout.flush()

    t0 = time.time()

    # First pass: collect all column names to build unified schema
    all_columns = set()
    for pf in parquet_files:
        schema = pq.read_schema(pf)
        all_columns.update(schema.names)

    # Define header columns in order
    header = [
        "fov", "cellID",
        "Area__Cell__RegionProps",
        "Centroid_X__Cell__RegionProps",
        "Centroid_Y__Cell__RegionProps",
    ]
    remaining = sorted(c for c in all_columns if c not in header)
    unified_columns = header + remaining
    print(f"  Unified schema: {len(unified_columns)} columns")
    sys.stdout.flush()

    # Second pass: read and align each file
    tables = []
    total_rows = 0
    for i, pf in enumerate(parquet_files):
        t = pq.read_table(pf)
        existing = set(t.column_names)

        # Add missing columns as null arrays
        for col in unified_columns:
            if col not in existing:
                null_arr = pa.nulls(t.num_rows, type=pa.float32())
                t = t.append_column(col, null_arr)

        # Reorder to unified column order
        t = t.select(unified_columns)
        tables.append(t)
        total_rows += t.num_rows

        if (i + 1) % 20 == 0 or i == len(parquet_files) - 1:
            print(f"  [{i+1}/{len(parquet_files)}] files loaded, {total_rows} total rows ({time.time()-t0:.0f}s)")
            sys.stdout.flush()

    # Concatenate all tables
    print("Concatenating tables...")
    sys.stdout.flush()
    merged = pa.concat_tables(tables)
    del tables

    # Optional: compare against CellTune reference and drop unmatched cells
    if args.celltune_cell_table:
        print(f"\nComparing against CellTune reference: {args.celltune_cell_table}")
        sys.stdout.flush()
        ref = pq.read_table(args.celltune_cell_table, columns=["fov", "cellID"]).to_pandas()
        merged_pd = merged.to_pandas()

        # Left-join to find which rows have a match in the reference
        ref["_keep"] = True
        merged_pd = merged_pd.merge(ref, on=["fov", "cellID"], how="left")
        keep_mask = merged_pd["_keep"].notna()
        dropped_pd = merged_pd[~keep_mask].drop(columns=["_keep"])
        merged_pd = merged_pd[keep_mask].drop(columns=["_keep"])

        if len(dropped_pd) > 0:
            print(f"  {len(dropped_pd)} cell(s) absent from CellTune reference (edge-clipped):")
            for _, row in dropped_pd.iterrows():
                cx = row.get("Centroid_X__Cell__RegionProps", float("nan"))
                cy = row.get("Centroid_Y__Cell__RegionProps", float("nan"))
                print(f"    {row['fov']}  cellID={row['cellID']}  centroid=({cx:.2f}, {cy:.2f}) µm")

            dropped_out = dropped_pd[
                ["fov", "cellID",
                 "Centroid_X__Cell__RegionProps",
                 "Centroid_Y__Cell__RegionProps"]
            ].copy()
            dropped_out.columns = ["fov", "cellID", "centroid_x_um", "centroid_y_um"]
            dropped_out.to_csv("dropped_cells.csv", index=False)
            print(f"  Written to dropped_cells.csv")
        else:
            print(f"  All {len(merged_pd)} cells match reference - no filtering needed")

        merged = pa.Table.from_pandas(merged_pd, preserve_index=False)
        sys.stdout.flush()

    print(f"\nWriting {args.output}...")
    sys.stdout.flush()
    pq.write_table(merged, args.output)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Output: {merged.num_rows} rows x {merged.num_columns} cols")
    print(f"  File size: {os.path.getsize(args.output) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
