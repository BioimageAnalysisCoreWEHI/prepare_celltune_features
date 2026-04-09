#!/usr/bin/env python3
"""Merge per-FOV parquet files into a single aggregated parquet.

Handles column alignment across FOVs (fills missing columns with NaN).
Uses pyarrow concat_tables for efficient zero-copy merging.

Usage:
    python merge_parquets.py \
        --input_dir /path/to/parquets \
        --output merged.parquet
"""

import argparse
import glob
import os
import sys
import time
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(
        description="Merge per-FOV parquet files into a single file"
    )
    parser.add_argument("--input_dir", required=True,
                        help="Directory containing per-FOV *.parquet files")
    parser.add_argument("--output", required=True,
                        help="Output merged parquet file path")
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

    print(f"Writing {args.output}...")
    sys.stdout.flush()
    pq.write_table(merged, args.output)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Output: {merged.num_rows} rows x {merged.num_columns} cols")
    print(f"  File size: {os.path.getsize(args.output) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
