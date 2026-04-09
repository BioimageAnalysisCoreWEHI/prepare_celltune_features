#!/usr/bin/env python3
"""Apply arcsinh normalization to measurement columns in a CellTune parquet.

Applies arcsinh(x / cofactor) to all measurement columns.
Skips: fov, cellID, Area, Centroid X/Y, and any kronos_* columns.

Usage:
    python arcsinh_normalize.py \
        --input features.parquet \
        --output features_arcsinh.parquet \
        --cofactor 100
"""

import argparse
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import time

SKIP_COLUMNS = {
    'fov', 'cellID',
    'Area__Cell__RegionProps',
    'Centroid_X__Cell__RegionProps',
    'Centroid_Y__Cell__RegionProps',
}

SKIP_PREFIXES = ('kronos_',)


def _should_skip(name):
    """Check if a column should be skipped from normalization."""
    if name in SKIP_COLUMNS:
        return True
    return any(name.startswith(p) for p in SKIP_PREFIXES)


def main():
    parser = argparse.ArgumentParser(
        description="Apply arcsinh normalization to CellTune measurement columns"
    )
    parser.add_argument("--input", required=True, help="Input parquet file")
    parser.add_argument("--output", required=True, help="Output parquet file")
    parser.add_argument("--cofactor", type=float, default=100,
                        help="Cofactor for arcsinh(x/cofactor). 100 for MIBI/IMC, 0.1 for fluorescence (default: 100)")
    args = parser.parse_args()

    t0 = time.time()

    print(f"Reading {args.input}...")
    table = pq.read_table(args.input)
    print(f"  {table.num_rows} rows x {table.num_columns} columns")
    print(f"  Cofactor: {args.cofactor}")

    skip_cols = [c for c in table.column_names if _should_skip(c)]
    meas_cols = [c for c in table.column_names if not _should_skip(c)]
    print(f"  Normalizing {len(meas_cols)} measurement columns, skipping {len(skip_cols)}")
    kronos_skipped = [c for c in skip_cols if c.startswith('kronos_')]
    if kronos_skipped:
        print(f"  Skipping {len(kronos_skipped)} KRONOS embedding columns")

    new_columns = []
    for i, name in enumerate(table.column_names):
        col = table.column(name)
        if _should_skip(name):
            new_columns.append(col)
        else:
            arr = col.to_numpy().astype(np.float32)
            arr = np.arcsinh(arr / args.cofactor).astype(np.float32)
            new_columns.append(pa.array(arr, type=pa.float32()))

        if (i + 1) % 500 == 0:
            print(f"    {i + 1}/{table.num_columns} columns processed ({time.time() - t0:.0f}s)")

    print("Building table...")
    out_table = pa.table({name: col for name, col in zip(table.column_names, new_columns)})

    print(f"Writing {args.output}...")
    pq.write_table(out_table, args.output)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Output: {out_table.num_rows} rows x {out_table.num_columns} columns")


if __name__ == "__main__":
    main()
