#!/usr/bin/env python3
"""Rename mask files from *_mask.tiff to *_segmentation_labels.tif for CellTune.

CellTune expects segmentation masks named: {IMAGE_NAME}_segmentation_labels.tif

This script copies (or symlinks) mask files from a source directory to an output
directory with the correct naming convention.

Usage:
    python rename_masks_for_celltune.py \
        --input_dir /path/to/cellmeasurement \
        --output_dir /path/to/output \
        --suffix _mask.tiff \
        [--symlink]
"""

import argparse
import glob
import os
import shutil
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Rename mask files to CellTune segmentation_labels format"
    )
    parser.add_argument("--input_dir", required=True,
                        help="Directory containing mask files")
    parser.add_argument("--output_dir", required=True,
                        help="Output directory for renamed masks")
    parser.add_argument("--suffix", default="_mask.tiff",
                        help="Current mask suffix to replace (default: _mask.tiff)")
    parser.add_argument("--symlink", action="store_true",
                        help="Create symlinks instead of copying files")
    args = parser.parse_args()

    mask_pattern = os.path.join(args.input_dir, f"*{args.suffix}")
    mask_files = sorted(glob.glob(mask_pattern))

    if not mask_files:
        print(f"ERROR: No files matching *{args.suffix} found in {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Found {len(mask_files)} mask files")
    print(f"  Renaming: *{args.suffix} -> *_segmentation_labels.tif")
    print(f"  Mode: {'symlink' if args.symlink else 'copy'}")

    for mask_path in mask_files:
        fname = os.path.basename(mask_path)
        # Replace the suffix: _mask.tiff -> _segmentation_labels.tif
        new_name = fname.replace(args.suffix, "_segmentation_labels.tif")
        new_path = os.path.join(args.output_dir, new_name)

        if args.symlink:
            abs_src = os.path.abspath(mask_path)
            if os.path.exists(new_path):
                os.remove(new_path)
            os.symlink(abs_src, new_path)
        else:
            shutil.copy2(mask_path, new_path)

        print(f"  {fname} -> {new_name}")

    print(f"\nDone: {len(mask_files)} masks renamed in {args.output_dir}")


if __name__ == "__main__":
    main()
