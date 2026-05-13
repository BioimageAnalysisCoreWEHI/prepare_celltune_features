# prepare_celltune_features

Nextflow pipeline to extract cell measurements from QuPath geojson.gz files and format them for [CellTune](https://celltune.org/) import.

## What it does

1. **Extract measurements** — Reads all `*.geojson.gz` files from a cell measurement directory, extracts per-cell measurements, computes centroids, and assigns 1-based `cellID` per FOV. Columns are renamed from QuPath colon format (`Marker: Cell: Mean`) to CellTune double-underscore format (`Marker__Mean__Cell`).

2. **Arcsinh normalization** (optional) — Applies `arcsinh(x / cofactor)` to all measurement columns. Skips `fov`, `cellID`, `Area`, `Centroid`, and any `kronos_*` embedding columns. CellTune recommends cofactor=100 for mass-based imaging (MIBI/IMC) and cofactor=0.1 for fluorescence (CODEX/COMET).

3. **Rename masks** — Copies mask files from `*_mask.tiff` (or custom suffix) to `*_segmentation_labels.tif` for CellTune import.

## Output format

The output parquet follows the CellTune `(fov, cellID)` import format:

| Column | Description |
|--------|-------------|
| `fov` | Image/FOV name |
| `cellID` | 1-based cell index within each FOV |
| `Area__Cell__RegionProps` | Cell area in µm² |
| `Centroid_X__Cell__RegionProps` | Cell centroid X in µm |
| `Centroid_Y__Cell__RegionProps` | Cell centroid Y in µm |
| `Marker__Stat__Compartment[__Region][__NeighborMean]` | Measurement features |

## Required parameters

- `--geojson_dir` — Directory containing `*.geojson.gz` files.
- `--pixel_size` — Pixel size in µm/pixel (e.g. `0.390625` for MIBI, `0.28` for COMET, `0.4964` for OPAL).

## Optional parameters

- `--cofactor` — Arcsinh cofactor for normalization (default: `100`). Use `0.1` for fluorescence (CODEX/COMET), `100` for mass-based imaging (MIBI/IMC).
- `--skip_arcsinh` — If specified, skips arcsinh normalization and outputs raw measurements (default: `false`).
- `--mask_suffix` — Suffix of mask files to rename to CellTune format (default: `_mask.tiff`).
- `--output` — Output parquet file name (default: `celltune_features.parquet`).
- `--outdir` — Output directory for published files (default: `results`).
- `--publish_dir_mode` — Method used by Nextflow publishDir (default: `copy`). Options: `symlink`, `rellink`, `link`, `copy`, `copyNoFollow`, `move`.
- `--celltune_cell_table` — Optional path to a CellTune `cellTable_region_props.parquet` file. If provided, only cells present in this table are retained; dropped cells are recorded in `dropped_cells.csv`.
- `--validate_params` — Validate parameters against the schema (default: `true`).
- `--help` — Show help and exit.
- `--version` — Show pipeline version and exit.

## Usage

### Local (Conda)

```bash
nextflow run main.nf \
    -profile conda \
    --geojson_dir /path/to/cellmeasurement \
    --pixel_size 0.3906 \
    --cofactor 100 \
    --output celltune_features.parquet \
    --outdir results
```

### Skip arcsinh normalization

```bash
nextflow run main.nf \
    -profile conda \
    --geojson_dir /path/to/cellmeasurement \
    --pixel_size 0.3906 \
    --skip_arcsinh \
    --output celltune_features_raw.parquet \
    --outdir results
```

### HPC (Slurm + Conda)

```bash
nextflow run main.nf \
    -profile conda,large \
    --geojson_dir /path/to/cellmeasurement \
    --pixel_size 0.3906 \
    --cofactor 100 \
    --output celltune_features.parquet \
    --outdir /path/to/output
```

## Outputs

The pipeline produces the following outputs:

- `celltune_features.parquet` (or filename passed via `--output`): Main feature table for CellTune import.
- `segmentation_labels/` — Directory containing renamed mask files (`*_segmentation_labels.tif`).
- `extract_measurements.log` — Log from the extraction step.
- `arcsinh_normalize.log` — Log from normalization (if not skipped).
- `rename_masks.log` — Log from mask renaming.
- `dropped_cells.csv` — List of cells dropped if `--celltune_cell_table` is used.
# Pipeline steps

1. **Extract measurements**: For each `*.geojson.gz` file, extracts per-cell measurements and computes centroids using `extract_geojson_measurements.py`.
2. **Merge**: Combines all per-FOV parquet files into a single table. If `--celltune_cell_table` is provided, only cells present in the reference are kept.
3. **Arcsinh normalization**: (unless `--skip_arcsinh` is set) Applies arcsinh transformation to measurement columns.
4. **Rename masks**: Copies mask files with the specified suffix to CellTune-compatible names in the output directory.

# Parameter details

| Parameter              | Required | Default                   | Description |
|------------------------|----------|---------------------------|-------------|
| `--geojson_dir`        | Yes      | —                         | Directory containing `*.geojson.gz` files |
| `--pixel_size`         | Yes      | —                         | Pixel size in µm/pixel |
| `--cofactor`           | No       | `100`                     | Arcsinh cofactor |
| `--skip_arcsinh`       | No       | `false`                   | Skip normalization |
| `--mask_suffix`        | No       | `_mask.tiff`              | Mask file suffix |
| `--output`             | No       | `celltune_features.parquet` | Output parquet filename |
| `--outdir`             | No       | `results`                 | Output directory |
| `--publish_dir_mode`   | No       | `copy`                    | Nextflow publish mode |
| `--celltune_cell_table`| No       | —                         | Reference cell table for filtering |
| `--validate_params`    | No       | `true`                    | Validate parameters against schema |
| `--help`               | No       | —                         | Show help and exit |
| `--version`            | No       | —                         | Show pipeline version and exit |

# Example commands

See above for local and HPC usage examples. For full parameter documentation, see `nextflow_schema.json`.
