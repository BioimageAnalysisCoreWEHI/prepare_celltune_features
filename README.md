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

- `--cofactor` — Arcsinh cofactor (default: `100`).
- `--skip_arcsinh` — Skip normalization, output raw measurements (default: `false`).
- `--mask_suffix` — Suffix of mask files to rename (default: `_mask.tiff`).
- `--output` — Output filename (default: `celltune_features.parquet`).
- `--outdir` — Published output directory (default: `results`).
- `--publish_dir_mode` — Nextflow publish mode (default: `copy`).

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

- `celltune_features.parquet` (or filename passed via `--output`)
- `segmentation_labels/` — directory with renamed mask files (`*_segmentation_labels.tif`)
- `extract_measurements.log`
- `arcsinh_normalize.log` (if normalization is not skipped)
- `rename_masks.log`
