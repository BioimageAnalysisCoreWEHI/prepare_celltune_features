nextflow.enable.dsl = 2

process EXTRACT_SINGLE_FOV {
    tag "${geojson_file.simpleName}"
    label 'process_medium'

    conda "${projectDir}/bin/environment.yml"

    input:
    tuple path(geojson_file), val(pixel_size), path(script_file)

    output:
    path "${geojson_file.simpleName}.parquet", emit: parquet

    script:
    """
    set -euo pipefail

    python "${script_file}" \
        --geojson "${geojson_file}" \
        --output "${geojson_file.simpleName}.parquet" \
        --pixel_size ${pixel_size}
    """
}

process MERGE_PARQUETS {
    tag "merge"
    label 'process_heavy'

    conda "${projectDir}/bin/environment.yml"

    publishDir "${params.outdir}", mode: params.publish_dir_mode, pattern: "dropped_cells.csv"

    input:
    tuple path('parquets/*'), val(celltune_cell_table), path(script_file)

    output:
    path 'merged.parquet', emit: parquet
    path 'merge.log', emit: log
    path 'dropped_cells.csv', emit: dropped, optional: true

    script:
    def cell_table_arg = celltune_cell_table ? "--celltune_cell_table \"${celltune_cell_table}\"" : ""
    """
    set -euo pipefail

    python "${script_file}" \
        --input_dir parquets \
        --output merged.parquet \
        ${cell_table_arg} \
        2>&1 | tee merge.log
    """
}

process ARCSINH_NORMALIZE {
    tag "arcsinh"
    label 'process_heavy'

    conda "${projectDir}/bin/environment.yml"

    publishDir "${params.outdir}", mode: params.publish_dir_mode

    input:
    tuple path(input_parquet), val(cofactor), val(output_name), path(script_file)

    output:
    path output_name, emit: parquet
    path 'arcsinh_normalize.log', emit: log

    script:
    """
    set -euo pipefail

    python "${script_file}" \
        --input "${input_parquet}" \
        --output "${output_name}" \
        --cofactor ${cofactor} \
        2>&1 | tee arcsinh_normalize.log
    """
}

process RENAME_MASKS {
    tag "rename_masks"
    label 'process_low'

    conda "${projectDir}/bin/environment.yml"

    publishDir "${params.outdir}/segmentation_labels", mode: params.publish_dir_mode

    input:
    tuple val(geojson_dir), val(mask_suffix), path(script_file)

    output:
    path '*_segmentation_labels.tif', emit: masks
    path 'rename_masks.log', emit: log

    script:
    """
    set -euo pipefail

    python "${script_file}" \
        --input_dir "${geojson_dir}" \
        --output_dir . \
        --suffix "${mask_suffix}" \
        2>&1 | tee rename_masks.log
    """
}

process PUBLISH_RAW {
    tag "publish_raw"

    publishDir "${params.outdir}", mode: params.publish_dir_mode

    input:
    tuple path(input_parquet), val(output_name)

    output:
    path output_name

    script:
    """
    cp "${input_parquet}" "${output_name}"
    """
}

workflow {
    if (!params.geojson_dir) {
        error "Missing required parameter: --geojson_dir"
    }
    if (params.pixel_size == null) {
        error "Missing required parameter: --pixel_size"
    }

    def geojsonDir = file(params.geojson_dir)
    if (!geojsonDir.exists() || !geojsonDir.isDirectory()) {
        error "geojson_dir does not exist or is not a directory: ${params.geojson_dir}"
    }

    def extractScript = file("${projectDir}/bin/extract_single_fov.py")
    if (!extractScript.exists()) {
        error "Extract script not found: ${extractScript}"
    }

    def mergeScript = file("${projectDir}/bin/merge_parquets.py")
    if (!mergeScript.exists()) {
        error "Merge script not found: ${mergeScript}"
    }

    def arcsinhScript = file("${projectDir}/bin/arcsinh_normalize.py")
    if (!arcsinhScript.exists()) {
        error "Arcsinh script not found: ${arcsinhScript}"
    }

    def renameScript = file("${projectDir}/bin/rename_masks_for_celltune.py")
    if (!renameScript.exists()) {
        error "Rename masks script not found: ${renameScript}"
    }

    def outputName = params.output.toString()

    // Rename masks for CellTune import (independent, runs in parallel)
    RENAME_MASKS(
        channel.of(tuple(
            geojsonDir.toString(),
            params.mask_suffix.toString(),
            renameScript
        ))
    )

    // Step 1: Extract measurements per FOV (parallel across all FOVs)
    Channel
        .fromPath("${params.geojson_dir}/*.geojson.gz")
        .ifEmpty { error "No *.geojson.gz files found in ${params.geojson_dir}" }
        .map { geojson -> tuple(geojson, params.pixel_size as double, extractScript) }
        .set { fov_ch }

    EXTRACT_SINGLE_FOV(fov_ch)

    // Step 2: Merge all per-FOV parquets
    MERGE_PARQUETS(
        EXTRACT_SINGLE_FOV.out.parquet
            .collect()
            .map { parquets -> tuple(parquets, params.celltune_cell_table ?: "", mergeScript) }
    )

    if (params.skip_arcsinh) {
        // Publish raw merged measurements
        PUBLISH_RAW(
            MERGE_PARQUETS.out.parquet.map { pq -> tuple(pq, outputName) }
        )
    } else {
        // Step 3: Apply arcsinh normalization
        ARCSINH_NORMALIZE(
            MERGE_PARQUETS.out.parquet.map { pq ->
                tuple(pq, params.cofactor as double, outputName, arcsinhScript)
            }
        )
    }
}
