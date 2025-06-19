# bigscript

A scalable toolkit for fetching and extracting large sets of genome assemblies and metadata from the ATB project.

## Features
- Download and verify ATB tarball archives and metadata
- Extract assemblies by species or accession code
- Batch and parallel extraction with SLURM support
- Robust folder structure to avoid filesystem bottlenecks

## Installation

1. Clone the repository and set up a Python 3.8+ environment (preferably with `venv` or `conda`).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

All commands are available via the Typer CLI in `run.py`:

```bash
python run.py [COMMAND] [OPTIONS]
```

### Commands

#### Fetch ATB Metadata and Archives

```bash
python bigscript-run.py fetch-atb
```
Downloads the latest ATB file list and all referenced tarball archives, verifying checksums.

#### Extract Assemblies by Species

```bash
python bigscript-run.py get-species --species "Achromobacter xylosoxidans" --output-dir my_assemblies
```
Extracts all assemblies for the given species to the specified directory. Supports batching and SLURM job submission:

- `--batch` : Enable batch mode (SLURM job scripts)
- `--max-jobs N` : Maximum parallel jobs (default: 1000)
- `--submit` : Submit jobs to SLURM automatically

#### Extract Assemblies by Accession

```bash
python bigscript-run.py get-accessions "SAMN12335635,SAMN12335634" --output-dir my_assemblies
```
Extracts assemblies for the given accession codes (comma-separated).

### Options
- `--tsv-gz-path` : Path to the ATB file list TSV (default from config)
- `--archive-dir` : Path to the tarball archive directory (default from config)
- `--parquet-path` : Path to the Parquet file (default from config)

## Configuration

Paths and settings are managed in `bigscript/conf.py`.
