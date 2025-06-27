
"""
Module: get_assembly

This module provides functions to extract genome assembly files for specific species or accession codes
from compressed tar.xz archives, supporting both single-threaded and batch (SLURM) extraction modes.

Functions:
    extract_species(
    ) -> None
        Given a species name, ensures a Parquet file exists for the file list, loads it, finds all paths
        for the species, and extracts them to the output directory using tar. Supports batch extraction
        via SLURM job scripts.

    ) -> None
        Given a list of accession codes, ensures a Parquet file exists for the file list, loads it,
        finds all paths for the accessions, and extracts them to the output directory using tar.
        Supports both single-threaded and batch (SLURM) extraction.

    submit_jobs(slurm_scripts: List[str]) -> None
        Submits a list of SLURM job scripts for batch extraction, tracks job IDs, and writes a summary
        CSV file with job IDs and script paths.

    create_folder_structure(accession: str, base_dir: str) -> str
        Creates a nested folder structure based on the accession code to avoid too many files in a single
        directory. Returns the final directory path.

    extract_single_thread(row, archive_dir: str, output_dir: str) -> None
        Extracts a single file from a tar.xz archive in a single-threaded manner.

Dependencies:
    - pandas (for DataFrame operations)
    - bigscript.conf (for configuration paths)
    - bigscript.util (for Parquet file list loading)
    - SLURM (for batch job submission)
    - tar, xz, pigz (for file extraction and compression)
"""
import os
import subprocess
from typing import List
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from bigscript.conf import FILE_LIST_PATH, ATB_ARCHIVE_PATH, PQ_FILE_LIST_PATH, TEMP_DIR
from bigscript.util import get_parquet_file_list

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def extract_species(
    species: str,
    output_dir: str,
    batch: bool = False,
    max_jobs: int = 1000,    
    submit: bool = False,
    tsv_gz_path: str = FILE_LIST_PATH,
    archive_dir: str = ATB_ARCHIVE_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH,
) -> None:
    """    Given a species name, ensure a Parquet file exists for the file list,
    load it, find all paths for the species, and extract them to the output dir using tar.
    """
    os.makedirs(output_dir, exist_ok=True)
    df = get_parquet_file_list(tsv_gz_path, parquet_path)
    filtered = df[df['species_sylph'] == species]
    # If no files found for the species, raise an error
    if filtered.empty:
        raise ValueError(f"No files found for species: {species}")
    logging.info("Found %d files for species: %s", len(filtered), species)
    # Extract the accessions from the filtered DataFrames
    logging.info("Extracting files for species: %s", species)
    extract_accessions(
        filtered['sample'].tolist(),
        output_dir,
        batch=batch,
        max_jobs=max_jobs,
        submit=submit,
        tsv_gz_path=tsv_gz_path,
        archive_dir=archive_dir,
        parquet_path=parquet_path)

def extract_accessions(
    accession_codes: List[str],
    output_dir: str,
    batch: bool = False,
    max_jobs: int = 1000,
    submit: bool = False,
    tsv_gz_path: str = FILE_LIST_PATH,    
    archive_dir: str = ATB_ARCHIVE_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH
) -> None:
    """
    Given a list of accession codes, ensure a Parquet file exists for the file list,
    load it, find all paths for the accessions, and extract them to the output dir using tar.
    Args:
        accession_codes: List of accession codes to extract.
        parquet_path: Path to the Parquet file to use/create.
        tsv_gz_path: Path to the gzipped TSV file.
        output_dir: Directory to extract files to.
        archive_dir: Directory containing the tar.xz archives.
    """
    os.makedirs(output_dir, exist_ok=True)
    df = get_parquet_file_list(tsv_gz_path, parquet_path)
    # Filter for accessions
    filtered = df[df['sample'].isin(accession_codes)]
    # if batch is True, process in batches as a slurm job
    if batch:
        logging.info("Batch processing enabled. Creating job scripts to process %d accessions.", len(filtered))
        # Create a script to run the extraction in batches      
        # xz still has to read through the entire tarball, so we need to ensure that we:
        # 1. Figure out all the tarballs that need to be extracted
        tarball_to_files = (
            filtered
            .groupby('tar_xz')['filename_in_tar_xz']
            .apply(list)
            .to_dict()
        )

        # 3. Put all this into a set of jobs to run in parallel as slurm jobs
        slurm_scripts = []
        os.makedirs(TEMP_DIR, exist_ok=True)
        log_dir = os.path.join(output_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        job_dir = os.path.join(output_dir, "jobs")
        os.makedirs(job_dir, exist_ok=True)
        for idx, (tarball, files) in enumerate(tarball_to_files.items()):
            tarball_path = os.path.join(archive_dir, tarball)
            script_path = os.path.join(job_dir, f'extract_{idx}.slurm')
            strip_components = files[0].count(os.sep)  # Number of leading directories to strip            
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write("#!/bin/bash\n")
                f.write(f"#SBATCH --job-name=extract_{idx}\n")
                f.write(f"#SBATCH --output={log_dir}/extract_{idx}.out\n")
                f.write(f"#SBATCH --error={log_dir}/extract_{idx}.err\n")
                f.write("#SBATCH --time=02:00:00\n")
                f.write("#SBATCH --mem=4G\n")
                f.write("#SBATCH --cpus-per-task=4\n\n")
                f.write("set -e\n\n")
                # Extract only the needed files from the tarball
                files_args = " ".join([f"'{file}'" for file in files])
                f.write(
                    f"tar --use-compress-program='xz -T4' -xvf {tarball_path} "
                    f"--strip-components={strip_components} -C {TEMP_DIR} {files_args}\n"
                )
                for filename in files:
                    final_path = create_folder_structure(os.path.basename(filename).split('.')[0], output_dir)
                    f.write(f"mv {TEMP_DIR}/{os.path.basename(filename)} {final_path}/\n")
                    # gzip the file 
                    f.write(f"pigz -p 4 {final_path}/{os.path.basename(filename)}\n")
            os.chmod(script_path, 0o755)
            slurm_scripts.append(script_path)
            logging.info("SLURM extraction script created at %s", script_path)
        # Optionally submit jobs, but do not exceed max_jobs running at once
        if submit:
            if len(slurm_scripts) > max_jobs:
                logging.warning("More than %d jobs to submit.", max_jobs)
                raise ValueError("Too many jobs to submit at once. Please reduce the number of accessions or increase max_jobs.")
            logging.info("Submitting %d SLURM jobs.", len(slurm_scripts))
            submit_jobs(slurm_scripts)
    else:
        for _, row in filtered.iterrows():
            extract_single_thread(row, archive_dir=archive_dir, output_dir=output_dir)


def submit_jobs(slurm_scripts: List[str]) -> None:
    running_jobs = {}
    for script in slurm_scripts:
        sbatch_proc = subprocess.run(["sbatch", script], capture_output=True, text=True, check=False)
        if sbatch_proc.returncode == 0:
            # Extract job ID from output
            for part in sbatch_proc.stdout.split():
                if part.isdigit():
                    running_jobs[int(part)] = script
                    break
            logging.info("Submitted SLURM job for %s: %s", script, sbatch_proc.stdout.strip())
        else:
            logging.error("Failed to submit SLURM job for %s: %s", script, sbatch_proc.stderr.strip())
    # write a table to outdir with the job IDs and script names
    job_table_path = os.path.join(os.path.dirname(slurm_scripts[0]), "submitted_jobs.csv")
    with open(job_table_path, 'w', encoding='utf-8') as f:
        f.write("job_id,script\n")
        for job_id, script in running_jobs.items():
            f.write(f"{job_id},{script}\n")
        logging.info("Submitted jobs written to %s", job_table_path)
        logging.info("You can review the status of these jobs using the job_status command.")
        logging.info("python bigscript-run.py job-status %s", job_table_path)

def create_folder_structure(accession: str, base_dir: str) -> str:
    """
    Create a nested folder structure based on the accession code to avoid too many files in a single directory.
    The structure will be: <base_dir>/<all but last 8>/<all but last 4>/
    Returns the final directory path.
    """
    if len(accession) < 8:
        raise ValueError("Accession code must be at least 8 characters long for folder structure.")
    first = accession[:-6]
    second = accession[:-3]
    dir_path = os.path.join(base_dir, first, second)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path

def extract_single_thread(row, archive_dir: str, output_dir: str) -> None:
    """
    Extract files (one row) from tarballs in a single-threaded manner.
    """
    tarball = row['tar_xz']
    filename_in_tar = row['filename_in_tar_xz']
    tarball_path = os.path.join(archive_dir, tarball)
    if not os.path.exists(tarball_path):
        raise FileNotFoundError(f"Tarball {tarball_path} does not exist. Please ensure it is downloaded. Use the fetch_atb command to download it.")
    # Extract to a flat output directory (no subdirectories)
    cmd = [
        'tar',
        '-xvf', tarball_path,
        '-C', output_dir,
        filename_in_tar,
        '--strip-components', str(filename_in_tar.count(os.sep))
    ]
    logging.info("Extracting %s from %s to %s", filename_in_tar, tarball_path, output_dir)
    subprocess.run(cmd, check=True)