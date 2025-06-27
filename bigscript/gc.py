import os
import subprocess
from typing import List
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from bigscript.conf import FILE_LIST_PATH, ATB_ARCHIVE_PATH, PQ_FILE_LIST_PATH, TEMP_DIR, PYTHON_BIN
from bigscript.util import get_parquet_file_list
from bigscript.get_assembly import submit_jobs, create_folder_structure
import re


def calculate_gc(
    output_dir: str,
    accession_codes = None,
    species=None,
    max_jobs: int = 1000,
    submit: bool = False,

):
    if accession_codes is None and species is  None:
        raise ValueError("Please provide either accession codes or a species name.")

    if species:
        if accession_codes is not None:
            accession_codes += extract_species(species, FILE_LIST_PATH, PQ_FILE_LIST_PATH)
        else:
            accession_codes = extract_species(species, FILE_LIST_PATH, PQ_FILE_LIST_PATH)
    run_gc_jobs(
        accession_codes=accession_codes,
        output_dir=output_dir,
        max_jobs=max_jobs,
        submit=submit,
        tsv_gz_path=FILE_LIST_PATH,
        archive_dir=ATB_ARCHIVE_PATH,
        parquet_path=PQ_FILE_LIST_PATH
    )


# Input is a species, or accession codes 

# Extract accesisons if species list is provided 
def extract_species(
    species: str,
    tsv_gz_path: str = FILE_LIST_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH,
) -> None:
    """    Given a species name, ensure a Parquet file exists for the file list,
    load it, find all paths for the species, and extract them to the output dir using tar.
    """
    df = get_parquet_file_list(tsv_gz_path, parquet_path)
    if species == 'Clostridium difficile':
        # Special case for Clostridium difficile, which is stored as Clostridioides difficile in the file list
        species = 'Clostridioides difficile'
    # The species name should match the 'species_sylph' column in the DataFrame.
    # Even if it's like Campylobacter_D coli, it should match the first part of the species name.
    # Or Campylobacter coli_A,
    # We'll match rows where the 'species_sylph' column starts with the provided species name (case-insensitive).
    genus, species_part = species.split()
    pattern = re.compile(
        rf"\b{re.escape(genus)}(?:[_A-Za-z]*)\s+{re.escape(species_part)}(?:[_A-Za-z]*)\b", 
        re.IGNORECASE
    )    
    filtered = df[df['species_sylph'].apply(lambda x: bool(pattern.match(str(x))))]


    # If no files found for the species, raise an error
    if filtered.empty:
        raise ValueError(f"No files found for species: {species}")
    logging.info("Found %d files for species: %s", len(filtered), species)
    # Extract the accessions from the filtered DataFrames
    logging.info("Extracting files for species: %s", species)
    accession_codes = filtered['sample'].unique().tolist()
    return accession_codes

def run_gc_jobs(
    accession_codes: List[str],
    output_dir: str,
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
    final_output_dir = os.path.join(output_dir, "gc_chunks")
    for idx, (tarball, files) in enumerate(tarball_to_files.items()):
        tarball_path = os.path.join(archive_dir, tarball)
        script_path = os.path.join(job_dir, f'extract_{idx}.slurm')
        strip_components = files[0].count(os.sep)  # Number of leading directories to strip            
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write("#!/bin/bash\n")
            f.write(f"#SBATCH --job-name=extract_{idx}\n")
            f.write(f"#SBATCH --output={log_dir}/extract_{idx}.out\n")
            f.write(f"#SBATCH --error={log_dir}/extract_{idx}.err\n")
            f.write("#SBATCH --time=04:00:00\n")
            f.write("#SBATCH --mem=4G\n")
            f.write("#SBATCH --cpus-per-task=4\n\n")
            f.write("set -e\n\n")
            # Extract only the needed files from the tarball
            files_args = " ".join([f"'{file}'" for file in files])
            f.write(
                f"tar --use-compress-program='xz -T4' -xvf {tarball_path} "
                f"--strip-components={strip_components} -C {TEMP_DIR} {files_args}\n"
            )
            f.write(f"mkdir -p {final_output_dir}\n")
            results_file = os.path.abspath(os.path.join(final_output_dir, f"gc_results_{idx}.txt"))
            # remove output file if it exists
            if os.path.exists(results_file):
                os.remove(results_file)
            # Create a results file for this job
            # Write header to results file
            f.write('echo -e "Filename\\tA\\tT\\tG\\tC\\tN\\tOther" > ' + results_file + '\n')
            # Current sctipt dir, but ../bin
            # Assuming seqkit is in a bin directory relative to the script
            seqkit_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bin', 'seqkit'))
            for filename in files:
                # Extract base filename without path and extension for output
                extracted_file = os.path.join(TEMP_DIR, os.path.basename(filename))                
                # Add command to calculate GC content
                f.write(f"{seqkit_path} seq -s {extracted_file} | tr -d '\\n' | fold -w1 | awk '\n")
                f.write("  BEGIN { A=0; T=0; G=0; C=0; N=0; other=0 }\n")
                f.write("  {\n")
                f.write("    b = toupper($0);\n")
                f.write('    if (b == "A") A++;\n')
                f.write('    else if (b == "T") T++;\n')
                f.write('    else if (b == "G") G++;\n')
                f.write('    else if (b == "C") C++;\n')
                f.write('    else if (b == "N") N++;\n')
                f.write("    else other++;\n")
                f.write("  }\n")
                f.write("  END {\n")
                f.write(f'    printf "{os.path.basename(filename)}\\t";\n')
                f.write('    printf "%d\\t%d\\t%d\\t%d\\t%d\\t%d\\n", A, T, G, C, N, other;\n')
                f.write("  }' >> " + results_file + "\n")
                
                # Clean up extracted file after processing
                f.write(f"rm -f {extracted_file}\n")
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
