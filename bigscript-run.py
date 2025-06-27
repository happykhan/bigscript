
#!/usr/bin/env python3
import typer
from bigscript.fetch_atb import main as fetch_atb_main
from bigscript.get_assembly import extract_species, extract_accessions
from bigscript.conf import FILE_LIST_PATH, ATB_ARCHIVE_PATH, PQ_FILE_LIST_PATH
from bigscript.slurm import kill_all_my_slurm_jobs, update_slurm_job_status
from bigscript.gc import calculate_gc

app = typer.Typer()

@app.command()
def fetch_atb():
    """Run the ATB fetch script."""
    fetch_atb_main()

@app.command()
def get_species(
    species: str,
    output_dir: str = "my_assemblies",
    batch: bool = False,
    max_jobs: int = 1000,
    submit: bool = False,    
    tsv_gz_path: str = FILE_LIST_PATH,
    archive_dir: str = ATB_ARCHIVE_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH
):
    """Extract files for a given species."""
    extract_species(species, output_dir, batch, max_jobs, submit, tsv_gz_path, archive_dir, parquet_path)

@app.command()
def get_accessions(
    accession_codes: str = typer.Argument(..., help="Comma-separated list of accession codes"),
    output_dir: str = "my_assemblies",
    batch: bool = False,
    max_jobs: int = 1000,
    submit: bool = False,    
    tsv_gz_path: str = FILE_LIST_PATH,
    archive_dir: str = ATB_ARCHIVE_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH
):
    """Extract files for given accession codes."""
    accession_list = accession_codes.split(',')
    extract_accessions(accession_list, output_dir, batch, max_jobs, submit, tsv_gz_path, archive_dir, parquet_path)

@app.command()
def kill_slurm_jobs():
    """Kill all running jobs."""
    kill_all_my_slurm_jobs()

@app.command()
def job_status(
    job_list_path: str
):
    """Update the status of SLURM jobs."""
    update_slurm_job_status(job_list_path)

@app.command()
def run_gc(
    output_dir: str = "gc_results",
    accession_codes: str = typer.Option(None, help="Comma-separated list of accession codes"),
    species: str = typer.Option(None, help="Species name to filter by"),
    max_jobs: int = 1000,
    submit: bool = False
):
    """Calculate GC content for given accession codes or species."""
    if not accession_codes and not species:
        raise typer.BadParameter("Please provide either accession codes or a species name.")
    calculate_gc(output_dir, accession_codes, species, max_jobs, submit)


if __name__ == "__main__":
    app()
