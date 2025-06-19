import typer
from bigscript.fetch_atb import main as fetch_atb_main
from bigscript.get_assembly import extract_species, extract_accessions
from bigscript.conf import FILE_LIST_PATH, ATB_ARCHIVE_PATH, PQ_FILE_LIST_PATH

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


if __name__ == "__main__":
    app()
