import csv
import subprocess
import os
from rich.console import Console

console = Console()

species_file = 'species_list.csv'
output_base_dir = 'gc_species'


# Ensure output base directory exists
os.makedirs(output_base_dir, exist_ok=True)

species_list = [x['species'] for x in csv.DictReader(open(species_file, newline=''))] 
existing_species = set()
for dir_name in os.listdir(output_base_dir):
    if os.path.isdir(os.path.join(output_base_dir, dir_name)):
        existing_species.add(dir_name.replace('_', ' '))
# Mention existing species and that they will be skipped
console.print(f"[bold yellow]Existing species in '{output_base_dir}':[/bold yellow] {', '.join(existing_species)}")
species_list = [s for s in species_list if s not in existing_species]

# Check all species are in filelist beforehand 
file_list = '/well/aanensen/shared/atb/metadata/file_list.all.latest.parquet'
if not os.path.exists(file_list):
    console.print(f"[bold red]File list not found: {file_list}[/bold red]")
    raise FileNotFoundError(f"File list not found: {file_list}")


for species in species_list:
    output_dir = os.path.join(output_base_dir, species.replace(' ', '_'))
    os.makedirs(output_dir, exist_ok=True)
    cmd = [
        'python', 'bigscript-run.py', 'run-gc',
        '--species', species,
        '--submit',
        '--output-dir', output_dir
    ]
    console.print(f"[bold green]Running:[/bold green] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)