# conf.py
import os 

PYTHON_BIN = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'venv', 'bin', 'python'))

# Diretories paths
ATB_FILES_PATH = "/well/aanensen/shared/atb/"
ATB_ARCHIVE_PATH = os.path.join(ATB_FILES_PATH, "arc")
ATB_METADATA_PATH = os.path.join(ATB_FILES_PATH, "metadata")
TEMP_DIR = os.path.abspath(os.path.join(ATB_FILES_PATH, 'tmp'))

# Files paths
FILE_LIST_PATH = os.path.join(ATB_METADATA_PATH, "file_list.all.latest.tsv.gz")
PQ_FILE_LIST_PATH = os.path.join(ATB_METADATA_PATH, "file_list.all.latest.parquet")
