import os 
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bigscript.conf import FILE_LIST_PATH, PQ_FILE_LIST_PATH

def read_tsv_to_df(tsv_gz_path: str) -> pd.DataFrame:
    """
    Read a gzipped TSV file into a pandas DataFrame.
    Args:
        tsv_gz_path: Path to the gzipped TSV file.
    Returns:
        DataFrame containing the TSV data.
    """
    import gzip
    with gzip.open(tsv_gz_path, 'rt') as f:
        return pd.read_csv(f, sep='\t')


def get_parquet_file_list(
    tsv_gz_path: str = FILE_LIST_PATH,
    parquet_path: str = PQ_FILE_LIST_PATH,
) -> pd.DataFrame:
    """
    Create a Parquet file from a gzipped TSV file containing the ATB file list.
    Args:
        tsv_gz_path: Path to the gzipped TSV file.
        parquet_path: Path to save the Parquet file.
    """
    if not os.path.exists(parquet_path):
        logging.info("Creating Parquet file from %s to %s", tsv_gz_path, parquet_path)
        df = read_tsv_to_df(tsv_gz_path)
        table = pa.Table.from_pandas(df)  # type: ignore[attr-defined]  # ignore error  PylancereportAttributeAccessIssue
        pq.write_table(table, parquet_path)
    else:
        logging.info("Loading Parquet file from %s", parquet_path)
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
    return df