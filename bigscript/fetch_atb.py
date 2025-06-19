import sys
import os
import gzip
import hashlib
import pandas as pd
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from bigscript.conf import FILE_LIST_PATH, ATB_ARCHIVE_PATH, ATB_FILES_PATH, PQ_FILE_LIST_PATH
from bigscript.util import get_parquet_file_list

def ensure_directories():
    """Ensure that the required directories for ATB files and archives exist."""
    os.makedirs(ATB_FILES_PATH, exist_ok=True)
    os.makedirs(ATB_ARCHIVE_PATH, exist_ok=True)

def read_tsv_to_df(tsv_gz_path: str) -> pd.DataFrame:
    """
    Read a gzipped TSV file into a pandas DataFrame.

    Args:
        tsv_gz_path: Path to the gzipped TSV file.

    Returns:
        DataFrame containing the TSV data.
    """
    with gzip.open(tsv_gz_path, 'rt') as f:
        return pd.read_csv(f, sep='\t')

def md5sum(filepath: str) -> str:
    """
    Calculate the MD5 checksum of a file.

    Args:
        filepath: Path to the file.

    Returns:
        MD5 checksum as a hex string.
    """
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def download_tarballs(df: pd.DataFrame, archive_path: str) -> None:
    """
    Download unique tarball files from URLs in the DataFrame, verifying their MD5 checksums.

    Args:
        df: DataFrame containing tarball info (must have 'tar_xz', 'tar_xz_url', 'tar_xz_md5').
        archive_path: Directory to save downloaded tarballs.
    """
    unique_tarballs = df[['tar_xz', 'tar_xz_url', 'tar_xz_md5']].drop_duplicates()
    for idx, row in unique_tarballs.iterrows():
        tarball_name = row['tar_xz']
        url = row['tar_xz_url']
        expected_md5 = row['tar_xz_md5'] if 'tar_xz_md5' in row else None
        out_path = os.path.join(archive_path, tarball_name)
        if os.path.exists(out_path):
            if expected_md5:
                file_md5 = md5sum(out_path)
                if file_md5 == expected_md5:
                    print(f"Already exists and checksum matches: {out_path}")
                    continue
                else:
                    print(f"Checksum mismatch for {out_path}, re-downloading...")
            else:
                print(f"Already exists: {out_path}")
                continue
        print(f"Downloading {tarball_name} from {url} ...")
        try:
            r = requests.get(url, stream=True, timeout=60)
            r.raise_for_status()
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            if expected_md5:
                file_md5 = md5sum(out_path)
                if file_md5 == expected_md5:
                    print(f"Saved to {out_path} (checksum OK)")
                else:
                    print(f"Checksum mismatch after download for {out_path}")
            else:
                print(f"Saved to {out_path}")
        except Exception as e:
            print(f"Failed to download {tarball_name} from {url}: {e}")

def main():
    """Main function to run the ATB file processing and downloading workflow."""
    ensure_directories()
    df = get_parquet_file_list(FILE_LIST_PATH, PQ_FILE_LIST_PATH)
    download_tarballs(df, ATB_ARCHIVE_PATH)

if __name__ == "__main__":
    main()
