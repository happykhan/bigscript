import subprocess
import getpass
import pandas as pd
import os
from termcolor import colored

def kill_all_my_slurm_jobs():
    """
    Kills all SLURM jobs belonging to the current user.
    """
    user = getpass.getuser()
    try:
        # Get all job IDs for the current user
        result = subprocess.run(
            ["squeue", "-u", user, "-h", "-o", "%A"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        job_ids = result.stdout.strip().splitlines()
        if not job_ids:
            print("No SLURM jobs found for user:", user)
            return
        # Cancel all jobs
        subprocess.run(
            ["scancel"] + job_ids,
            check=True
        )
        print(f"Cancelled {len(job_ids)} SLURM jobs for user: {user}")
    except subprocess.CalledProcessError as e:
        print("Error killing SLURM jobs:", e)

def update_slurm_job_status(table_path: str) -> None:
    """
    Given a csv with job id in the first column and slurm script path in the second,
    check the status of all jobs, add/update a 'status' column, collect error messages,
    add those to an 'error' column, print the table, and write it back to the original file.
    """
    df = pd.read_csv(table_path)
    # Ensure columns
    for col in ['status', 'error']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(object)
    # Ensure 'status' and 'error' columns are string type to avoid dtype warnings
    df['status'] = df['status'].astype(str)
    df['error'] = df['error'].astype(str)
    for idx, row in df.iterrows():
        job_id = str(row.iloc[0])
        script_path = row.iloc[1]
        # Check job status
        status = 'UNKNOWN'
        try:
            # The job might have completed and is now in sacct. So just use sacct to get the status
            sacct_job_status_output = sacct_job_status(job_id)
            status = sacct_job_status_output['State']
            df.at[idx, 'runTime'] = sacct_job_status_output.get('Elapsed', '')
            df.at[idx, 'NodeList'] = sacct_job_status_output.get('NodeList', '')
            df.at[idx, 'StartTime'] = sacct_job_status_output.get('Start', '')
            df.at[idx, 'EndTime'] = sacct_job_status_output.get('End', '')
            reason = sacct_job_status_output.get('Reason', '')
            if reason == 'None':
                reason = ''
            df.at[idx, 'Reason'] = reason
            

        except subprocess.SubprocessError as e:
            status = f'ERROR: {e}'
        df.at[idx, 'status'] = status
        # If job is completed or failed, check for error file
        error_msg = ''
        if status in ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'BOOT_FAIL', 'DEADLINE', 'ERROR', 'SPECIAL_EXIT', 'REVOKED', 'SUSPENDED', 'STOPPED', 'UNKNOWN'):
            # Get error file path from the script itself
            err_path = None
            try:
                with open(script_path, 'r', encoding='utf-8', errors='ignore') as sf:
                    for line in sf:
                        if (line.strip().startswith('#SBATCH') and '--error=' in line):
                            err_path = (line.split('--error=')[1].strip().split()[0])
                            break
            except OSError as e:
                print(f"Error reading script {script_path}: {e}")
            if err_path and os.path.exists(err_path):
                with open(err_path,'r',encoding='utf-8',errors='ignore') as ef:
                    lines = ef.readlines()
                    if len(lines) > 0:
                        error_msg = f"{len(lines)} lines in error log: {err_path}"
        df.at[idx, 'error'] = error_msg
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    # Set desired column order for printing
    desired_columns = ['job_id', 'NodeList', 'StartTime', 'EndTime', 'runTime', 'script',  'status', 'reason', 'error']
    # Map to actual column names in df (first column is job id, second is script)
    col_map = {
        'job_id': df.columns[0],
        'script': df.columns[1],
        'status': 'status',
        'NodeList': 'NodeList' if 'NodeList' in df.columns else '',
        'StartTime': 'StartTime' if 'StartTime' in df.columns else '',
        'EndTime': 'EndTime' if 'EndTime' in df.columns else '',
        'runTime': 'runTime' if 'runTime' in df.columns else '',
        'error': 'error',
        'reason': 'Reason' if 'Reason' in df.columns else ''
    }
    ordered_cols = [col_map[c] for c in desired_columns if col_map[c] in df.columns]
    # Print the DataFrame with colored status
    # Print header
    header_str = []
    for col in ordered_cols:
        header_str.append(colored(col, 'cyan'))
    print('\t'.join(header_str))
    for _, row in df[ordered_cols].iterrows():
        row_str = []
        for col in ordered_cols:
            if col == 'status':
                row_str.append(color_status(str(row[col])))
            else:
                row_str.append(str(row[col]))
        print('\t'.join(row_str))
    df.to_csv(table_path, index=False)



# Print with color for status column
def color_status(val):
    if val == 'COMPLETED':
        return colored(val, 'green')
    elif val in ('FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'OUT_OF_MEMORY', 'ERROR', 'UNKNOWN'):
        return colored(val, 'red')
    elif val == 'RUNNING':
        return colored(val, 'yellow')
    else:
        return val    


def parse_scontrol_output(text):
    job_info = {}
    # Flatten newlines: treat multiple lines as space-separated tokens
    tokens = text.replace('\n', ' ').split()
    
    for token in tokens:
        if '=' in token:
            key, val = token.split('=', 1)
            job_info[key.strip()] = val.strip()
    return job_info

def sacct_job_status(job_id):
    """
    Get job status using sacct command and return a dictionary of relevant fields for the given job_id.
    """
    try:
        # Use --noheader to avoid extra header lines, and --parsable2 for easier parsing
        result = subprocess.run(
            ["sacct", "-j", job_id, "--parsable2", "--format", "JobID,State,Elapsed,NodeList,Start,End,Reason"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        lines = result.stdout.strip().splitlines()
        # Step 1: Extract headers (first non-separator line)
        headers = [h.strip() for h in lines[0].split('|')]
        
        # Step 2: Ignore any separator lines (e.g., lines full of dashes)
        data_lines = [line.split('|') for line in lines[1:]]
        # Step 3: Parse each row into a dict
        rows = []
        for line in data_lines:
            row_dict = dict(zip(headers, line))
            # remove empty values
            row_dict = {k: v for k, v in row_dict.items() if v.strip() != ''}
            rows.append(row_dict)
        # Return the row where JobID matches job_id (not job_id.batch or job_id.extern)
        for row in rows:
            if row.get('JobID') == job_id:
                return row
        return {}
    except subprocess.CalledProcessError as e:
        print(f"Error getting job status for {job_id}: {e}")
        return {}