import os
import sys
from src.utils import *
from src.redbench import WORKLOADS_DIR
from src.imdb import setup_imdb
from prettytable import PrettyTable
from datetime import timedelta
import time
import argparse


DEFAULT_DUCKDB_CLI = os.path.expanduser("~/.duckdb/cli/latest/duckdb")


def parse_args():
    # Parse the arguments
    parser = argparse.ArgumentParser(description="Run Redbench.")
    parser.add_argument(
        "duckdb_cli",
        type=str,
        nargs="?",
        default=DEFAULT_DUCKDB_CLI,
        help=f"DuckDB binary (default: {DEFAULT_DUCKDB_CLI})."
    )
    args = parser.parse_args()

    # Check whether the binary is available.
    if not os.path.isfile(args.duckdb_cli):
        print(f"Couldn\'t find {args.duckdb_cli}. Please install DuckDB and try again.")
        sys.exit(-1)

    return args

# TIP: Simply modify this to run Redbench on your system
def run_sql_cmd(duckdb_cli, db_file, sql_file):
    os.system(f"{duckdb_cli} --readonly {db_file} < {sql_file} > /dev/null 2>&1")

# Run Redbench
def main(duckdb_cli):
    # Download and setup the IMDb database and its benchmarks JOB and CEB
    setup_imdb(duckdb_cli)

    # Extract the duckdb version.
    duckdb_version = get_duckdb_version(duckdb_cli)
    assert duckdb_version is not None, "Something went wrong when extracting the version of your DuckDB binary."
    log(f"Running Redbench on DuckDB {duckdb_version}..")

    exec_times = dict()
    # Iterate over the query repetition buckets
    for subdir in sorted(get_sub_directories(WORKLOADS_DIR)):
        bucket_name = os.path.basename(subdir)
        log(f"Running Redbench bucket {bucket_name}..")
        start_time = time.perf_counter_ns()
        # Iterate over the 3 different variability workloads for this bucket
        for filename in os.listdir(subdir):
            if not filename.endswith(".sql"):
                continue
            filepath = os.path.join(subdir, filename)

            # And run.
            run_sql_cmd(duckdb_cli, 'imdb/db.duckdb', filepath)
        exec_times[bucket_name] = (time.perf_counter_ns() - start_time) / 1e9

    # Prepare and print the results table
    results_table = PrettyTable()
    results_table.field_names = ["Query repetition bucket", "Total execution time"]
    for bucket_name, exec_time in exec_times.items():
        results_table.add_row([bucket_name, str(timedelta(seconds=exec_time))])
    print(results_table)

# And run
if __name__ == "__main__":
    main(parse_args().duckdb_cli)