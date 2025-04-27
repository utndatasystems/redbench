import os
import sys
from src.utils import *
from src.redbench import WORKLOADS_DIR
from src.benchmarks.imdb import setup_imdb_db
from src.benchmarks.tpcds import setup_tpcds_db
from prettytable import PrettyTable
from datetime import timedelta
import time
import argparse


DB_FILEPATHS = {
    "imdb": "imdb/db.duckdb",
    "tpcds": "tpcds/db.duckdb",
}


def get_db_filepath(support_benchmark):
    """
    Get the DuckDB database filepath for the given benchmark.
    """
    if any(i in support_benchmark for i in ["ceb_job", "job", "ceb"]):
        return DB_FILEPATHS["imdb"]
    elif "tpcds" in support_benchmark:
        return DB_FILEPATHS["tpcds"]


DEFAULT_DUCKDB_CLI = os.path.expanduser("~/.duckdb/cli/latest/duckdb")


def parse_args():
    # Parse the arguments
    parser = argparse.ArgumentParser(description="Run Redbench.")
    parser.add_argument(
        "duckdb_cli",
        type=str,
        nargs="?",
        default=DEFAULT_DUCKDB_CLI,
        help=f"DuckDB binary (default: {DEFAULT_DUCKDB_CLI}).",
    )
    parser.add_argument(
        "support_benchmark",
        type=str,
        nargs="?",
        default=None,
        help=f"""
            Support benchmark, e.g., ceb_job, tpcds_10gb, or all (default: all).
            The support benchmark names are the names of the directories
            under the workloads directory. The repository includes by default
            ceb_job and tpcds_10gb.
        """,
    )
    args = parser.parse_args()

    # Check whether the binary is available.
    if not os.path.isfile(args.duckdb_cli):
        print(f"Couldn't find {args.duckdb_cli}. Please install DuckDB and try again.")
        sys.exit(-1)

    return args


# TIP: Simply modify this to run Redbench on your system
def run_sql_cmd(duckdb_cli, db_file, sql_file):
    os.system(f"{duckdb_cli} --readonly {db_file} < {sql_file} > /dev/null 2>&1")


# Run Redbench
def main(duckdb_cli, support_benchmark):
    # Download and setup the IMDb database and its benchmarks JOB and CEB
    setup_imdb_db(duckdb_cli)
    setup_tpcds_db(duckdb_cli)

    # Extract the duckdb version.
    duckdb_version = get_duckdb_version(duckdb_cli)
    assert (
        duckdb_version is not None
    ), "Something went wrong when extracting the version of your DuckDB binary."
    log(f"Running Redbench on DuckDB {duckdb_version}..")

    exec_times = dict()
    # Iterate over the query repetition buckets
    workload_dirpaths = {
        support_benchmark: f"{WORKLOADS_DIR}/{support_benchmark}"
    } if support_benchmark else {
        support_benchmark: f"{WORKLOADS_DIR}/{support_benchmark}"
        for support_benchmark in os.listdir(WORKLOADS_DIR)
    }
    for support_benchmark, workload_dirpath in workload_dirpaths.items():
        for subdir in sorted(get_sub_directories(workload_dirpath)):
            bucket_name = os.path.basename(subdir)
            log(f"Running redbench[{support_benchmark}] bucket {bucket_name}..")
            start_time = time.perf_counter_ns()
            # Iterate over the 3 different variability workloads for this bucket
            for filename in os.listdir(subdir):
                if not filename.endswith(".sql"):
                    continue
                filepath = os.path.join(subdir, filename)

                # And run.
                run_sql_cmd(duckdb_cli, get_db_filepath(support_benchmark), filepath)
            exec_times[bucket_name] = (time.perf_counter_ns() - start_time) / 1e9

        # Prepare and print the results table
        results_table = PrettyTable()
        results_table.field_names = ["Query repetition bucket", "Total execution time"]
        for bucket_name, exec_time in exec_times.items():
            results_table.add_row([bucket_name, str(timedelta(seconds=exec_time))])
        print(f"\nRedbench[{support_benchmark}]:")
        print(results_table, "\n")


# And run
if __name__ == "__main__":
    args = parse_args()
    main(args.duckdb_cli, args.support_benchmark)
