import sys
from src.redset import Redset
from src.user_stats import UserStats
from src.redbench import Redbench
from src.utils import get_experiment_db
from src.benchmarks.tpcds import TPCDSBenchmark, setup_tpcds_db
from src.benchmarks.imdb import IMDbBenchmark, setup_imdb_db
from setup import unpack_workloads
import argparse

# Fetch the latest duckdb binary
import os

DEFAULT_DUCKDB_CLI = os.path.expanduser("~/.duckdb/cli/latest/duckdb")


def get_args():
    parser = argparse.ArgumentParser(
        description="""
        Run the entire pipeline to generate RedBench. This includes downloading
        and setting up IMDb, JOB, CEB, and Redset. This script also creates and
        dumps a bunch of plots under the directory figures/.
    """
    )
    parser.add_argument(
        "-b",
        "--duckdb_cli",
        type=str,
        default=DEFAULT_DUCKDB_CLI,
        help=f"DuckDB binary (default: {DEFAULT_DUCKDB_CLI}).",
    )
    parser.add_argument(
        "-o",
        "--override",
        action="store_true",
        help="Enable this flag to override existing data, i.e. rerun the generation pipeline.",
    )
    args = parser.parse_args()

    # Check if the binary is available.
    if not os.path.isfile(args.duckdb_cli):
        print(f"Couldn't find {args.duckdb_cli}. Please install DuckDB and try again.")
        sys.exit(-1)

    return args


if __name__ == "__main__":
    args = get_args()

    # (Create and) connect to the experiment database
    db = get_experiment_db()

    # Download and setup IMDB database
    setup_imdb_db(args.duckdb_cli)

    # Download and setup TPC-DS database
    setup_tpcds_db(
        duckdb_cli=args.duckdb_cli,
        override=args.override,
        scale=10
    )

    # Define benchmarks to be used
    benchmarks = [
        IMDbBenchmark(
            duckdb_cli=args.duckdb_cli,
            stats_db=db,
            target_benchmark="ceb_job",
        ),
        TPCDSBenchmark(
            duckdb_cli=args.duckdb_cli,
            stats_db=db,
            scale=10,
        ),
    ]

    # Download, prefilter, and compute user stats for Redset
    redset = Redset(db)
    redset.setup(override=args.override)
    redset.compute_stats(override=args.override)
    redset.dump_plots()

    # Setup benchmarks and generate Redbench
    for benchmark in benchmarks:
        benchmark.setup(override=args.override)
        benchmark.compute_stats(override=args.override)
        benchmark.dump_plots()
        redbench = Redbench(benchmark, db)
        redbench.generate()

    # Unpack Redbench workloads
    unpack_workloads()
