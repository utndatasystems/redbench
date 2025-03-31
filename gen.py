import sys
from src.redset import Redset
from src.user_stats import UserStats
from src.redbench import Redbench
from src.utils import get_experiment_db
from src.benchmarks.imdb import IMDbBenchmark
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
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    # Check if the binary is available.
    if not os.path.isfile(args.duckdb_cli):
        print(f"Couldn't find {args.duckdb_cli}. Please install DuckDB and try again.")
        sys.exit(-1)

    # (Create and) connect to the experiment database
    db = get_experiment_db()

    # Download the IMDb benchmarks and compute query stats
    imdb_benchmarks = IMDbBenchmark(
        duckdb_cli=args.duckdb_cli,
        stats_db=db,
        target_benchmark="ceb_job",
    )
    imdb_benchmarks.setup(override=args.override)
    imdb_benchmarks.dump_plots()

    # Download, prefilter, and compute user stats from Redset
    redset = Redset(db)
    redset.setup(override=args.override)
    redset.dump_plots()

    # Generate RedBench
    redbench = Redbench(imdb_benchmarks, db)
    redbench.generate()

    # Unpack Redbench workloads
    unpack_workloads()
