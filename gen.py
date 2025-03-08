from src.redset import Redset
from src.user_stats import UserStats
from src.benchmark_stats import BenchmarkStats
from src.redbench import Redbench
from src.utils import DB_FILEPATH, get_experiment_db
from src.benchmarks import setup_benchmarks
from src.imdb import setup_imdb
import duckdb
import os
import argparse


def get_args():
    parser = argparse.ArgumentParser(
        description="""
        Run the entire pipeline to generate RedBench. This includes downloading
        and setting up IMDb, JOB, CEB, and Redset. This script also creates and
        dumps a bunch of plots under the directory figures/.
    """
    )
    parser.add_argument(
        "-s",
        "--show-stats",
        action="store_true",
        help="enable this flag to dump extra stats on JOB, CEB, and Redset to stdout.",
    )
    parser.add_argument(
        "-o",
        "--override",
        action="store_true",
        help="enable this flag to override existing data, i.e. rerun the generation pipeline.",
    )
    parser.add_argument(
        "-v",
        "--version",
        type=str,
        default="serverless",
        choices=["provisioned", "serverless", "both"],
        help="choose Redset version (default: serverless).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    # (Create and) connect to the experiment database
    db = get_experiment_db()

    # Download and setup the IMDb database and its benchmarks JOB and CEB
    setup_imdb()
    setup_benchmarks()

    # Collect stats about the JOB and CEB queries, and dump plots
    benchmark_stats = BenchmarkStats(db, verbose=args.show_stats)
    benchmark_stats.setup()
    benchmark_stats.dump_plots()

    # Download and prefilter the Redset dataset, and dump stats
    redset = Redset(db, args.version, override=args.override, verbose=args.show_stats)
    redset.setup()
    redset.dump_stats()

    # Collect Redset user stats, and dump plots
    user_stats = UserStats(db, override=args.override)
    user_stats.setup()
    user_stats.dump_plots()

    # Generate RedBench
    redbench = Redbench(db, override=args.override)
    redbench.generate()
