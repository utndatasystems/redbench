from .utils import IMDB_DB_FILEPATH, log
import duckdb
import os


def _is_setup():
    return (
        os.path.exists("imdb/benchmarks")
        and os.path.exists("imdb/benchmarks/job")
        and os.path.exists("imdb/benchmarks/ceb")
    )


def setup_benchmarks(override=False):
    if not override and _is_setup():
        log("IMDb benchmarks already set up.")
        return
    log("Setting up IMDb benchmarks JOB and CEB...")
    os.system("rm -rf imdb/benchmarks")
    os.system("tar -xzf benchmarks.tar.gz -C imdb/")
