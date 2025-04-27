import os
from collections import defaultdict
from .benchmark import Benchmark
from ..utils import *
import re

# Constraints on CEB+JOB queries.
# This is to ensure that each value of num_joins has a sufficient
# number of distinct queries and query templates.
MIN_NUM_JOINS_ALLOWED = 6
MAX_NUM_JOINS_ALLOWED = 11


JOB_DIR_PATH = "imdb/benchmarks/job/"
CEB_DIR_PATH = "imdb/benchmarks/ceb/"
IMDB_DB_FILEPATH = "imdb/db.duckdb"


def setup_imdb_db(duckdb_cli, override=False):
    if not override and os.path.exists(IMDB_DB_FILEPATH):
        log("IMDb database already set up.")
        return
    os.system(f'[ -f "{IMDB_DB_FILEPATH}" ] && rm "{IMDB_DB_FILEPATH}"')
    log("Downloading and setting up the IMDb database. This may take a few minutes.")
    os.system("wget -q http://event.cwi.nl/da/job/imdb.tgz -O imdb.tgz")
    os.system("mkdir -p imdb/raw_data")
    os.system("tar -xzf imdb.tgz -C imdb/raw_data")
    os.system("rm imdb.tgz")
    os.system(f"{duckdb_cli} {IMDB_DB_FILEPATH} < imdb/schema.sql")
    os.system(f"{duckdb_cli} {IMDB_DB_FILEPATH} < imdb/load.sql")


class IMDbBenchmark(Benchmark):
    """
    This class represents the IMDb benchmarks JOB and CEB.
    Query stats are computed for both benchmarks.
    """

    def __init__(self, **kwargs):
        """
        target_benchmark: str
            The target benchmark to setup and compute stats on.
            Either "job", "ceb", or "ceb_job".
        """
        super().__init__(**kwargs)
        self.min_num_joins = MIN_NUM_JOINS_ALLOWED
        self.max_num_joins = MAX_NUM_JOINS_ALLOWED
        self.db_filepath = IMDB_DB_FILEPATH
        self.benchmark_name = kwargs.get("target_benchmark", None)

    def _is_benchmarks_setup(self):
        return os.path.exists(JOB_DIR_PATH) and os.path.exists(CEB_DIR_PATH)

    def setup(self, override=False):
        if not override and self._is_benchmarks_setup():
            log("IMDb benchmarks already set up.")
            return
        log("Setting up IMDb benchmarks JOB and CEB...")
        os.system("rm -rf imdb/benchmarks")
        os.system("tar -xzf imdb/benchmarks.tar.gz -C imdb/")


    def _extract_template_from_filepath(self, filepath):
        filepath = filepath.split("imdb/benchmarks/")[1]
        benchmark = filepath.split("/")[0]
        template = filepath.split("/")[1]
        if benchmark == "job":
            # Get the number as the template (1, 2, 3, ..., 33)
            return re.match(r"\d+", template).group()
        assert benchmark == "ceb"
        # The folder name is the template (1a, 2a, 2b, ..., 11b)
        return filepath.split("/")[1]

    def compute_stats(self, override=False):
        if not override and all([self._is_stats_setup(benchmark_name) for benchmark_name in ["job", "ceb", "ceb_job"]]):
            log("IMDb benchmark stats already set up.")
            return

        benchmark_stats = defaultdict(dict)
        for benchmark_name in ["job", "ceb", "ceb_job"]:
            self._create_stats_tables(benchmark_name=benchmark_name)

        log("Collecting stats for JOB queries..")
        self._process_dir(JOB_DIR_PATH, benchmark_stats["job"])

        log("Collecting stats for CEB queries..")
        for subdir in get_sub_directories(CEB_DIR_PATH):
            self._process_dir(subdir, benchmark_stats["ceb"])

        for benchmark_name, query_stats in benchmark_stats.items():
            for filepath, stats in query_stats.items():
                self._insert_stats(filepath, stats, benchmark_name)
                self._insert_stats(filepath, stats, "ceb_job")

    def dump_plots(self):
        for benchmark_name in ["job", "ceb", "ceb_job"]:
            super().dump_plots(benchmark_name=benchmark_name)
