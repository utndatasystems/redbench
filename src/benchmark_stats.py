import os
import time
import subprocess
import duckdb
from typing import Dict
from collections import defaultdict
from tqdm import tqdm
from .utils import *


# Constraints on CEB+JOB queries.
# This is to ensure that each value of num_joins has a sufficient
# number of distinct queries and distinct readsets.
MAX_NUM_JOINS_ALLOWED = 15
MIN_NUM_JOINS_ALLOWED = 6


class BenchmarkStats:
    def __init__(self, db, override=False, verbose=False):
        self.db = db
        self.imdb_db = duckdb.connect(IMDB_DB_FILEPATH, read_only=True)
        self.override = override
        self.verbose = verbose

    def _is_setup(self):
        return all(
            self.db.execute(
                f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}_stats'"
            ).fetchone()[0]
            > 0
            for table in ["job", "ceb", "ceb_job"]
        ) and all(
            self.db.execute(f"SELECT COUNT(*) FROM {table}_stats").fetchone()[0] > 0
            for table in ["job", "ceb", "ceb_job"]
        )

    def _create_tables(self):
        for benchmark_name in ["job", "ceb", "ceb_job"]:
            self.db.execute(
                f"""
                CREATE OR REPLACE TABLE {benchmark_name}_stats (
                    filepath VARCHAR,
                    num_joins INTEGER,
                    readset VARCHAR
                )
            """
            )

    def _collect_stats(self):
        benchmark_stats = defaultdict(dict)

        log("Collecting stats for JOB queries...")
        self._process_dir(JOB_DIR_PATH, benchmark_stats["job"])

        log("Collecting stats for CEB queries...")
        for subdir in get_sub_directories(CEB_DIR_PATH):
            self._process_dir(subdir, benchmark_stats["ceb"])

        for benchmark_name, query_stats in benchmark_stats.items():
            for filepath, stats in query_stats.items():
                self._insert_stats(filepath, stats, benchmark_name)
                self._insert_stats(filepath, stats, "ceb_job")

    def setup(self):
        if not self.override and self._is_setup():
            log("Benchmark stats already set up.")
            return
        log("Setting up benchmark stats...")
        self._create_tables()
        self._collect_stats()

    def _process_dir(self, dir_path, query_stats):
        for filename in os.listdir(dir_path):
            # Read query
            filepath = os.path.join(dir_path, filename)
            with open(filepath, "r") as file:
                query = file.read()

            # Execute explain plan query
            explain_df = self.imdb_db.execute(f"EXPLAIN {query}").fetch_df()

            # Create stats dict
            query_stats[filepath] = {
                "num_joins": sum(
                    line.count("HASH_JOIN") for line in explain_df["explain_value"]
                ),
                "readset": extract_readset_from_query(filepath),
            }

    def _insert_stats(self, filepath, query_stats, benchmark_name):
        self.db.execute(
            f"""
            INSERT INTO {benchmark_name}_stats
            VALUES (
                '{filepath}',
                {query_stats["num_joins"]},
                '{query_stats["readset"]}'
            )
        """
        )

    def get(
        self, benchmark_name, bounds=(MIN_NUM_JOINS_ALLOWED, MAX_NUM_JOINS_ALLOWED)
    ):
        benchmark_stats = {}
        # as dict
        for row in (
            self.db.execute(f"SELECT * FROM {benchmark_name}_stats")
            .fetchdf()
            .to_dict(orient="records")
        ):
            benchmark_stats[row["filepath"]] = {
                "num_joins": row["num_joins"],
                "readset": row["readset"],
            }
        return (
            bound_num_joins(benchmark_stats, min_joins=bounds[0], max_joins=bounds[1])
            if bounds is not None
            else benchmark_stats
        )

    def _draw_bar_plot(self, map, x_label, y_label, save_path, log_scale_y=False):
        tt = sorted([(p[0], p[1]) for p in map.items()], key=lambda x: x[0])
        xs = [t[0] for t in tt]
        ys = [t[1] for t in tt]
        draw_bar_plot(
            xs, ys, x_label, y_label, save_path=save_path, log_scale_y=log_scale_y
        )

    def _dump_plots(self, benchmark_name, dir_path):
        stats = self.get(benchmark_name, bounds=None)
        benchmark_name = (
            benchmark_name.upper() if benchmark_name != "ceb_job" else "CEB+"
        )
        map_n_joins_to_distinct_readsets = defaultdict(set)
        map_n_joins_to_number_of_queries = defaultdict(int)
        for filepath, single_query_stats in stats.items():
            readset = tuple(sorted(list(extract_readset_from_query(filepath))))
            map_n_joins_to_distinct_readsets[single_query_stats["num_joins"]].add(
                readset
            )
            map_n_joins_to_number_of_queries[single_query_stats["num_joins"]] += 1

        self._draw_bar_plot(
            map_n_joins_to_number_of_queries,
            "Number of joins",
            f"Number of query instances in {benchmark_name} (log scale)",
            os.path.join(dir_path, "query_instances.png"),
            log_scale_y=True,
        )
        self._draw_bar_plot(
            {
                k: len(map_n_joins_to_distinct_readsets[k])
                for k in map_n_joins_to_distinct_readsets
            },
            "Number of joins",
            f"Number of distinct readsets in {benchmark_name}",
            os.path.join(dir_path, "distinct_readsets.png"),
        )

        log(
            f"Number of distinct query instances in {benchmark_name}: {sum(map_n_joins_to_number_of_queries.values())}",
            verbose=self.verbose,
        )
        log(
            f"Number of distinct readsets in {benchmark_name}: {sum(map(len, map_n_joins_to_distinct_readsets.values()))}",
            verbose=self.verbose,
        )

    def dump_plots(self):
        benchmark_dir_paths = {
            "job": "figures/imdb_benchmarks/job",
            "ceb": "figures/imdb_benchmarks/ceb",
            "ceb_job": "figures/imdb_benchmarks/ceb_job",
        }
        for benchmark_name, dir_path in benchmark_dir_paths.items():
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            self._dump_plots(benchmark_name, dir_path)
        log(
            f"IMDb benchmark (JOB, CEB, CEB+) plots dumped to figures/imdb_benchmarks/.",
        )
