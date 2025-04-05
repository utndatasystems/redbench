import os
from collections import defaultdict
from .benchmark import Benchmark
from ..utils import *
import re
import queue
import threading

# Constraints on CEB+JOB queries.
# This is to ensure that each value of num_joins has a sufficient
# number of distinct queries and query templates.
MIN_NUM_JOINS_ALLOWED = 6
MAX_NUM_JOINS_ALLOWED = 11


CEB_DIR_PATH = "imdb/benchmarks/ceb"
JOB_DIR_PATH = "imdb/benchmarks/job"


def setup_imdb_db(duckdb_cli, override=False):
    if not override and os.path.exists(IMDB_DB_FILEPATH):
        log("IMDb already set up.")
        return
    os.system(f'[ -f "{IMDB_DB_FILEPATH}" ] && rm "{IMDB_DB_FILEPATH}"')
    log("Downloading and setting up the IMDb database. This may take a few minutes.")
    os.system("wget -q http://event.cwi.nl/da/job/imdb.tgz -O imdb.tgz")
    os.system("mkdir -p imdb/raw_data")
    os.system("tar -xzf imdb.tgz -C imdb/raw_data")
    os.system("rm imdb.tgz")
    os.system(f"{duckdb_cli} imdb/db.duckdb < imdb/schema.sql")
    os.system(f"{duckdb_cli} imdb/db.duckdb < imdb/load.sql")


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
        self.target_benchmark = kwargs.get("target_benchmark", None)

    def _is_benchmarks_setup(self):
        return os.path.exists(JOB_DIR_PATH) and os.path.exists(CEB_DIR_PATH)

    def setup(self, override=False):
        if not override and self._is_benchmarks_setup():
            log("IMDb benchmarks already set up.")
            return
        log("Setting up IMDb benchmarks JOB and CEB...")
        os.system("rm -rf imdb/benchmarks")
        os.system("tar -xzf imdb/benchmarks.tar.gz -C imdb/")

    def _create_stats_tables(self):
        for benchmark_name in ["job", "ceb", "ceb_job"]:
            self.stats_db.execute(
                f"""
                CREATE OR REPLACE TABLE {benchmark_name}_stats (
                    filepath VARCHAR,
                    num_joins INTEGER,
                    template VARCHAR
                )
            """
            )

    def _extract_template_from_filepath(self, filepath):
        assert "imdb/benchmarks" in filepath
        filepath = filepath.split("imdb/benchmarks/")[1]
        benchmark = filepath.split("/")[0]
        template = filepath.split("/")[1]
        if benchmark == "job":
            # Get the number as the template (1, 2, 3, ..., 33)
            return re.match(r"\d+", template).group()
        assert benchmark == "ceb"
        # The folder name is the template (1a, 2a, 2b, ..., 11b)
        return filepath.split("/")[1]


    def _process_dir(self, dir_path, query_stats):
        template_to_num_joins = dict()
        for filename in os.listdir(dir_path):
            filepath = os.path.join(dir_path, filename)
            template = self._extract_template_from_filepath(filepath)
            if template in template_to_num_joins:
                query_stats[filepath] = {
                    "num_joins": template_to_num_joins[template],
                    "template": template,
                }
                continue

            # Read query
            with open(filepath, "r") as file:
                query = file.read()

            # Get number of joins in the execution plan
            tmp_query_filepath = f"tmp/query.sql"
            profile_filepath = f"tmp/query_profile.json"
            with open(tmp_query_filepath, "w") as file:
                file.write(
                    f"""
                    PRAGMA enable_profiling='json';
                    PRAGMA profiling_output = '{profile_filepath}';
                    {query};
                """
                )
            os.system(
                f"{self.duckdb_cli} imdb/db.duckdb < {tmp_query_filepath} > /dev/null"
            )
            with open(profile_filepath, "r") as file:
                profile = file.read()
            os.remove(profile_filepath)
            os.remove(tmp_query_filepath)

            num_joins = profile.count('"operator_type": "HASH_JOIN"') - profile.count(
                '"operator_type": "COLUMN_DATA_SCAN"'
            )
            template_to_num_joins[template] = num_joins

            query_stats[filepath] = {
                "num_joins": num_joins,
                "template": template,
            }

    def _is_stats_setup(self):
        return all(
            self.stats_db.execute(
                f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}_stats'"
            ).fetchone()[0]
            > 0
            for table in ["job", "ceb", "ceb_job"]
        ) and all(
            self.stats_db.execute(f"SELECT COUNT(*) FROM {table}_stats").fetchone()[0] > 0
            for table in ["job", "ceb", "ceb_job"]
        )

    def compute_stats(self, override=False):
        if not override and self._is_stats_setup():
            log("IMDb benchmark stats already set up.")
            return

        self._create_stats_tables()
        benchmark_stats = defaultdict(dict)

        log("Collecting stats for JOB queries..")
        self._process_dir(JOB_DIR_PATH, benchmark_stats["job"])

        log("Collecting stats for CEB queries..")
        for subdir in get_sub_directories(CEB_DIR_PATH):
            self._process_dir(subdir, benchmark_stats["ceb"])

        for benchmark_name, query_stats in benchmark_stats.items():
            for filepath, stats in query_stats.items():
                self._insert_stats(filepath, stats, benchmark_name)
                self._insert_stats(filepath, stats, "ceb_job")

    def _insert_stats(self, filepath, query_stats, benchmark_name):
        self.stats_db.execute(
            f"""
            INSERT INTO {benchmark_name}_stats
            VALUES (
                '{filepath}',
                {query_stats["num_joins"]},
                '{query_stats["template"]}'
            )
        """
        )

    def get_stats(
        self,
        bounds=(MIN_NUM_JOINS_ALLOWED, MAX_NUM_JOINS_ALLOWED),
        target_benchmark=None,
    ):
        target_benchmark = target_benchmark or self.target_benchmark
        benchmark_stats = {}
        for row in (
            # We order by filepath to ensure that Redbench generation is
            # deterministic even across different DuckDB versions.
            self.stats_db.execute(
                f"""
                SELECT * FROM {target_benchmark}_stats ORDER BY filepath
            """
            )
            .fetchdf()
            .to_dict(orient="records")
        ):
            benchmark_stats[row["filepath"]] = {
                "num_joins": row["num_joins"],
                "template": row["template"],
            }
        return (
            bound_num_joins(benchmark_stats, min_joins=bounds[0], max_joins=bounds[1])
            if bounds is not None
            else benchmark_stats
        )

    def _draw_bar_plot(
        self, map, x_label, y_label, title, save_path, log_scale_y=False
    ):
        tt = sorted([(p[0], p[1]) for p in map.items()], key=lambda x: x[0])
        xs = [t[0] for t in tt]
        ys = [t[1] for t in tt]
        draw_bar_plot(
            xs,
            ys,
            x_label,
            y_label,
            save_path=save_path,
            log_scale_y=log_scale_y,
            title=title,
        )

    def _dump_plots(self, benchmark_name, dir_path):
        stats = self.get_stats(bounds=None, target_benchmark=benchmark_name)
        benchmark_name = (
            benchmark_name.upper() if benchmark_name != "ceb_job" else "CEB+"
        )
        map_n_joins_to_templates = defaultdict(set)
        map_n_joins_to_number_of_queries = defaultdict(int)
        for filepath, single_query_stats in stats.items():
            template = self._extract_template_from_filepath(filepath)
            map_n_joins_to_templates[single_query_stats["num_joins"]].add(template)
            map_n_joins_to_number_of_queries[single_query_stats["num_joins"]] += 1

        self._draw_bar_plot(
            map_n_joins_to_number_of_queries,
            "Number of joins",
            f"Number of query instances (log scale)",
            f"Number of distinct query instances in {benchmark_name} per number of joins",
            os.path.join(dir_path, "query_instances.png"),
            log_scale_y=True,
        )
        self._draw_bar_plot(
            {k: len(map_n_joins_to_templates[k]) for k in map_n_joins_to_templates},
            "Number of joins",
            f"Number of templates",
            f"Number of templates in {benchmark_name} per number of joins",
            os.path.join(dir_path, "templates.png"),
        )

        log(
            f"Number of distinct query instances in {benchmark_name}: {sum(map_n_joins_to_number_of_queries.values())}"
        )
        log(
            f"Number of templates in {benchmark_name}: {len(set(sum(list(map(list, map_n_joins_to_templates.values())), [])))}"
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

    def normalize_num_joins(self, num_joins):
        return int(
            num_joins * (MAX_NUM_JOINS_ALLOWED - MIN_NUM_JOINS_ALLOWED)
            + MIN_NUM_JOINS_ALLOWED
            + 0.5
        )
