import os
import duckdb
from collections import defaultdict
from .utils import *
from .benchmarks import extract_template_from_filepath
import threading
import queue


# TODO: Update this
# Constraints on CEB+JOB queries.
# This is to ensure that each value of num_joins has a sufficient
# number of distinct queries and distinct readsets.
MIN_NUM_JOINS_ALLOWED = 4
MAX_NUM_JOINS_ALLOWED = 11


class BenchmarkStats:
    def __init__(self, db, override=False, verbose=False):
        self.db = db
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
                    template VARCHAR
                )
            """
            )

    def _collect_stats(self):
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

        for benchmark_name in ["job", "ceb", "ceb_job"]:
            self._is_valid(benchmark_name)

    def setup(self):
        if not self.override and self._is_setup():
            log("Benchmark stats already set up.")
            return
        log("Setting up benchmark stats..")
        self._create_tables()
        self._collect_stats()

    def _process_file(self, filepath, query_stats, lock):
        with open(filepath, "r") as file:
            query = file.read()

        # Get number of joins in the execution plan
        tmp_query_filepath = f"tmp/{threading.get_ident()}.sql"
        profile_filepath = f"tmp/{threading.get_ident()}_profile.json"
        with open(tmp_query_filepath, "w") as file:
            file.write(f"""
                PRAGMA enable_profiling='json';
                PRAGMA profiling_output = '{profile_filepath}';
                {query};
            """)
        os.system(f"duckdb --readonly imdb/db.duckdb < {tmp_query_filepath} > /dev/null")
        os.remove(tmp_query_filepath)

        with open(profile_filepath, "r") as file:
            profile = file.read()
        os.remove(profile_filepath)

        num_joins = profile.count('"operator_type": "HASH_JOIN"') - profile.count('"operator_type": "COLUMN_DATA_SCAN"')
        
        stats = {
            "num_joins": num_joins,
            "template": extract_template_from_filepath(filepath),
        }
        
        with lock:
            query_stats[filepath] = stats

    def _worker(self, file_queue, query_stats, lock):
        while True:
            try:
                filepath = file_queue.get_nowait()
            except queue.Empty:
                break
            self._process_file(filepath, query_stats, lock)
            file_queue.task_done()

    def _process_dir(self, dir_path, query_stats, num_threads=48):
        os.makedirs("tmp", exist_ok=True)

        file_queue = queue.Queue()
        lock = threading.Lock()

        for filename in os.listdir(dir_path):
            filepath = os.path.join(dir_path, filename)
            file_queue.put(filepath)

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=self._worker, args=(file_queue, query_stats, lock))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        os.system("rm -rf tmp")

    def _is_valid(self, benchmark_name):
        assert self.db.execute(
            f"SELECT COUNT(*) FROM {benchmark_name}_stats"
        ).fetchone()[0] > 0, f"Query stats for {benchmark_name} not set up."
        assert self.db.execute(f"""
            SELECT COUNT(*)
            from {benchmark_name}_stats q1, {benchmark_name}_stats q2
            WHERE
                q1.template == q2.template AND
                q1.num_joins != q2.num_joins
        """).fetchone()[0] == 0, f"Two queries with same template must have same number of joins, benchmark={benchmark_name}."

    def _insert_stats(self, filepath, query_stats, benchmark_name):
        self.db.execute(
            f"""
            INSERT INTO {benchmark_name}_stats
            VALUES (
                '{filepath}',
                {query_stats["num_joins"]},
                '{query_stats["template"]}'
            )
        """
        )

    def get(
        self, benchmark_name, bounds=(MIN_NUM_JOINS_ALLOWED, MAX_NUM_JOINS_ALLOWED)
    ):
        benchmark_stats = {}
        for row in (
            self.db.execute(f"SELECT * FROM {benchmark_name}_stats")
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
        stats = self.get(benchmark_name, bounds=None)
        benchmark_name = (
            benchmark_name.upper() if benchmark_name != "ceb_job" else "CEB+"
        )
        map_n_joins_to_templates = defaultdict(set)
        map_n_joins_to_number_of_queries = defaultdict(int)
        for filepath, single_query_stats in stats.items():
            template = extract_template_from_filepath(filepath)
            map_n_joins_to_templates[single_query_stats["num_joins"]].add(
                template
            )
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
            {
                k: len(map_n_joins_to_templates[k])
                for k in map_n_joins_to_templates
            },
            "Number of joins",
            f"Number of templates",
            f"Number of templates in {benchmark_name} per number of joins",
            os.path.join(dir_path, "templates.png"),
        )

        log(
            f"Number of distinct query instances in {benchmark_name}: {sum(map_n_joins_to_number_of_queries.values())}",
            verbose=self.verbose,
        )
        log(
            f"Number of templates in {benchmark_name}: {len(set(sum(list(map(list, map_n_joins_to_templates.values())), [])))}",
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
