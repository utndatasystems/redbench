from abc import ABC, abstractmethod
from collections import defaultdict
from ..utils import log, draw_bar_plot
import os


FIGURES_DIR = "figures/benchmarks"


class Benchmark(ABC):
    def __init__(self, **kwargs):
        self.duckdb_cli = kwargs.get("duckdb_cli", None)
        self.stats_db = kwargs.get("stats_db", None)

    @abstractmethod
    def setup(self, override):
        assert False, "Not implemented"

    @abstractmethod
    def get_stats(self):
        assert False, "Not implemented"

    @abstractmethod
    def compute_stats(self, override):
        assert False, "Not implemented"

    @abstractmethod
    def dump_plots(self):
        assert False, "Not implemented"

    @abstractmethod
    def normalize_num_joins(self, num_joins):
        assert False, "Not implemented"

    def get_stats(
        self,
        benchmark_name=None,
        bound_num_joins=True,
    ):
        benchmark_name = benchmark_name or self.benchmark_name
        benchmark_stats = {}
        for row in (
            # We order by filepath to ensure that Redbench generation is
            # deterministic even across different DuckDB versions.
            self.stats_db.execute(
                f"""
                SELECT * FROM {benchmark_name}_stats ORDER BY filepath
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
            self._bound_num_joins(benchmark_stats)
            if bound_num_joins
            else benchmark_stats
        )

    def normalize_num_joins(self, num_joins):
        return int(
            num_joins * (self.max_num_joins - self.min_num_joins)
            + self.min_num_joins
            + 0.5
        )

    def dump_plots(self, benchmark_name=None):
        benchmark_name = benchmark_name or self.benchmark_name
        dir_path = f"{FIGURES_DIR}/{benchmark_name}"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        self._dump_plots(benchmark_name, dir_path)
        log(
            f"{benchmark_name} plots dumped to {dir_path}.",
        )

    def get_name(self):
        return self.benchmark_name

    def _create_stats_tables(self, benchmark_name=None):
        benchmark_name = benchmark_name or self.benchmark_name
        self.stats_db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {benchmark_name}_stats (
                filepath VARCHAR,
                num_joins INTEGER,
                template VARCHAR
            )
        """
        )

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
            tmp_query_filepath = f"/tmp/query.sql"
            profile_filepath = f"/tmp/query_profile.json"
            with open(tmp_query_filepath, "w") as file:
                file.write(
                    f"""
                    PRAGMA enable_profiling='json';
                    PRAGMA profiling_output = '{profile_filepath}';
                    {query};
                """
                )
            exit_code = os.system(
                f"{self.duckdb_cli} {self.db_filepath} < {tmp_query_filepath} > /dev/null 2>&1"
            )
            if exit_code != 0:
                os.remove(tmp_query_filepath)
                if os.path.exists(profile_filepath):
                    os.remove(profile_filepath)
                continue
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

    def _insert_stats(self, filepath, query_stats, benchmark_name=None):
        benchmark_name = benchmark_name or self.benchmark_name
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

    def _bound_num_joins(self, query_stats):
        return {
            filename: query_stats[filename]
            for filename in query_stats
            if query_stats[filename]["num_joins"] >= self.min_num_joins
            and query_stats[filename]["num_joins"] <= self.max_num_joins
        }

    def _is_stats_setup(self, benchmark_name=None):
        benchmark_name = benchmark_name or self.benchmark_name
        return self.stats_db.execute(
            f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = '{benchmark_name}_stats'
        """
        ).fetchone()[0] > 0 and self.stats_db.execute(
            f"""
            SELECT COUNT(*) FROM {benchmark_name}_stats
        """
        ).fetchone()[0] > 0

    def _dump_plots(self, benchmark_name, dir_path):
        stats = self.get_stats(bound_num_joins=False, benchmark_name=benchmark_name)
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
