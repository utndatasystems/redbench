from .benchmark import Benchmark
import re
import os
from ..utils import log

TPCDS_DB_FILEPATH = "tpcds/db.duckdb"

MIN_NUM_JOINS_ALLOWED = 2
MAX_NUM_JOINS_ALLOWED = 7

def setup_tpcds_db(duckdb_cli, override=False, scale=1):
    if not override and os.path.exists(TPCDS_DB_FILEPATH):
        log("TPC-DS database already set up.")
        return
    os.system(f'[ -f "{TPCDS_DB_FILEPATH}" ] && rm "{TPCDS_DB_FILEPATH}"')
    log("Downloading and setting up the TPC-DS database. This may take a few minutes.")
    os.system(f"{duckdb_cli} {TPCDS_DB_FILEPATH} -c 'CALL dsdgen(sf = {scale})'")

class TPCDSBenchmark(Benchmark):
    def __init__(self, scale=1, **kwargs):
        super().__init__(**kwargs)
        self.scale = scale
        self.min_num_joins = MIN_NUM_JOINS_ALLOWED
        self.max_num_joins = MAX_NUM_JOINS_ALLOWED
        self.db_filepath = TPCDS_DB_FILEPATH
        self.benchmark_name = f"tpcds_{scale}gb"
        self.queries_dir_path = f"tpcds/{scale}gb/queries"

    def _extract_template_from_filepath(self, filepath):
        assert self.queries_dir_path in filepath, filepath
        return filepath.split(self.queries_dir_path)[1].split("/")[1]

    def _replace_days_with_interval(self, sql):
        pattern = r"(\d+)\s+days"
        replacement = r"INTERVAL '\1' DAY"
        return re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    def setup(
        self, override=False, target_os="LINUX"
    ):
        if not override and os.path.exists(self.queries_dir_path):
            log("TPC-DS benchmark already set up.")
            return
        log("Setting up TPC-DS benchmark...")
        os.system(f"rm -rf {self.queries_dir_path}")
        # TODO: Add in README that people need to install the dependencies
        os.system("cd tpcds && git clone git@github.com:skander-krid/tpcds-kit.git")
        os.system(f"cd tpcds/tpcds-kit/tools && make OS={target_os}")
        os.system(f"cd tpcds/tpcds-kit/tools && ./dsdgen -scale {self.scale} -verbose")
        os.system(f"mkdir -p {self.queries_dir_path}")

        for template_num in range(1, 100):
            os.system(
                f"""
                cd tpcds/tpcds-kit/tools &&
                ./dsqgen \
                    -DIRECTORY ../query_templates \
                    -INPUT ../query_templates/templates.lst \
                    -VERBOSE Y \
                    -QUALIFY Y \
                    -SCALE {self.scale} \
                    -DIALECT netezza \
                    -OUTPUT_DIR /tmp \
                    -template query{template_num}.tpl \
                    -count 1000
            """
            )

            with open("/tmp/query_0.sql", "r") as file:
                queries = file.readlines()
            os.remove("/tmp/query_0.sql")
            queries = [line.strip() for line in queries]
            queries = " ".join(queries)
            queries = self._replace_days_with_interval(queries)
            queries = queries.split(";")
            queries = [line.strip() for line in queries if len(line.strip()) > 0]
            queries = set(queries) # Eliminate duplicates

            os.system(f"rm -rf {self.queries_dir_path}/{template_num}")
            os.system(f"mkdir -p {self.queries_dir_path}/{template_num}")
            for i, query in enumerate(queries):
                with open(f"{self.queries_dir_path}/{template_num}/{i}.sql", "w") as file:
                    file.write(query)

    def compute_stats(self, override=False):
        if not override and self._is_stats_setup():
            log("TPC-DS benchmark stats already set up.")
            return
        log("Computing TPC-DS benchmark stats..")

        self._create_stats_tables()

        benchmark_stats = dict()
        for template_num in range(1, 100):
            self._process_dir(
                f"{self.queries_dir_path}/{template_num}",
                benchmark_stats,
            )

        for filepath, stats in benchmark_stats.items():
            self._insert_stats(filepath, stats)
