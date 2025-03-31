from .utils import *
from .user_stats import UserStats
import src.benchmarks.imdb as benchmark


MAX_ALLOWED_NUM_JOINS_GAP = (benchmark.MAX_NUM_JOINS_ALLOWED - benchmark.MIN_NUM_JOINS_ALLOWED) * 2 + 1


REDSET_FILEPATH = (
    "https://s3.amazonaws.com/redshift-downloads/redset/provisioned/full.parquet"
)


class Redset:
    """
    Download, prefilter, and ingest Redset into a new table 'redset' in the provided db.

    Args:
        db (duckdb.DuckDB): The DuckDB database used by the experiments.
        override (bool): Whether to override the table 'redset' if exists.
        verbose (bool): Whether to print extra stats on the Redset dataset.
    """

    def __init__(self, db):
        self.db = db
        self.user_stats = None


    def _is_setup(self):
        """
        Whether the Redset table is already set up.
        """
        return (
            self.db.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'redset'"
            ).fetchone()[0]
            > 0
            and self.db.execute("SELECT COUNT(*) FROM redset").fetchone()[0] > 0
        )

    def setup(self, override=False):
        # Download and prefilter Redset
        self._setup(override)
        self._dump_stats()

        # Compute user stats
        self.user_stats = UserStats(self.db)
        self.user_stats.setup(override)

    def _setup(self, override):
        if not override and self._is_setup():
            log("Redset already set up.")
            return
        log(
            f"Downloading and prefiltering Redset. This may take a few minutes..",
        )

        self.db.execute(
            f"""
                CREATE TABLE IF NOT EXISTS raw_redset AS (
                    select
                        *,
                        concat(user_id, '#', instance_id) as user_key,
                    from '{REDSET_FILEPATH}'
                )
            """
        )

        self.db.execute(
            f"""
            CREATE OR REPLACE TABLE redset AS (
                with redset_1 as (
                    SELECT
                        *,
                        '' as query_hash
                    FROM raw_redset T
                    WHERE
                        T.num_external_tables_accessed = 0
                        and T.num_system_tables_accessed = 0
                        and T.query_type = 'select'
                        and T.num_joins > 0
                        and T.read_table_ids is not null
                        and T.was_cached = 0
                ), days as (
                    SELECT generate_series as day_start
                    from generate_series('2024-03-04 08:00:00'::timestamp, '2025-01-01'::timestamp, INTERVAL 1 WEEK)
                ), queries_per_day as (
                    select user_key, day_start, count(*) as num_queries
                    from redset_1, days
                    where arrival_timestamp >= day_start
                        and arrival_timestamp < day_start + INTERVAL 105 HOURS
                    group by user_key, day_start
                ), best_day as (
                    select queries_per_day.user_key, queries_per_day.day_start
                    from
                        queries_per_day,
                        (
                            select user_key, max(num_queries) as num_queries
                            from queries_per_day
                            group by user_key
                        ) max_num_queries
                    where
                        max_num_queries.user_key = queries_per_day.user_key
                        and max_num_queries.num_queries = queries_per_day.num_queries
                        and not exists (
                            select 1
                            from queries_per_day qpd
                            where qpd.user_key = queries_per_day.user_key
                                and qpd.day_start < queries_per_day.day_start
                                and qpd.num_queries = max_num_queries.num_queries
                        )
                ), redset_2 as (
                    SELECT T.*
                    FROM redset_1 T, best_day
                    WHERE
                        T.user_key = best_day.user_key
                        and arrival_timestamp >= best_day.day_start
                        and arrival_timestamp < best_day.day_start + INTERVAL 105 HOURS
                ), ordered_queries AS (
                    SELECT 
                        user_key,
                        query_id, 
                        ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY arrival_timestamp ASC) AS rank
                    FROM redset_2
                ), redset_3 as (
                    select redset_2.*
                    from redset_2, ordered_queries
                    where redset_2.user_key = ordered_queries.user_key
                        and redset_2.query_id = ordered_queries.query_id
                        and ordered_queries.rank <= 1000
                ), eliminated_users as (
                    select user_key
                    from redset_3
                    group by user_key
                    having
                        max(num_joins) == min(num_joins) or
                        max(num_joins) - min(num_joins) > {MAX_ALLOWED_NUM_JOINS_GAP}
                )
                select *
                from redset_3
                where user_key not in (select * from eliminated_users)
            )
        """
        )
        log("Computing Redset query readsets and hashes..")
        queries = (
            self.db.execute("SELECT * FROM redset").fetchdf().to_dict(orient="records")
        )
        for query in queries:
            readset = ",".join(map(str, get_readset_from_user_query(query)))
            self.db.execute(
                f"""
                UPDATE redset
                SET
                    query_hash = concat(feature_fingerprint, '#', num_scans, '#', num_joins, '#{readset}')
                WHERE user_key = '{query["user_key"]}' AND query_id = {query["query_id"]}
            """
            )

    def _dump_stats(self):
        num_queries = self.db.execute("SELECT COUNT(*) FROM redset").fetchone()[0]
        log(
            f"Number of queries in prefiltered Redset: {num_queries}",
        )
        num_users = self.db.execute(
            "SELECT COUNT(*) FROM (select user_key from redset group by user_key)"
        ).fetchone()[0]
        log(f"Number of users in prefiltered Redset: {num_users}")

    def dump_plots(self):
        assert self.user_stats is not None, "User stats not set up. Please run setup() first."
        self.user_stats.dump_plots()