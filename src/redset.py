import duckdb
from .utils import *


REDSET_FILEPATHS = {
    "provisioned": "https://s3.amazonaws.com/redshift-downloads/redset/provisioned/full.parquet",
    "serverless": "https://s3.amazonaws.com/redshift-downloads/redset/serverless/full.parquet",
}


def get_redset_filepaths(version):
    """
    Get the Redset filepaths for the given version.

    Args:
        version (str): The Redset version.

    Returns:
        dict: The Redset version names and their corresponding filepaths.
    """
    assert version in REDSET_FILEPATHS.keys() or version == "both"
    return (
        REDSET_FILEPATHS if version == "both" else {version: REDSET_FILEPATHS[version]}
    )


class Redset:
    """
    Download, prefilter, and ingest Redset into a new table 'redset' in the provided db.

    Args:
        db (duckdb.DuckDB): The DuckDB database used by the experiments.
        override (bool): Whether to override the table 'redset' if exists.
    """

    def __init__(self, db, version, override=False, verbose=False):
        self.db = db
        self.version = version
        self.override = override
        self.verbose = verbose

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

    def setup(self):
        if not self.override and self._is_setup():
            log("Redset already set up.")
            return
        log(
            f"Downloading and prefiltering Redset version '{self.version}'. This may take a few minultes...",
        )
        redset_subquery = " union all ".join(
            f"""
            select
                *,
                concat('{redset_version}#', user_id, '#', instance_id) as user_key,
            from '{path}'
        """
            for redset_version, path in get_redset_filepaths(self.version).items()
        )
        # self.db.execute(
        #     f"""CREATE TABLE IF NOT EXISTS raw_redset AS ({redset_subquery})"""
        # )
        self.db.execute(
            f"""
            CREATE OR REPLACE TABLE redset AS (
                with raw_redset as (
                    {redset_subquery}
                ), days as (
                    SELECT generate_series as day_start
                    from generate_series('2024-03-04 08:00:00'::timestamp, '2025-01-01'::timestamp, INTERVAL 1 WEEK)
                ), queries_per_day as (
                    select user_key, day_start, count(*) as num_queries
                    from raw_redset, days
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
                ), redset_1 as (
                    SELECT
                        *,
                        '' as query_hash
                    FROM raw_redset T, best_day
                    WHERE
                        T.user_key = best_day.user_key
                        and T.num_external_tables_accessed = 0
                        and T.num_system_tables_accessed = 0
                        and T.query_type = 'select'
                        and T.num_joins > 0
                        and T.read_table_ids is not null
                        and T.was_cached = 0
                        and arrival_timestamp >= best_day.day_start
                        and arrival_timestamp < best_day.day_start + INTERVAL 105 HOURS
                ), ordered_queries AS (
                    SELECT 
                        user_key,
                        query_id, 
                        ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY arrival_timestamp ASC) AS rank
                    FROM redset_1
                ), redset_2 as (
                    select redset_1.*
                    from redset_1, ordered_queries
                    where redset_1.user_key = ordered_queries.user_key
                        and redset_1.query_id = ordered_queries.query_id
                        and ordered_queries.rank <= 1000
                ), eliminated_users as (
                    select user_key
                    from redset_2
                    where
                        (
                            CASE
                                WHEN read_table_ids is NULL THEN 0
                                ELSE (
                                    LENGTH(read_table_ids)
                                    - LENGTH(REPLACE(read_table_ids, ',', ''))
                                    + 1
                                )
                            END
                        ) > num_joins + 1
                    group by user_key
                    UNION
                    select user_key
                    from redset_2
                    group by user_key
                    having max(num_joins) == min(num_joins) or max(num_joins) - min(num_joins) > 18
                )
                select *
                from redset_2
                where user_key not in (select * from eliminated_users)
            )
        """
        )
        log("Computing Redset query readsets and hashes...")
        queries = (
            self.db.execute("SELECT * FROM redset").fetchdf().to_dict(orient="records")
        )
        for query in queries:
            readset = ",".join(map(str, extract_readset_from_string(query)))
            self.db.execute(
                f"""
                UPDATE redset
                SET
                    query_hash = concat(feature_fingerprint, '#', num_scans, '#', num_joins, '#{readset}')
                WHERE user_key = '{query["user_key"]}' AND query_id = {query["query_id"]}
            """
            )

    def dump_stats(self):
        num_queries = self.db.execute("SELECT COUNT(*) FROM redset").fetchone()[0]
        log(
            f"Number of queries in prefiltered Redset: {num_queries}",
            verbose=self.verbose,
        )
        num_users = self.db.execute(
            "SELECT COUNT(*) FROM (select user_key from redset group by user_key)"
        ).fetchone()[0]
        log(f"Number of users in prefiltered Redset: {num_users}", verbose=self.verbose)
