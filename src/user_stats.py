from collections import defaultdict
import matplotlib.pyplot as plt
import os
from .utils import *


class UserStats:
    """
    Aggregates statistics on users from the Redset dataset.
    """

    def __init__(self, db, override=False, verbose=False):
        self.db = db
        self.override = override
        self.verbose = verbose
        self.setup()

    def _first_agg_step(self):
        """
        Computes per-user aggregated stats.
        """

        self.db.execute(
            f"""
            create or replace table user_stats as
            (
                with stats as (
                    select
                        user_key,
                        count(*) total_num_queries,
                        avg(num_joins) as mean_num_joins,
                        VAR_POP(num_joins) as variance_num_joins,
                        min(num_joins) as min_num_joins,
                        max(num_joins) as max_num_joins,
                        count(distinct num_joins) as num_distinct_num_joins,
                        sum(execution_duration_ms) as total_exec_time
                    from redset
                    group by user_key
                ),
                repeated_queries AS (
                    SELECT
                        user_key,
                        sum(count) AS count
                    FROM (
                        SELECT user_key, query_hash, COUNT(*) - 1 AS count
                        FROM redset
                        GROUP BY user_key, query_hash
                        HAVING COUNT(*) > 1
                    ) t
                    GROUP BY user_key
                )
                SELECT
                    stats.*,
                    COALESCE(reps.count, 0) / stats.total_num_queries AS query_repetition_rate,
                    0 as num_distinct_readsets,
                    0 as num_distinct_readset_sizes,
                FROM stats LEFT JOIN repeated_queries reps ON stats.user_key = reps.user_key
            )
        """
        )

    def _second_agg_step(self):
        """
        Compute per-user aggregated readset stats: number of distinct readsets and readset sizes.
        """

        # Find all distinct readsets for each user
        redset_queries = (
            self.db.execute(f"select * from redset").fetchdf().to_dict(orient="records")
        )
        user_to_readsets = defaultdict(set)
        for query in redset_queries:
            assert query["read_table_ids"] is not None
            readset = tuple(sorted(query["read_table_ids"].split(",")))
            user_to_readsets[query["user_key"]].add(readset)

        # Update the users stats with the number of distinct readsets and readset sizes
        for user_key, readsets in user_to_readsets.items():
            num_readsets = len(readsets)
            num_distinct_readset_sizes = len(
                set({len(readset) for readset in readsets})
            )
            self.db.sql(
                f"""
                update user_stats
                    set
                        num_distinct_readsets = {num_readsets},
                        num_distinct_readset_sizes = {num_distinct_readset_sizes}
                    where user_key = '{user_key}'
            """
            )

    def _is_user_stats_collected(self):
        return (
            self.db.execute(
                f"""
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type='table' AND name='user_stats'
        """
            ).fetchone()[0]
            == 1
            and self.db.execute(
                f"""
            SELECT COUNT(*)
            FROM user_stats
        """
            ).fetchone()[0]
            > 0
        )

    def setup(self):
        if not self.override and self._is_user_stats_collected():
            log("User stats already collected.")
            return
        log("Collecting user stats..")
        self._first_agg_step()
        self._second_agg_step()

    def _dump_plot_1(self, dir_path):
        xs_txt = [f"{ratio+10}%" for ratio in range(0, 91, 10)]
        ys = []
        ys2 = []
        ys3 = []
        for rep_ratio in range(10, 101, 10):
            n_users = self.db.execute(
                f"""
                select count(*)
                from user_stats
                where
                    query_repetition_rate between {rep_ratio/100-0.1} and {rep_ratio/100}
            """
            ).fetchone()[0]

            ys.append(n_users)

            n_queries = self.db.execute(
                f"""
                select sum(total_num_queries)
                from user_stats
                where
                    query_repetition_rate between {rep_ratio/100-0.1} and {rep_ratio/100}
            """
            ).fetchone()[0]
            ys2.append(n_queries)

            sum_exec_time = self.db.execute(
                f"""
                select sum(total_exec_time)
                from user_stats
                where
                    query_repetition_rate between {rep_ratio/100-0.1} and {rep_ratio/100}
            """
            ).fetchone()[0]
            ys3.append(sum_exec_time)

        ys = [y / sum(ys) for y in ys]
        ys2 = [y / sum(ys2) for y in ys2]
        ys3 = [y / sum(ys3) for y in ys3]

        for i in range(1, len(ys)):
            ys[i] += ys[i - 1]
            ys2[i] += ys2[i - 1]
            ys3[i] += ys3[i - 1]

        _, ax = plt.subplots()
        ax.plot(xs_txt, ys, marker="o", label="Percentage of users")
        ax.plot(xs_txt, ys2, marker="o", label="Percentage of queries")
        ax.plot(xs_txt, ys3, marker="o", label="Percentage of total execution time")
        ax.axhline(y=1, color="black", linestyle=":")
        ax.set_xlabel("Query repetition rate")
        ax.set_ylabel("eCDF")
        ax.set_title(
            "\n".join(
                wrap(
                    "Cumulative distributions of number of users, number of queries and total execution time over the query repetition groups",
                    60,
                )
            )
        )
        plt.ylim(bottom=0)
        ax.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(dir_path, "01_cdf.png"))
        plt.close()

    def _dump_plot_2(self, dir_path):
        stats = {
            "mean_num_joins": False,
            "total_num_queries": True,
            "num_distinct_readsets": True,
            "num_distinct_readset_sizes": False,
            "num_distinct_num_joins": False,
        }
        for stat, log_scale_y in stats.items():
            ys = []
            for rep_ratio in range(10, 101, 10):
                n_joins_for_bracket = list(
                    self.db.execute(
                        f"""
                        select {stat}
                        from user_stats
                        where
                            query_repetition_rate between {rep_ratio/100-0.1} and {rep_ratio/100}
                        order by {stat}
                    """
                    ).fetchdf()[stat]
                )
                ys.append(n_joins_for_bracket)
            draw_box_plot(
                [f"{rep_ratio}%" for rep_ratio in range(10, 101, 10)],
                ys,
                "Query repetition group",
                f"Distr. of {stat} over users",
                save_path=os.path.join(dir_path, f"02_{stat}.png"),
                log_scale_y=log_scale_y,
                title=f"Distributions of {stat} over users per query repetition groups",
            )

    def dump_plots(self):
        dir_path = "figures/redset/"
        os.makedirs(dir_path, exist_ok=True)
        self._dump_plot_1(dir_path)
        self._dump_plot_2(dir_path)
        log(f"Redset plots dumped to {dir_path}.")
