import duckdb
from .benchmark_stats import (
    BenchmarkStats,
    MIN_NUM_JOINS_ALLOWED,
    MAX_NUM_JOINS_ALLOWED,
)
from collections import defaultdict
import random
import os
import copy
from .utils import *
import pandas
import json
from prettytable import PrettyTable
from datetime import timedelta
import numpy as np


random.seed(10)

PROFILES_DIR = "_tmp_profiles"


def normalize_num_joins(num_joins, min_num_joins, max_num_joins):
    return (num_joins - min_num_joins) / (max_num_joins - min_num_joins)


def denormalize_num_joins(num_joins):
    return int(
        num_joins * (MAX_NUM_JOINS_ALLOWED - MIN_NUM_JOINS_ALLOWED)
        + MIN_NUM_JOINS_ALLOWED
        + 0.5
    )


class Redbench:
    def __init__(self, db=None, override=False):
        self.db = db
        self.override = override

    def _plot_sampling_decision(
        self,
        users,
        sampled_users,
        group_id,
        stat_1,
        stat_2,
        dir_name,
        stats_1_name,
        stat_2_name,
        draw_line=False,
    ):
        os.makedirs(f"figures/redbench/{dir_name}", exist_ok=True)
        plt.scatter(
            [user[stat_1] for user in users],
            [user[stat_2] for user in users],
            color="gray",
        )
        for idx, user in enumerate(sampled_users):
            plt.scatter(user[stat_1], user[stat_2], label=user["workload_type"])

        if draw_line and len(sampled_users) == 3:
            x = np.linspace(0, max([user[stat_1] for user in users]), 400)
            plt.plot(
                x,
                sampled_users[1][stat_1] + sampled_users[1][stat_2] - x,
                color="black",
                linestyle="--",
            )

        plt.xlabel(stats_1_name)
        plt.ylabel(stat_2_name)
        plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig(f"figures/redbench/{dir_name}/{group_id}.png")
        plt.close()

    def _sample_users(self):
        repetition_rates = [(hi / 100 - 0.1, hi / 100) for hi in range(10, 101, 10)]
        sampled_users = dict()  # group id -> list of the 5 user's stats
        for rep_lo, rep_hi in repetition_rates:
            group_id = f"{int(rep_lo * 100)}%-{int(rep_hi * 100)}%"
            users_list = (
                self.db.execute(
                    f"""
                    select
                        *,
                        '{group_id}' as group_id,
                        rank() over (order by num_distinct_num_joins asc) as rank_1,
                        rank() over (order by num_distinct_readsets asc) as rank_2,
                        rank_1 + rank_2 as variability
                    from user_stats
                    where
                        query_repetition_rate between {rep_lo} and {rep_hi}
                    order by variability asc
                """
                )
                .fetchdf()
                .to_dict(orient="records")
            )
            sample = (users_list[0], users_list[len(users_list) // 2], users_list[-1])
            sample[0]["workload_type"] = "low_variability"
            sample[1]["workload_type"] = "mid_variability"
            sample[2]["workload_type"] = "high_variability"
            self._plot_sampling_decision(
                users_list,
                sample,
                group_id,
                "rank_1",
                "rank_2",
                "ranks",
                "Ranking by number of distinct num_joins",
                "Ranking by number of distinct readsets",
                draw_line=True,
            )
            self._plot_sampling_decision(
                users_list,
                sample,
                group_id,
                "num_distinct_num_joins",
                "num_distinct_readsets",
                "values",
                "Number of distinct num_joins",
                "Number of distinct readsets",
            )
            sampled_users[group_id] = sample
        return sampled_users

    def exists(self):
        return (
            os.path.exists("redbench")
            and len(os.listdir("redbench")) > 0
            and all(
                map(
                    lambda subdir: any(
                        map(lambda file: file.endswith(".sql"), os.listdir(subdir))
                    ),
                    get_sub_directories("redbench"),
                )
            )
        )

    def generate(self):
        if not self.override and self.exists():
            log("Redbench already generated.")
            return
        os.system("rm -rf redbench")
        log("Generating Redbench...")
        benchmark_stats = BenchmarkStats(self.db)
        benchmark_stats = benchmark_stats.get("ceb_job")
        self.num_joins_to_ceb_queries = map_num_joins_to_ceb_queries(benchmark_stats)
        self.ceb_readsets_to_ceb_queries = map_ceb_readsets_to_ceb_queries(
            benchmark_stats
        )
        self.num_joins_to_ceb_readsets = map_num_joins_to_ceb_readsets(benchmark_stats)
        for group_id, users_sample in self._sample_users().items():
            sampling_stats = defaultdict(lambda: defaultdict(int))
            for user in users_sample:
                self._sample_benchmark_for_user(user, sampling_stats)
            self._dump_sampling_stats(group_id, users_sample, sampling_stats)
        log("Finished generating Redbench.")

    def _sample_benchmark_for_user(self, user_stats, sampling_stats):
        # Fetch the query timeline of this user
        queries_timeline = get_queries_timeline_for_user(
            user_stats["user_key"], self.db
        )

        # Prepare maps needed across the sampling process for the user
        ceb_readset_to_unused_queries = copy.deepcopy(self.ceb_readsets_to_ceb_queries)
        num_joins_to_unmapped_ceb_readsets = copy.deepcopy(
            self.num_joins_to_ceb_readsets
        )
        assert not any([len(v) == 0 for _, v in ceb_readset_to_unused_queries.items()])

        # Iterate over all queries in the user's query timeline
        sampled_benchmark = []
        query_hash_to_ceb_query = dict()
        user_readset_to_ceb_readset = dict()
        for user_query in queries_timeline:
            # Normalize & denormalize number of joins -> get corresponding number of joins for CEB+ queries
            normalized_num_joins = normalize_num_joins(
                user_query["num_joins"],
                user_stats["min_num_joins"],
                user_stats["max_num_joins"],
            )
            num_joins = denormalize_num_joins(normalized_num_joins)

            # Sample a single query
            benchmark_query = self._sample_single_query(
                user_query,
                num_joins,
                sampling_stats,
                ceb_readset_to_unused_queries,
                num_joins_to_unmapped_ceb_readsets,
                query_hash_to_ceb_query,
                user_readset_to_ceb_readset,
            )
            assert benchmark_query is not None

            sampled_benchmark.append(
                f"{benchmark_query},{user_query["num_joins"]},{num_joins},{user_query['query_id']}"
            )
            sampling_stats[user_stats["user_key"]]["num_queries"] = len(
                sampled_benchmark
            )
        self._write_benchmark_file_to_disk(user_stats, sampled_benchmark)

    def _sample_single_query(
        self,
        user_query,
        num_joins,
        sampling_stats,
        ceb_readset_to_unused_queries,
        num_joins_to_unmapped_ceb_readsets,
        query_hash_to_ceb_query,
        user_readset_to_ceb_readset,
    ):
        user_query_hash = user_query["query_hash"]
        user_query_readset = extract_readset_from_string(user_query)
        benchmark_query = None
        if (
            user_query_hash in query_hash_to_ceb_query
        ):  # We have seen the same query hash before
            benchmark_query = query_hash_to_ceb_query[user_query_hash]
        elif (
            user_query["query_type"] == "select"
        ):  # We have never seen the query hash before

            def step_6():
                benchmark_query = None
                shuffled_pool = copy.deepcopy(self.num_joins_to_ceb_readsets[num_joins])
                random.shuffle(shuffled_pool)
                for ceb_readset in shuffled_pool:
                    # (6): Pick a random, already mapped, CEB readset with unused query instances
                    if (
                        not ceb_readset in num_joins_to_unmapped_ceb_readsets
                        and len(ceb_readset_to_unused_queries[ceb_readset]) > 0
                    ):
                        benchmark_query = ceb_readset_to_unused_queries[
                            ceb_readset
                        ].pop()
                        final_step = "6"
                        break
                # (7): No already mapped CEB readset with remaining query instances
                #  -> just pick a random query instance
                if benchmark_query is None:
                    final_step = "7"
                    benchmark_query = random.choice(
                        self.num_joins_to_ceb_queries[num_joins]
                    )
                return benchmark_query, final_step

            # We have already encountered this user readset (1)
            if user_query_readset in user_readset_to_ceb_readset:
                corresponding_ceb_readset = user_readset_to_ceb_readset[
                    user_query_readset
                ]
                remaining_ceb_query_instances_for_readset = (
                    ceb_readset_to_unused_queries[corresponding_ceb_readset]
                )
                if len(remaining_ceb_query_instances_for_readset) > 0:
                    used_sampling_step = "2"
                    benchmark_query = (
                        remaining_ceb_query_instances_for_readset.pop()
                    )  # (2)
                else:
                    benchmark_query, final_step = step_6()
                    used_sampling_step = f"3 -> {final_step}"  # (3)
            # This user readset has never occured before (4)
            else:
                if len(num_joins_to_unmapped_ceb_readsets[num_joins]) > 0:
                    # We still have unmapped CEB readsets
                    # Look for the one with the most number of remaining queries
                    best, best_value = None, 0
                    for candidate_readset in num_joins_to_unmapped_ceb_readsets[
                        num_joins
                    ]:
                        this_value = len(
                            ceb_readset_to_unused_queries[candidate_readset]
                        )
                        if this_value > best_value:
                            best_value = this_value
                            best = candidate_readset
                    if best_value > 0:
                        # We found one with some remaining queries
                        corresponding_ceb_readset = best
                        assert (
                            len(
                                ceb_readset_to_unused_queries[corresponding_ceb_readset]
                            )
                            > 0
                        )

                        # Remove the CEB readset from the list of unmapped readset for all other num_joins
                        for (
                            _,
                            unmapped_readsets_list,
                        ) in num_joins_to_unmapped_ceb_readsets.items():
                            if corresponding_ceb_readset in unmapped_readsets_list:
                                unmapped_readsets_list.remove(corresponding_ceb_readset)

                        # Add the mapping user_readset -> CEB readset
                        user_readset_to_ceb_readset[user_query_readset] = (
                            corresponding_ceb_readset
                        )

                        # Use one of the unused query instances
                        benchmark_query = ceb_readset_to_unused_queries[
                            corresponding_ceb_readset
                        ].pop()
                        used_sampling_step = "5"
                # All CEB readsets have already been mapped (6)
                if benchmark_query is None:
                    benchmark_query, final_step = step_6()
                    used_sampling_step = f"4 -> {final_step}"
            sampling_stats[user_query["user_key"]][used_sampling_step] += 1
            query_hash_to_ceb_query[user_query_hash] = benchmark_query
        return benchmark_query

    def _dump_sampling_stats(self, group_id, users_sample, sampling_stats):
        possible_sampling_paths = ["2", "5", "3 -> 6", "3 -> 7", "4 -> 6", "4 -> 7"]
        with open(
            f"redbench/{group_id}/stats.csv",
            "w",
        ) as file:
            file.write(
                "workload_type,redset_version,user_id,instance_id,number_of_queries,query_repetition_rate,n_distinct_num_joins,n_distinct_readsets,"
                + ",".join(
                    map(
                        lambda x: "n_occurrences_step_"
                        + x.replace(" ", "").replace("->", "_to_"),
                        possible_sampling_paths,
                    )
                )
                + "\n"
            )
            for user_infos in users_sample:
                user_key = user_infos["user_key"]
                file.write(
                    ",".join(
                        [
                            user_infos["workload_type"],
                            parse_user_key(user_key)["redset_version"],
                            str(parse_user_key(user_key)["user_id"]),
                            str(parse_user_key(user_key)["instance_id"]),
                            str(sampling_stats[user_key]["num_queries"]),
                            f'{user_infos["query_repetition_rate"]:.3f}',
                            str(user_infos["num_distinct_num_joins"]),
                            str(user_infos["num_distinct_readsets"]),
                        ]
                    )
                )
                for sampling_path in possible_sampling_paths:
                    value = sampling_stats[user_key][sampling_path]
                    file.write("," + str(value))
                file.write("\n")

    def _write_benchmark_file_to_disk(self, user_stats, sampled_benchmark):
        dir_path = f"redbench/{user_stats['group_id']}"
        csv_header = (
            "filepath,num_joins_in_user_query,num_joins_in_benchmark_query,query_id\n"
        )

        os.makedirs(dir_path, exist_ok=True)
        filepath = f"{dir_path}/{user_stats['workload_type']}"
        with open(f"{filepath}.csv", "w") as file:
            file.write(csv_header)
            file.write("\n".join(sampled_benchmark))
        with open(f"{filepath}.sql", "w") as file:
            file.write("PRAGMA enable_profiling='json';\n")
            for i, line in enumerate(sampled_benchmark):
                file.write(
                    f"PRAGMA profile_output='{PROFILES_DIR}/{i}_profile.json';\n"
                )
                file.write(f".read {line.split(',')[0]}\n")

    def run(self):
        exec_times = dict()
        num_queries = defaultdict(int)
        for subdir in sorted(get_sub_directories("redbench")):
            group_name = os.path.basename(subdir)
            log(
                f"Running RedBench group {group_name}...",
            )
            exec_time = 0
            for filename in os.listdir(subdir):
                if not filename.endswith(".sql"):
                    continue
                os.makedirs(PROFILES_DIR, exist_ok=True)
                filepath = os.path.join(subdir, filename)
                os.system(
                    f"duckdb --readonly imdb/db.duckdb < {filepath} >/dev/null 2>&1"
                )
                for profile_name in os.listdir(PROFILES_DIR):
                    with open(os.path.join(PROFILES_DIR, profile_name), "r") as file:
                        query_profile = json.load(file)
                    exec_time += float(query_profile["latency"])
                    num_queries[group_name] += 1
                os.system(f"rm -rf {PROFILES_DIR}")
            exec_times[group_name] = exec_time
        results_table = PrettyTable()
        results_table.field_names = [
            "Repetition rate",
            "Total execution time",
            "Average query execution time",
        ]
        for group_name, exec_time in exec_times.items():
            results_table.add_row(
                [
                    group_name,
                    str(timedelta(seconds=exec_time)),
                    str(timedelta(seconds=exec_time / num_queries[group_name])),
                ]
            )
        print(results_table)
