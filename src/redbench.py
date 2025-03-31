from collections import defaultdict
import random
import os
import copy
from .utils import *
import numpy as np


random.seed(0)


WORKLOADS_DIR = "workloads"


class Redbench:
    def __init__(self, benchmark, db=None):
        self.db = db
        self.benchmark = benchmark

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
        title,
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
        plt.title("\n".join(wrap(title, 60)))
        plt.legend()
        plt.grid()
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
                    order by variability, user_key asc
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
                f"Ranking of each user in the group {group_id} by number of distinct num_joins and readsets",
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
                f"Number of distinct num_joins and readsets for each user in the group {group_id}",
            )
            sampled_users[group_id] = sample
        return sampled_users

    def exists(self):
        return (
            os.path.exists(WORKLOADS_DIR)
            and len(os.listdir(WORKLOADS_DIR)) > 0
            and all(
                map(
                    lambda subdir: any(
                        map(lambda file: file.endswith(".sql"), os.listdir(subdir))
                    ),
                    get_sub_directories(WORKLOADS_DIR),
                )
            )
        )

    def generate(self, override=True):
        if not override and self.exists():
            log("Redbench already generated.")
            return
        os.system("rm -rf {WORKLOADS_DIR}")
        log("Generating Redbench..")
        benchmark_stats = self.benchmark.get_stats()
        self.num_joins_to_ceb_queries = map_num_joins_to_ceb_queries(benchmark_stats)
        self.ceb_template_to_ceb_queries = map_ceb_template_to_ceb_queries(
            benchmark_stats
        )
        self.num_joins_to_ceb_templates = map_num_joins_to_ceb_templates(
            benchmark_stats
        )
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
        ceb_template_to_unused_queries = copy.deepcopy(self.ceb_template_to_ceb_queries)
        num_joins_to_unmapped_ceb_templates = copy.deepcopy(
            self.num_joins_to_ceb_templates
        )
        assert not any([len(v) == 0 for _, v in ceb_template_to_unused_queries.items()])

        # Iterate over all queries in the user's query timeline
        sampled_benchmark = []
        query_hash_to_ceb_query = dict()
        readset_to_ceb_template = dict()
        for user_query in queries_timeline:
            # Normalize & denormalize number of joins -> get corresponding number of joins for CEB+ queries
            old_num_joins = user_query["num_joins"]
            num_joins = self.benchmark.normalize_num_joins(
                (user_query["num_joins"] - user_stats["min_num_joins"])
                / (user_stats["max_num_joins"] - user_stats["min_num_joins"])
            )
            user_query["num_joins"] = num_joins

            # Sample a single query
            benchmark_query = self._sample_single_query(
                user_query,
                sampling_stats,
                ceb_template_to_unused_queries,
                num_joins_to_unmapped_ceb_templates,
                query_hash_to_ceb_query,
                readset_to_ceb_template,
            )
            assert benchmark_query is not None

            sampled_benchmark.append(
                f"{benchmark_query},{old_num_joins},{num_joins},{user_query['query_id']}"
            )
            sampling_stats[user_stats["user_key"]]["num_queries"] = len(
                sampled_benchmark
            )
        self._write_benchmark_file_to_disk(user_stats, sampled_benchmark)

    def _sample_single_query(
        self,
        user_query,
        sampling_stats,
        ceb_template_to_unused_queries,
        num_joins_to_unmapped_ceb_templates,
        query_hash_to_ceb_query,
        readset_to_ceb_template,
    ):
        user_query_hash, num_joins = user_query["query_hash"], user_query["num_joins"]
        user_query_readset = get_readset_from_user_query(user_query)
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
                templates_pool = copy.deepcopy(
                    self.num_joins_to_ceb_templates[num_joins]
                )
                random.shuffle(templates_pool)
                for ceb_template in templates_pool:
                    # (6): Pick a random, already mapped, CEB+ template with unused query instances
                    if (
                        not ceb_template in num_joins_to_unmapped_ceb_templates
                        and len(ceb_template_to_unused_queries[ceb_template]) > 0
                    ):
                        benchmark_query = ceb_template_to_unused_queries[
                            ceb_template
                        ].pop()
                        final_step = "6"
                        break
                # (7): No already mapped CEB+ templates with remaining query instances
                #  -> just pick a random query instance
                if benchmark_query is None:
                    final_step = "7"
                    benchmark_query = random.choice(
                        self.num_joins_to_ceb_queries[num_joins]
                    )
                return benchmark_query, final_step

            # We have already encountered this readset (1)
            if user_query_readset in readset_to_ceb_template:
                corresponding_ceb_template = readset_to_ceb_template[user_query_readset]
                remaining_ceb_query_instances_for_template = (
                    ceb_template_to_unused_queries[corresponding_ceb_template]
                )
                if len(remaining_ceb_query_instances_for_template) > 0:
                    used_sampling_step = "2"
                    benchmark_query = (
                        remaining_ceb_query_instances_for_template.pop()
                    )  # (2)
                else:
                    benchmark_query, final_step = step_6()
                    used_sampling_step = f"3 -> {final_step}"  # (3)
            # This readset has never occured before (4)
            else:
                if len(num_joins_to_unmapped_ceb_templates[num_joins]) > 0:
                    # We still have unmapped CEB+ templates
                    # Look for the one with the most number of remaining queries
                    best, best_value = None, 0
                    for candidate_template in num_joins_to_unmapped_ceb_templates[
                        num_joins
                    ]:
                        this_value = len(
                            ceb_template_to_unused_queries[candidate_template]
                        )
                        if this_value > best_value:
                            best_value = this_value
                            best = candidate_template
                    if best_value > 0:
                        # We found one with some remaining queries
                        corresponding_ceb_template = best
                        assert (
                            len(
                                ceb_template_to_unused_queries[
                                    corresponding_ceb_template
                                ]
                            )
                            > 0
                        )

                        # Mark the CEB+ template as mapped/ remove it from the unmapped list
                        count = 0
                        for (
                            _,
                            unmapped_templates_list,
                        ) in num_joins_to_unmapped_ceb_templates.items():
                            if corresponding_ceb_template in unmapped_templates_list:
                                unmapped_templates_list.remove(
                                    corresponding_ceb_template
                                )
                                count += 1
                        assert (
                            count == 1
                        ), f"The same template {corresponding_ceb_template} produces different num_joins"

                        # Add the mapping readset -> CEB+ template
                        readset_to_ceb_template[user_query_readset] = (
                            corresponding_ceb_template
                        )

                        # Use one of the unused query instances
                        benchmark_query = ceb_template_to_unused_queries[
                            corresponding_ceb_template
                        ].pop()
                        used_sampling_step = "5"
                # All CEB+ templates have already been mapped (6)
                if benchmark_query is None:
                    benchmark_query, final_step = step_6()
                    used_sampling_step = f"4 -> {final_step}"
            sampling_stats[user_query["user_key"]][used_sampling_step] += 1
            query_hash_to_ceb_query[user_query_hash] = benchmark_query
        return benchmark_query

    def _dump_sampling_stats(self, group_id, users_sample, sampling_stats):
        possible_sampling_paths = ["2", "5", "3 -> 6", "3 -> 7", "4 -> 6", "4 -> 7"]
        with open(
            f"{WORKLOADS_DIR}/{group_id}/stats.csv",
            "w",
        ) as file:
            file.write(
                "workload_type,user_id,instance_id,number_of_queries,query_repetition_rate,n_distinct_num_joins,n_distinct_readsets,"
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
        dir_path = f"{WORKLOADS_DIR}/{user_stats['group_id']}"
        csv_header = (
            "filepath,num_joins_in_user_query,num_joins_in_benchmark_query,query_id\n"
        )

        os.makedirs(dir_path, exist_ok=True)
        filepath = f"{dir_path}/{user_stats['workload_type']}"
        with open(f"{filepath}.csv", "w") as file:
            file.write(csv_header)
            file.write("\n".join(sampled_benchmark))
