import os
from collections import defaultdict
import duckdb
import matplotlib.pyplot as plt
import logging
import re


DB_FILEPATH = "db.duckdb"


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(message)s", "%H:%M:%S")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


def get_sub_directories(directory):
    return [x[0] for x in os.walk(directory) if x[0] != directory]


def map_num_joins_to_ceb_queries(query_stats):
    map_n_joins_to_queries = defaultdict(list)
    for filepath, stats in query_stats.items():
        map_n_joins_to_queries[stats["num_joins"]].append(filepath)
    
    # Sort to ensure determinism
    map_n_joins_to_queries = {
        k: sorted(v) for k, v in map_n_joins_to_queries.items()
    }
    return map_n_joins_to_queries


def map_num_joins_to_ceb_templates(query_stats):
    res = defaultdict(list)
    for _, stats in query_stats.items():
        if stats["template"] not in res[stats["num_joins"]]:
            res[stats["num_joins"]].append(stats["template"])

    # Sort to ensure determinism
    res = {k: sorted(v) for k, v in res.items()}
    return res


def map_ceb_template_to_ceb_queries(query_stats):
    res = defaultdict(list)
    for filepath, stats in query_stats.items():
        res[stats["template"]].append(filepath)

    # Sort to ensure determinism
    res = {k: sorted(v) for k, v in res.items()}
    return res


def get_queries_timeline_for_user(user_key, db):
    return (
        db.execute(
            f"""
            select *
            from redset
            where user_key='{user_key}'
            order by arrival_timestamp asc
        """
        )
        .fetchdf()
        .to_dict(orient="records")
    )


def get_readset_from_user_query(user_query):
    if user_query["read_table_ids"] is None:
        return []
    return tuple(sorted(map(int, user_query["read_table_ids"].split(","))))


def get_experiment_db():
    return duckdb.connect(DB_FILEPATH)


def wrap(text, width):
    res = []
    while True:
        if len(text) <= width:
            res.append(text)
            break
        idx = text.find(" ", width)
        if idx == -1:
            res.append(text)
            break
        res.append(text[:idx])
        text = text[idx + 1 :]
    return res


def draw_box_plot(
    xs, ys, xlabel, ylabel, save_path=None, log_scale_y=False, title=None
):
    fig, ax = plt.subplots()
    plt.boxplot(ys, vert=True, patch_artist=True)
    plt.xticks(list(range(1, len(ys) + 1)), xs)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid()
    if log_scale_y:
        ax.set_yscale("log")
    if title is not None:
        plt.title("\n".join(wrap(title, 60)))
    if save_path is not None:
        plt.savefig(save_path)
        plt.close()
    else:
        plt.show()


def draw_bar_plot(
    xs, ys, xlabel, ylabel, save_path=None, log_scale_y=False, title=None
):
    fig, ax = plt.subplots()
    plt.bar(xs, ys)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid()
    if log_scale_y:
        ax.set_yscale("log")
    if title is not None:
        plt.title("\n".join(wrap(title, 60)))
    if save_path is not None:
        plt.savefig(save_path)
        plt.close()
    else:
        plt.show()


def log(line, verbose=True):
    LOGGER.info(line) if verbose else None


def parse_user_key(user_key):
    return {
        "user_id": int(user_key.split("#")[0]),
        "instance_id": int(user_key.split("#")[1]),
    }


def get_duckdb_version(duckdb_cli):
    import subprocess

    try:
        process = subprocess.run(
            [duckdb_cli, "--version"], capture_output=True, text=True, check=True
        )
        version_str = process.stdout.strip()
        parts = version_str.split()
        return parts[0] if len(parts) > 0 else None
    except FileNotFoundError:
        print(f"Error: DuckDB binary '{duckdb_cli}' not found.")
    except subprocess.CalledProcessError:
        print(f"Error: Failed to execute '{duckdb_cli} --version'.")
