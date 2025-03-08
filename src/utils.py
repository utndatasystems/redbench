import os
from collections import defaultdict
import duckdb
import matplotlib.pyplot as plt
import logging
import re


DB_FILEPATH = "db.duckdb"
JOB_DIR_PATH = "imdb/benchmarks/job/"
CEB_DIR_PATH = "imdb/benchmarks/ceb/"
IMDB_DB_FILEPATH = "imdb/db.duckdb"


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(message)s", "%H:%M:%S")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


def extract_readset_from_query(filepath):
    with open(filepath, "r") as file:
        sql = " ".join(file.readlines())
    tokens = list(map(lambda x: x.lower(), sql.split()))
    tokens = sum(
        map(lambda x: re.split(r"(,)", x) if not x.startswith("'") else [x], tokens), []
    )
    assert "join" not in tokens

    tables = []
    for idx in range(tokens.index("from"), len(tokens)):
        if tokens[idx] in (",", "from"):
            tables.append(tokens[idx + 1])
        elif tokens[idx] == "where":
            break
    return ",".join(sorted(tables))


def get_sub_directories(directory):
    return [x[0] for x in os.walk(directory) if x[0] != directory]


def map_num_joins_to_ceb_queries(query_stats):
    map_n_joins_to_queries = defaultdict(list)
    for filepath, stats in query_stats.items():
        map_n_joins_to_queries[stats["num_joins"]].append(filepath)
    return map_n_joins_to_queries


def map_num_joins_to_ceb_readsets(query_stats):
    res = defaultdict(list)
    for _, stats in query_stats.items():
        if stats["readset"] not in res[stats["num_joins"]]:
            res[stats["num_joins"]].append(stats["readset"])
    return res


def map_ceb_readsets_to_ceb_queries(query_stats):
    res = defaultdict(list)
    for filepath, stats in query_stats.items():
        res[stats["readset"]].append(filepath)
    return res


def bound_num_joins(query_stats, min_joins, max_joins):
    return {
        filename: query_stats[filename]
        for filename in query_stats
        if query_stats[filename]["num_joins"] >= min_joins
        and query_stats[filename]["num_joins"] <= max_joins
    }


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


def extract_readset_from_string(user_query):
    if user_query["read_table_ids"] is None:
        return []
    return tuple(sorted(map(int, user_query["read_table_ids"].split(","))))


def get_experiment_db():
    db = duckdb.connect(DB_FILEPATH)
    db.execute("INSTALL httpfs")
    db.execute("LOAD httpfs;")
    return db


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
        process = subprocess.run([duckdb_cli, '--version'], capture_output=True, text=True, check=True)
        version_str = process.stdout.strip()
        parts = version_str.split()
        return parts[0] if len(parts) > 0 else None
    except FileNotFoundError:
        print(f"Error: DuckDB binary '{duckdb_cli}' not found.")
    except subprocess.CalledProcessError:
        print(f"Error: Failed to execute '{duckdb_cli} --version'.")
