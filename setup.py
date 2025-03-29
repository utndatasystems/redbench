from src.benchmarks import setup_benchmarks
from src.imdb import setup_imdb
from src.utils import *
from src.redbench import WORKLOADS_DIR
from collections import defaultdict
import os


# Unpack/ inline the workload queries (convert the csv files to runnable sql files)
def unpack_workloads():
    num_queries = defaultdict(int)
    # Iterate over the query repetition groups
    for subdir in sorted(get_sub_directories(WORKLOADS_DIR)):
        group_name = os.path.basename(subdir)
        # Iterate over the 3 different variability workloads for this group
        for filename in os.listdir(subdir):
            if not filename.endswith(".csv") or filename == "stats.csv":
                continue
            # Read the workload csv
            with open(os.path.join(subdir, filename), "r") as csv_file:
                workload = csv_file.readlines()[1:]
            sql_workload = ""
            # Unpack the queries
            for line in workload:
                num_queries[group_name] += 1
                query_path = line.split(",")[0]
                with open(query_path, "r") as query_file:
                    query = query_file.read().strip()
                    query += ";" if not query.endswith(";") else ""
                    sql_workload += f"-- {query_path}\n{query}\n\n"
            # Write the unpacked workload to a new sql file
            with open(
                os.path.join(subdir, filename.replace(".csv", ".sql")), "w"
            ) as sql_workload_file:
                sql_workload_file.write(sql_workload)
    log("Finished unpacking Redbench workloads.")


if __name__ == "__main__":
    # Setup JOB and CEB
    setup_benchmarks()
    unpack_workloads()
