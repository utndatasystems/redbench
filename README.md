# Redbench

Redbench is a set of 30 analytical SQL workloads that can be used to benchmark workload-driven optimizations. The workloads mimic real-world production queries from [Redset](https://github.com/amazon-science/redset), a dataset of query metadata published by Amazon Redshift, by leveraging the support benchmarks [TPC-DS](https://www.tpc.org/tpcds/), [Join Ordering Benchmark (JOB)](https://github.com/viktorleis/job), and [Cardinality Estimation Benchmark (CEB)](https://github.com/learnedsystems/CEB).

## Motivation

Recent trends in the database community highlight the repetitive nature of real-world workloads and how these recurring query patterns can be leveraged for optimization. However, existing benchmarks such as JOB and CEB do not accurately represent recurring query patterns. Redbench aims to bridge this gap by better reflecting the query behavior observed in production systems.

## Method

We cluster Redset users into 10 groups based on their query repetitiveness. For each group, we sample 3 "interesting" users, and we reverse-engineer their workloads by sampling similar queries from a support benchmark. This reverse-engineering step considers the number of joins and the set of scanned tables.

The resulting 30 workloads, found in the `workloads/` directory, can be used to compare the effectiveness of workload-driven optimizations.

For more details on how we generate Redbench, refer to `DETAILS.md`. Summary plots for JOB, CEB, TPC-DS, Redset, the sampling process, and the resulting benchmarks can be found in the `figures/` directory.

## Setup

To unpack the workloads, i.e., create runnable SQL workloads from the CSV files provided in `workloads/`:

```
pip install -r requirements.txt
python setup.py
```

The resulting SQL files are written back to the `workloads/` directory.

## Run

To run Redbench with the latest [DuckDB](https://duckdb.org/) version:

```
curl https://install.duckdb.org | sh
python run.py
```

The output looks as follows when run on DuckDB v1.2.1 with 48 threads on an Intel® Xeon® Gold 5318Y CPU with 128GB DDR4 RAM:

```
Redbench[tpcds_10gb]:
+-------------------------+----------------------+
| Query repetition bucket | Total execution time |
+-------------------------+----------------------+
|          0%-10%         |    0:00:15.013784    |
|         10%-20%         |    0:00:31.658575    |
|         20%-30%         |    0:01:52.562805    |
|         30%-40%         |    0:02:28.490707    |
|         40%-50%         |    0:03:14.545362    |
|         50%-60%         |    0:03:14.913141    |
|         60%-70%         |    0:03:06.618478    |
|         70%-80%         |    0:02:58.567623    |
|         80%-90%         |    0:02:16.787367    |
|         90%-100%        |    0:02:41.556895    |
+-------------------------+----------------------+

Redbench[imdb]:
+-------------------------+----------------------+
| Query repetition bucket | Total execution time |
+-------------------------+----------------------+
|          0%-10%         |    0:00:23.385233    |
|         10%-20%         |    0:00:33.803160    |
|         20%-30%         |    0:01:10.741311    |
|         30%-40%         |    0:02:54.233235    |
|         40%-50%         |    0:04:59.825286    |
|         50%-60%         |    0:02:33.452159    |
|         60%-70%         |    0:02:49.140971    |
|         70%-80%         |    0:04:26.023394    |
|         80%-90%         |    0:03:41.828650    |
|         90%-100%        |    0:05:24.071659    |
+-------------------------+----------------------+
```

> [!TIP]
> To run Redbench on a system other than DuckDB:
> 1. Set up an IMDb database on your system.
> 2. Make a one-line change in `run.py` to execute the workloads.

## Reproduce

To reproduce Redbench, i.e., re-generate the workloads from scratch:

```
python gen.py
```

## Licensing

This project has two separate licenses:
1. **Software** license: The software to generate the workload files is licensed under the MIT License. See [LICENSE](LICENSE). 
2. **Data** license: The workload files are licensed under CC BY-NC 4.0. See [DATASET_LICENSE](DATASET_LICENSE).

Redbench's workload files are based on statistics from [Redset](https://github.com/amazon-science/redset), released by Amazon  and licensed under [CC BY-NC 4.0](https://github.com/amazon-science/redset/blob/main/LICENSE).