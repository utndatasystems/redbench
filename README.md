# Redbench

Redbench is a set of 30 analytical SQL workloads that can be used to benchmark workload-driven optimizations. The workloads mimic real-world production queries from [Redset](https://github.com/amazon-science/redset), a dataset of query metadata published by Amazon Redshift, by leveraging the [Join Ordering Benchmark (JOB)](https://github.com/viktorleis/job) and the [Cardinality Estimation Benchmark (CEB)](https://github.com/learnedsystems/CEB) that run on the IMDb database.

## Motivation

Recent trends in the database community highlight the repetitive nature of real-world workloads and how these recurring query patterns can be leveraged for optimization. However, existing benchmarks such as JOB and CEB do not accurately represent recurring query patterns. Redbench aims to bridge this gap by better reflecting the query behavior observed in production systems.

## Method

We cluster Redset users into 10 groups based on their query repetitiveness. For each group, we sample 3 "interesting" users, and we reverse-engineer their workloads by sampling similar queries from JOB and CEB. This reverse-engineering step considers the number of joins and the set of scanned tables.

The resulting 30 workloads, found in the `workloads/` directory, can be used to compare the effectiveness of workload-driven optimizations.

For more details on how we generate Redbench, refer to `DETAILS.md`. Summary plots for JOB, CEB, Redset, and the sampling process can be found in the `figures/` directory.

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
+-------------------------+----------------------+
| Query repetition bucket | Total execution time |
+-------------------------+----------------------+
|          0%-10%         |    0:00:58.190302    |
|         10%-20%         |    0:00:38.223970    |
|         20%-30%         |    0:03:14.451365    |
|         30%-40%         |    0:00:24.266414    |
|         40%-50%         |    0:02:43.778529    |
|         50%-60%         |    0:02:13.973391    |
|         60%-70%         |    0:01:02.777971    |
|         70%-80%         |    0:03:08.130011    |
|         80%-90%         |    0:04:21.272561    |
|         90%-100%        |    0:06:34.652367    |
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
