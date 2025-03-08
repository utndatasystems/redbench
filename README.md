# Redbench

Redbench is a set of 30 analytical SQL workloads that can be used to benchmark workload-driven optimizations. The workloads mimic real-world production queries from [Redset](https://github.com/amazon-science/redset), a dataset of query metadata published by Amazon Redshift, by leveraging the [Join Ordering Benchmark (JOB)](https://github.com/viktorleis/job) and the [Cardinality Estimation Benchmark (CEB)](https://github.com/learnedsystems/CEB) that run on the IMDb database.

## Motivation

Recent trends in the database community highlight the repetitive nature of real-world workloads and how these recurring query patterns can be leveraged for optimization. However, existing benchmarks such as JOB and CEB do not accurately represent recurring query patterns. Redbench aims to bridge this gap by better reflecting the query behavior observed in production systems.

## Method

We cluster Redset users into 10 groups based on their query repetitiveness. For each group, we sample 3 "interesting" users, and we reverse-engineer their workloads by sampling similar queries from JOB and CEB. This reverse-engineering step considers the number of joins and the set of scanned tables.

The resulting 30 workloads, found in the `workloads/` directory, can be used to compare the effectiveness of workload-driven optimizations.

For more details on how we generate Redbench, refer to `DETAILS.md`. Summary plots for JOB, CEB, Redset, and the sampling process can be found in the `figures/` directory.

## Setup

To unpack the workloads in `workloads/`, i.e., create runnable SQL workloads from the provided CSV files:

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
|          0%-10%         |    0:00:51.170783    |
|         10%-20%         |    0:00:13.534119    |
|         20%-30%         |    0:02:06.761769    |
|         30%-40%         |    0:00:23.974739    |
|         40%-50%         |    0:01:50.975217    |
|         50%-60%         |    0:00:10.666858    |
|         60%-70%         |    0:00:51.867003    |
|         70%-80%         |    0:02:25.618717    |
|         80%-90%         |    0:09:37.785972    |
|         90%-100%        |    0:03:51.074048    |
+-------------------------+----------------------+
```

> [!TIP]
> To run Redbench on a system other than DuckDB:
> 1. Set up an IMDb database on your system.
> 2. Make a one-line change in `run.py` to execute the workloads.

## Reproduce

To reproduce Redbench, i.e., re-generate the workloads from scratch:

```
python gen.py --override
```
