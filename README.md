# Redbench

Redbench is a set of 30 analytical sql workloads that can be used to benchmark workload-driven optimizations. The workloads mimic real-world production queries by leveraging the Redset dataset published by Redshift, and the industry standard benchmarks JOB and CEB that run on the IMDb database.

## Motivation

Recent trends [?] in database research highlight the repetitive nature of real-world workloads and how these recurring query patterns can be leveraged for optimization. However, traditional benchmarks like CEB and JOB fall short in capturing the effects of such workload-driven optimizations due to their unrealistic query repetition patterns. Redbench aims to bridge this gap by addressing the limitations of existing industry benchmarks and better reflecting the query behavior observed in real production systems, such as those in Redshift.

## Method

Redbench relies on Redset as a source of truth for what production workloads look like. We cluster Redset users into 10 groups based on their workload repetitiveness. For each group, we sample 3 "interesting" users, and we reverse-engineer their workloads by sampling similar queries from JOB and CEB. This reverse-engineering step considers the number of joins and the set of scanned tables.

The resulting 30 workloads, found in the `redbench/` directory, can be used to compare the effectiveness of workload-driven optimizations for different workload categories.

For more details on how we generate Redbench, refer to `DETAILS.md`. Summary plots for JOB, CEB, Redset, and the sampling process can be found in the `figures/` directory.

## Setup
```
pip install -r requirements.txt
curl https://install.duckdb.org | sh
```

## Reproduce

To reproduce Redbench, i.e. re-generate the workloads from scratch:

```
python gen.py --override
```

## Run

To run Redbench with duckdb:

```
python run.py
```

The output looks as follows:

```
+------------------+----------------------+------------------------------+
| Repetition rate | Total execution time | Average query execution time |
+------------------+----------------------+------------------------------+
|      0%-10%      |    0:00:53.245447    |        0:00:00.145479        |
|     10%-20%      |    0:02:09.517975    |        0:00:00.153822        |
|     20%-30%      |    0:00:20.857511    |        0:00:00.164232        |
|     30%-40%      |    0:00:20.213274    |        0:00:00.139402        |
|     40%-50%      |    0:02:07.920647    |        0:00:00.133808        |
|     50%-60%      |    0:01:24.109906    |        0:00:00.205146        |
|     60%-70%      |    0:04:09.590554    |        0:00:00.123621        |
|     70%-80%      |    0:02:44.109312    |        0:00:00.132346        |
|     80%-90%      |    0:00:51.420873    |        0:00:00.152133        |
|     90%-100%     |    0:03:20.493453    |        0:00:00.142497        |
+------------------+----------------------+------------------------------+
```
