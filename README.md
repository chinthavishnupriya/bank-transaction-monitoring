# Bank Transaction Monitoring

A Scala and Apache Spark mini-project for monitoring bank transactions and identifying unusual transaction frequency, high-value transactions, and unusual geographic activity.

## 1. Project Overview

This project implements a bank transaction monitoring pipeline using Scala and Apache Spark.

The system processes transaction records and identifies potentially unusual activity using three main monitoring rules:

1. High transaction frequency for an account
2. High transaction amount
3. High transaction activity from a geographic location

The project demonstrates both batch and streaming-oriented Spark processing.

The implementation covers:

- Scala
- Apache Spark
- RDDs
- Pair RDDs
- Stateful processing
- Sliding window processing
- Broadcast variables
- Accumulators
- Cache/persist
- DataFrames
- Aggregations
- Partition management
- Repartition and coalesce
- DAG and stage concepts
- Shuffle concepts
- RDD lineage and fault tolerance

---

## 2. Problem Statement

Process account transactions and identify unusual transaction frequency, amount, and geographic patterns.

The project is based on Project 11 — Bank Transaction Monitoring from the Scala + Apache Spark real-time case-study mini-project set.

---

## 3. Objectives

The main objectives are:

- Read transaction data from CSV files.
- Validate and parse transaction records.
- Count transactions for each account.
- Detect accounts with unusually high transaction frequency.
- Detect high-value transactions.
- Count transactions by geographic location.
- Detect locations with unusually high transaction activity.
- Demonstrate stateful streaming processing.
- Demonstrate sliding-window processing.
- Demonstrate Spark partition management.
- Use broadcast variables for monitoring thresholds.
- Use an accumulator for malformed transaction records.
- Use DataFrames for analytical aggregations.
- Demonstrate cache/persist for repeated computation.
- Explain Spark execution concepts such as DAG, stages, shuffle, and lineage.

---

## 4. Technology Stack

| Technology | Version |
|---|---|
| Scala | 2.12.18 |
| Apache Spark | 3.5.6 |
| Java | 17 |
| sbt | 2.0.7 |
| Spark Core | 3.5.6 |
| Spark SQL | 3.5.6 |
| Spark Streaming | 3.5.6 |
| Execution Mode | Local |

The recommended implementation stack for the case studies is Scala 2.12 with Apache Spark 3.5.x.

---

## 5. Project Structure

```text
bank-transaction-monitoring/
│
├── build.sbt
├── .gitignore
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           ├── Models.scala
│           ├── BatchProcessor.scala
│           ├── StreamingProcessor.scala
│           ├── WindowProcessor.scala
│           └── FinalMonitoringProcessor.scala
│
└── data/
    ├── input/
    │   ├── transactions.csv
    │   ├── stream/
    │   └── window/
    │
    ├── reference/
    │
    └── output/