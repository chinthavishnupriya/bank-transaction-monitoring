# Bank Transaction Monitoring

A Scala and Apache Spark mini-project for monitoring bank transactions and identifying unusual transaction frequency, high-value transactions, and unusual geographic activity.

## 🌐 Live Dashboard

The Bank Transaction Monitoring Dashboard is deployed using GitHub Pages.

👉 **[Open Live Dashboard](https://chinthavishnupriya.github.io/bank-transaction-monitoring/)**

The dashboard displays:
- Valid and malformed transaction counts
- High-frequency account alerts
- High-amount transaction alerts
- Geographic transaction monitoring
- Account-level statistics
- Transaction-type statistics
- Spark partition information

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

The implementation uses Scala 2.12 and Apache Spark 3.5.6.

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
```

---

## 6. Transaction Schema

The input CSV contains the following fields:

| Field | Type | Description |
|---|---|---|
| transactionId | String | Unique transaction identifier |
| accountId | String | Bank account identifier |
| customerId | String | Customer identifier |
| transactionType | String | DEPOSIT, WITHDRAWAL, or TRANSFER |
| amount | Double | Transaction amount |
| timestamp | String | Transaction timestamp |
| location | String | Transaction location |

Example:

```csv
transactionId,accountId,customerId,transactionType,amount,timestamp,location
TXN001,ACC001,CUST001,DEPOSIT,50000,2026-09-03T09:00:00,Hyderabad
TXN002,ACC001,CUST001,WITHDRAWAL,5000,2026-09-03T09:15:00,Hyderabad
TXN003,ACC002,CUST002,TRANSFER,2500,2026-09-03T09:20:00,Chennai
```

---

## 7. Monitoring Thresholds

The final monitoring processor uses broadcast thresholds:

| Rule | Threshold |
|---|---:|
| Account transaction frequency | 3 |
| High transaction amount | 50000 |
| Geographic transaction frequency | 3 |

The monitoring logic identifies values that **exceed** the configured threshold.

Therefore, with a frequency threshold of 3, an account or location must have more than 3 transactions to be reported as high frequency.

---

## 8. Sample Dataset

The project contains 20 valid transaction records.

Important account counts:

| Account | Transaction Count |
|---|---:|
| ACC001 | 6 |
| ACC002 | 3 |
| ACC003 | 3 |
| ACC004 | 2 |
| ACC005 | 1 |
| ACC006 | 1 |
| ACC007 | 1 |
| ACC008 | 1 |
| ACC009 | 1 |
| ACC010 | 1 |

Important location counts:

| Location | Transaction Count |
|---|---:|
| Hyderabad | 7 |
| Chennai | 3 |
| Bangalore | 3 |
| Mumbai | 3 |
| Delhi | 2 |
| Kolkata | 1 |
| Pune | 1 |

---

## 9. Data Model

`Models.scala` defines the transaction model:

```scala
case class BankTransaction(
  transactionId: String,
  accountId: String,
  customerId: String,
  transactionType: String,
  amount: Double,
  timestamp: String,
  location: String
)
```

The case class provides a structured representation of every transaction.

---

## 10. Batch Processing

`BatchProcessor.scala` implements the main batch-processing workflow.

The batch pipeline performs the following operations:

1. Create a `SparkSession`.
2. Read the CSV input file.
3. Skip the header.
4. Parse and validate transaction records.
5. Count malformed records using an accumulator.
6. Cache the parsed transaction RDD.
7. Create a Pair RDD using `(accountId, 1)`.
8. Use `reduceByKey` to count transactions per account.
9. Broadcast monitoring thresholds.
10. Identify high-frequency accounts.
11. Identify high-value transactions.
12. Count transactions by location.
13. Identify high-frequency locations.
14. Demonstrate `repartition` and `coalesce`.
15. Convert transaction data to a DataFrame.
16. Perform analytical aggregations.
17. Release cached data and broadcast variables.
18. Stop the Spark session.

---

## 11. Pair RDD Processing

The project demonstrates Pair RDD operations for account-level monitoring.

Example transformation:

```scala
val accountPairs = transactions.map(tx => (tx.accountId, 1))
```

The counts are calculated using:

```scala
val accountCounts = accountPairs.reduceByKey(_ + _)
```

`reduceByKey` is a key-based aggregation and requires data movement across partitions when matching keys are located in different partitions.

---

## 12. High-Frequency Account Monitoring

The account transaction count is compared against the broadcast frequency threshold.

For the sample data:

- ACC001 has 6 transactions.
- ACC002 has 3 transactions.
- ACC003 has 3 transactions.
- The configured threshold is 3.

Because the rule reports accounts that exceed the threshold, only ACC001 is classified as a high-frequency account.

Final result:

```text
High-frequency accounts: 1
```

---

## 13. High-Amount Transaction Monitoring

The high-amount rule compares each transaction amount against the broadcast amount threshold of 50,000.

The following four transactions exceed the threshold:

| Transaction | Account | Amount | Location |
|---|---|---:|---|
| TXN005 | ACC004 | 75000 | Mumbai |
| TXN009 | ACC003 | 60000 | Bangalore |
| TXN014 | ACC007 | 90000 | Delhi |
| TXN015 | ACC008 | 120000 | Kolkata |

Final result:

```text
High-amount transactions: 4
```

---

## 14. Geographic Monitoring

Transactions are grouped by location to identify locations with unusually high transaction activity.

For the sample data:

- Hyderabad = 7
- Chennai = 3
- Bangalore = 3
- Mumbai = 3
- Delhi = 2
- Kolkata = 1
- Pune = 1

With a geographic threshold of 3 and an exceeds-threshold rule, Hyderabad is the only high-frequency location.

Final result:

```text
High-frequency locations: 1
```

---

## 15. Stateful Streaming Processing

`StreamingProcessor.scala` demonstrates stateful processing using Spark Streaming and `updateStateByKey`.

The streaming processor:

- Watches the `data/input/stream` directory.
- Processes newly arriving files as micro-batches.
- Maintains cumulative account transaction counts.
- Uses checkpointing for state recovery.

The state update logic maintains the previous account count and adds new transaction counts.

Example observed behavior:

```text
First batch:
ACC001 -> 6
ACC002 -> 3

After the second batch:
ACC001 -> 8
ACC002 -> 4
```

This demonstrates that state is maintained across micro-batches instead of being calculated only from the current batch.

---

## 16. Sliding Window Processing

`WindowProcessor.scala` demonstrates Spark Streaming window operations.

The project uses:

```scala
window(
  Seconds(15),
  Seconds(5)
)
```

This means:

- Window duration = 15 seconds
- Sliding interval = 5 seconds

The window continuously considers the most recent 15 seconds of streaming data and updates the result every 5 seconds.

Observed sample behavior included changing account counts between successive windows, demonstrating that transactions enter and leave the active window as time advances.

---

## 17. Broadcast Variables

Broadcast variables are used to distribute monitoring thresholds efficiently to Spark executors.

The project broadcasts:

```text
Frequency threshold = 3
Amount threshold = 50000
Geographic threshold = 3
```

Broadcasting avoids repeatedly shipping the same small read-only configuration with every task.

The final processor also ensures that all required computations using the broadcasts are completed before destroying them.

---

## 18. Accumulator

An accumulator named `malformedTransactions` is used to count records that cannot be parsed correctly.

For the sample dataset:

```text
Valid transactions: 20
Malformed transactions: 0
```

Accumulators are useful for counters and metrics that are updated by tasks and observed by the driver.

---

## 19. Cache and Persist

The parsed transaction RDD is reused by several operations, so caching is demonstrated.

Typical usage is:

```scala
transactions.cache()
```

After the required computations are complete, the cached RDD is released.

Caching is useful when the same RDD is used repeatedly and recomputing it would otherwise increase execution cost.

---

## 20. Narrow and Wide Transformations

### Narrow Transformations

A narrow transformation does not require data to be shuffled between partitions.

Examples used or demonstrated in the project include:

- `map`
- `filter`
- `mapValues`

Each output partition can be computed from a small number of corresponding input partitions.

### Wide Transformations

A wide transformation requires data movement between partitions and can create a shuffle boundary.

Examples in this project include:

- `reduceByKey`
- `groupByKey`
- `repartition`

These operations can cause records with the same key or required partitioning relationship to be moved across the cluster.

---

## 21. DAG

Spark builds a Directed Acyclic Graph (DAG) of transformations before executing an action.

A simplified flow for this project is:

```text
Input CSV
   |
   v
Parse and Validate
   |
   v
Cached Transactions
   |
   +--------------------+
   |                    |
   v                    v
Pair RDD             DataFrame
   |                    |
   v                    v
reduceByKey         groupBy / agg
   |
   v
Account Alerts
```

The DAG allows Spark to optimize execution before running tasks.

---

## 22. Spark Stages

Spark divides a job into stages around shuffle boundaries.

For example, operations such as `map` and `filter` can remain in the same stage when no shuffle is required. A key-based aggregation such as `reduceByKey` introduces a shuffle boundary and can therefore result in additional stages.

The exact number of stages can vary depending on the action and execution plan.

---

## 23. Shuffle

A shuffle occurs when Spark must redistribute data across partitions.

In this project, key-based account aggregation using `reduceByKey` is a major example.

Conceptually:

```text
Partition 1 ----\
Partition 2 ----- > Shuffle by accountId ---> Aggregated partitions
Partition 3 ----/
```

Shuffle operations can be expensive because they involve network and serialization overhead.

---

## 24. Partition Management

The project demonstrates partition management using:

```scala
repartition(4)
```

followed by:

```scala
coalesce(2)
```

The observed partition sequence is:

```text
Initial partitions : 2
After repartition  : 4
After coalesce     : 2
```

### Repartition vs Coalesce

| Operation | Typical Purpose | Shuffle |
|---|---|---|
| `repartition(n)` | Increase or decrease partitions | Yes |
| `coalesce(n)` | Usually reduce partitions | Usually avoids a full shuffle |

`repartition` is useful when a more even redistribution is required. `coalesce` is useful when reducing partitions without requiring a complete redistribution.

---

## 25. DataFrame Aggregations

The final monitoring processor converts transaction data into a DataFrame and performs analytical aggregations.

The project calculates:

- Transaction count by account
- Total transaction amount by account
- Average transaction amount by account
- Transaction count by transaction type
- Total transaction amount by transaction type
- Average transaction amount by transaction type
- Transaction count by location
- Total transaction amount by location

Example account statistics:

| Account | Count | Total Amount | Average Amount |
|---|---:|---:|---:|
| ACC001 | 6 | 65500 | 10916.67 |
| ACC002 | 3 | 10500 | 3500.00 |
| ACC003 | 3 | 80500 | 26833.33 |
| ACC004 | 2 | 85000 | 42500.00 |

---

## 26. Transaction-Type Analysis

The final pipeline produces the following transaction-type statistics:

| Type | Count | Total Amount | Average Amount |
|---|---:|---:|---:|
| DEPOSIT | 3 | 85000 | 28333.33 |
| TRANSFER | 10 | 277000 | 27700.00 |
| WITHDRAWAL | 7 | 139500 | 19928.57 |

---

## 27. Location Analysis

The final pipeline produces the following location statistics:

| Location | Count | Total Amount |
|---|---:|---:|
| Bangalore | 3 | 80500 |
| Chennai | 3 | 10500 |
| Delhi | 2 | 100000 |
| Hyderabad | 7 | 72500 |
| Kolkata | 1 | 120000 |
| Mumbai | 3 | 110000 |
| Pune | 1 | 8000 |

---

## 28. Spark SQL, Joins, Windows and UDFs

The current transaction schema does not require a reference-table join or custom UDF for the implemented monitoring rules.

The project demonstrates DataFrame aggregations directly. The same architecture can be extended with:

- Spark SQL queries
- Reference-table joins
- Window functions for time-based account analysis
- UDFs for custom transaction classification

These features are useful extensions when the monitoring system is expanded with customer profiles, account metadata, risk categories, or more advanced fraud rules.

---

## 29. Fault Tolerance and RDD Lineage

Spark maintains lineage information for RDD transformations.

If a partition is lost, Spark can recompute the required data from the lineage rather than requiring the entire dataset to be stored redundantly in memory.

The project also demonstrates checkpointing in the stateful streaming processor because maintained streaming state requires recovery support.

---

## 30. YARN Deployment Concept

The project is executed locally for development and demonstration, but the application can conceptually be deployed to a Hadoop YARN cluster.

After creating the application JAR with sbt, a deployment could follow this pattern:

```bash
spark-submit \
  --master yarn \
  --class FinalMonitoringProcessor \
  bank-transaction-monitoring.jar
```

The current implementation was validated in local Spark mode.

---

## 31. Running the Project

### Compile and Package

```bash
sbt clean
sbt package
```

### Run Batch Processor

```bash
sbt "runMain BatchProcessor"
```

### Run Final Monitoring Processor

```bash
sbt "runMain FinalMonitoringProcessor"
```

### Run Streaming Processor

```bash
sbt "runMain StreamingProcessor"
```

### Run Window Processor

```bash
sbt "runMain WindowProcessor"
```

### Spark Submit

The packaged application was successfully executed with:

```bash
spark-submit \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --class FinalMonitoringProcessor \
  --master local[*] \
  bank-transaction-monitoring.jar
```

The `spark.hadoop.fs.defaultFS=file:///` setting ensures that the local project input is treated as a local filesystem path in the tested environment.

---

## 32. Java 17 Spark Configuration

The project uses Java 17 with Spark 3.5.6.

For the tested local environment, Spark execution used the following Java module-opening options:

```bash
export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
```

---

## 33. Actual Execution Output

The final monitoring pipeline was successfully executed with the following results:

```text
Monitoring thresholds:
Frequency: > 3
Amount: > ₹50000.00
Location: > 3

Input summary:
Total valid transactions: 20
Malformed transactions: 0

High-frequency account alerts:
ALERT -> ACC001 has 6 transactions

High amount alerts:
TXN005 | ACC004 | ₹75000 | Mumbai
TXN009 | ACC003 | ₹60000 | Bangalore
TXN014 | ACC007 | ₹90000 | Delhi
TXN015 | ACC008 | ₹120000 | Kolkata

High-frequency location:
ALERT -> Hyderabad has 7 transactions

Summary:
Valid 20
Malformed 0
High-frequency accounts 1
High-amount transactions 4
High-frequency locations 1
Final monitoring pipeline completed.
```

---

## 34. Architecture

```text
                +----------------------+
                | transactions.csv     |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | CSV Parsing/Validate |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Spark Transaction RDD|
                +----------+-----------+
                           |
              +------------+------------+
              |            |            |
              v            v            v
        Account RDD   Amount Rules   Location RDD
              |            |            |
              v            v            v
        Frequency       Alerts       Geo Alerts
              |            |            |
              +------------+------------+
                           |
                           v
                +----------------------+
                | DataFrame Analytics  |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Monitoring Results   |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | GitHub Pages Dashboard|
                +----------------------+
```

---

## 35. Learning Outcomes

This project demonstrates practical knowledge of:

- Scala programming
- Apache Spark fundamentals
- RDD processing
- Pair RDDs
- Stateful streaming
- Window operations
- Broadcast variables
- Accumulators
- Caching
- DataFrames and aggregations
- Partition management
- Narrow and wide transformations
- DAG and stage concepts
- Shuffle behavior
- RDD lineage
- Fault tolerance
- Spark deployment concepts
- Git and GitHub project management
- Static dashboard deployment using GitHub Pages

---

## 36. Project Checklist

| Requirement | Status |
|---|---|
| Scala implementation | ✅ |
| Spark RDD processing | ✅ |
| Pair RDD | ✅ |
| Stateful processing | ✅ |
| Sliding window | ✅ |
| Broadcast variable | ✅ |
| Accumulator | ✅ |
| Cache/persist | ✅ |
| DataFrame aggregation | ✅ |
| Narrow transformations | ✅ |
| Wide transformations | ✅ |
| DAG explanation | ✅ |
| Stage explanation | ✅ |
| Shuffle explanation | ✅ |
| Partition management | ✅ |
| Repartition/coalesce | ✅ |
| Fault tolerance/lineage | ✅ |
| YARN deployment concept | ✅ |
| Packaged JAR | ✅ |
| Spark-submit execution | ✅ |
| Public dashboard | ✅ |

---

## 37. Author

**Chintha Vishnupriya**

Project: **Bank Transaction Monitoring**

Technology: **Scala + Apache Spark**

Repository: `chinthavishnupriya/bank-transaction-monitoring`
