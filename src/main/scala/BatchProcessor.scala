import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bank Transaction Monitoring")
      .master("local[*]")
      .config(
        "spark.serializer",
        "org.apache.spark.serializer.JavaSerializer"
      )
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    import spark.implicits._

    println()
    println("==========================================")
    println("       BANK TRANSACTION MONITORING")
    println("==========================================")

    // ==================================================
    // STEP 1: CREATE ACCUMULATOR
    // ==================================================

    val malformedTransactions: LongAccumulator =
      sc.longAccumulator("Malformed Transactions")

    // ==================================================
    // STEP 2: READ CSV
    // ==================================================

    val inputPath = "data/input/transactions.csv"

    val lines = sc.textFile(inputPath)

    // ==================================================
    // STEP 3: REMOVE HEADER
    // ==================================================

    val dataLines = lines.filter { line =>
      !line.startsWith("transactionId")
    }

    // ==================================================
    // STEP 4: PARSE TRANSACTIONS SAFELY
    // ==================================================

    val parsedTransactions = dataLines.flatMap { line =>

      val fields = line.split(",", -1)

      if (fields.length != 7) {

        malformedTransactions.add(1)

        None

      } else {

        val requiredFieldsValid =
          fields.forall(_.trim.nonEmpty)

        if (!requiredFieldsValid) {

          malformedTransactions.add(1)

          None

        } else {

          try {

            val transaction = BankTransaction(
              transactionId = fields(0),
              accountId = fields(1),
              customerId = fields(2),
              transactionType = fields(3),
              amount = fields(4).toDouble,
              timestamp = fields(5),
              location = fields(6)
            )

            Some(transaction)

          } catch {

            case _: NumberFormatException =>

              malformedTransactions.add(1)

              None
          }
        }
      }
    }

    // ==================================================
    // STEP 5: CACHE VALID TRANSACTIONS
    // ==================================================

    val transactions =
      parsedTransactions.cache()

    // ==================================================
    // STEP 6: TOTAL VALID TRANSACTIONS
    // ==================================================

    val totalTransactions =
      transactions.count()

    println()
    println(
      s"Total valid transactions: $totalTransactions"
    )

    // ==================================================
    // STEP 7: MALFORMED TRANSACTION COUNT
    // ==================================================

    println()
    println("Malformed transaction count:")
    println("------------------------------------------")

    println(
      s"Malformed transactions: " +
      s"${malformedTransactions.value}"
    )

    // ==================================================
    // STEP 8: FIRST 5 TRANSACTIONS
    // ==================================================

    println()
    println("First 5 valid transactions:")
    println("------------------------------------------")

    transactions.take(5).foreach { transaction =>
      println(transaction)
    }

    // ==================================================
    // STEP 9: BROADCAST MONITORING THRESHOLDS
    // ==================================================

    val monitoringThresholds = Map(
      "frequency" -> 3.0,
      "amount" -> 50000.0,
      "geographic" -> 3.0
    )

    val broadcastThresholds =
      sc.broadcast(monitoringThresholds)

    println()
    println("Broadcast monitoring thresholds:")
    println("------------------------------------------")

    println(
      s"Frequency threshold: " +
      s"${broadcastThresholds.value("frequency").toInt}"
    )

    println(
      f"Amount threshold: " +
      f"₹${broadcastThresholds.value("amount")}%.2f"
    )

    println(
      s"Geographic threshold: " +
      s"${broadcastThresholds.value("geographic").toInt}"
    )

    // ==================================================
    // STEP 10: PARTITION INFORMATION
    // ==================================================

    println()
    println("Partition information:")
    println("------------------------------------------")

    println(
      s"Transactions RDD partitions: " +
      s"${transactions.getNumPartitions}"
    )

    // ==================================================
    // STEP 11: ACCOUNT PAIR RDD
    // ==================================================

    val accountTransactionPairs =
      transactions.map { transaction =>
        (transaction.accountId, 1)
      }

    println(
      s"Account Pair RDD partitions: " +
      s"${accountTransactionPairs.getNumPartitions}"
    )

    // ==================================================
    // STEP 12: REDUCE BY KEY
    // ==================================================

    val transactionCountByAccount =
      accountTransactionPairs.reduceByKey(_ + _)

    println(
      s"After reduceByKey partitions: " +
      s"${transactionCountByAccount.getNumPartitions}"
    )

    // ==================================================
    // STEP 13: DISPLAY ACCOUNT COUNTS
    // ==================================================

    println()
    println("Transaction count by account:")
    println("------------------------------------------")

    transactionCountByAccount.foreach {
      case (accountId, count) =>
        println(s"$accountId -> $count")
    }

    // ==================================================
    // STEP 14: FREQUENCY MONITORING
    // ==================================================

    val suspiciousFrequency =
      transactionCountByAccount.filter {
        case (_, count) =>
          count > broadcastThresholds.value("frequency")
      }

    println()
    println("High transaction frequency accounts:")
    println("------------------------------------------")

    suspiciousFrequency.foreach {
      case (accountId, count) =>
        println(
          s"ALERT -> $accountId has $count transactions"
        )
    }

    val suspiciousAccountCount =
      suspiciousFrequency.count()

    println()
    println(
      s"Number of high-frequency accounts: " +
      s"$suspiciousAccountCount"
    )

    // ==================================================
    // STEP 15: AMOUNT MONITORING
    // ==================================================

    val suspiciousAmountTransactions =
      transactions.filter { transaction =>
        transaction.amount >
          broadcastThresholds.value("amount")
      }

    println()
    println("High amount transactions:")
    println("------------------------------------------")

    suspiciousAmountTransactions.foreach { transaction =>

      println(
        s"ALERT -> ${transaction.transactionId} | " +
        s"Account: ${transaction.accountId} | " +
        f"Amount: ₹${transaction.amount}%.2f | " +
        s"Type: ${transaction.transactionType} | " +
        s"Location: ${transaction.location}"
      )
    }

    val suspiciousAmountCount =
      suspiciousAmountTransactions.count()

    println()
    println(
      s"Number of high-amount transactions: " +
      s"$suspiciousAmountCount"
    )

    // ==================================================
    // STEP 16: GEOGRAPHIC MONITORING
    // ==================================================

    val locationTransactionPairs =
      transactions.map { transaction =>
        (transaction.location, 1)
      }

    val transactionCountByLocation =
      locationTransactionPairs.reduceByKey(_ + _)

    println()
    println("Transaction count by location:")
    println("------------------------------------------")

    transactionCountByLocation.foreach {
      case (location, count) =>
        println(s"$location -> $count")
    }

    val suspiciousLocations =
      transactionCountByLocation.filter {
        case (_, count) =>
          count >
            broadcastThresholds.value("geographic")
      }

    println()
    println("High-frequency geographic locations:")
    println("------------------------------------------")

    suspiciousLocations.foreach {
      case (location, count) =>
        println(
          s"ALERT -> $location has $count transactions"
        )
    }

    val suspiciousLocationCount =
      suspiciousLocations.count()

    println()
    println(
      s"Number of high-frequency locations: " +
      s"$suspiciousLocationCount"
    )

    // ==================================================
    // STEP 17: REPARTITION
    // ==================================================

    val repartitionedTransactions =
      transactions.repartition(4)

    println()
    println("After repartition:")
    println("------------------------------------------")

    println(
      s"Partitions after repartition(4): " +
      s"${repartitionedTransactions.getNumPartitions}"
    )

    // ==================================================
    // STEP 18: COALESCE
    // ==================================================

    val coalescedTransactions =
      repartitionedTransactions.coalesce(2)

    println()
    println("After coalesce:")
    println("------------------------------------------")

    println(
      s"Partitions after coalesce(2): " +
      s"${coalescedTransactions.getNumPartitions}"
    )

    // ==================================================
    // STEP 19: CONVERT RDD TO DATAFRAME
    // ==================================================

    val transactionDF =
      transactions.toDF()

    println()
    println("Transaction DataFrame:")
    println("------------------------------------------")

    transactionDF.show(5, truncate = false)

    // ==================================================
    // STEP 20: DATAFRAME SCHEMA
    // ==================================================

    println()
    println("DataFrame schema:")
    println("------------------------------------------")

    transactionDF.printSchema()

    // ==================================================
    // STEP 21: DATAFRAME AGGREGATIONS
    // ==================================================

    val accountStatistics =
      transactionDF
        .groupBy("accountId")
        .agg(
          count("*").alias("transactionCount"),
          sum("amount").alias("totalAmount"),
          avg("amount").alias("averageAmount"),
          max("amount").alias("maximumAmount")
        )
        .orderBy("accountId")

    // ==================================================
    // STEP 22: DISPLAY ACCOUNT STATISTICS
    // ==================================================

    println()
    println("Account transaction statistics:")
    println("------------------------------------------")

    accountStatistics.show(
      truncate = false
    )

    // ==================================================
    // STEP 23: STOP CACHE
    // ==================================================

    transactions.unpersist()

    // ==================================================
    // STEP 24: DESTROY BROADCAST
    // ==================================================

    broadcastThresholds.destroy()

    // ==================================================
    // STEP 25: STOP SPARK
    // ==================================================

    spark.stop()
  }
}
