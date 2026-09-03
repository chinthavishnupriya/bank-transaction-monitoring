import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator

object FinalMonitoringProcessor {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // STEP 1: CREATE SPARK SESSION
    // ==================================================

    val spark = SparkSession.builder()
      .appName("Final Bank Transaction Monitoring")
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
    println("==================================================")
    println("       FINAL BANK TRANSACTION MONITORING")
    println("==================================================")

    // ==================================================
    // STEP 2: CREATE ACCUMULATOR
    // ==================================================

    val malformedTransactions: LongAccumulator =
      sc.longAccumulator("Malformed Transactions")

    // ==================================================
    // STEP 3: BROADCAST MONITORING THRESHOLDS
    // ==================================================

    val thresholds = Map(
      "frequency" -> 3.0,
      "amount" -> 50000.0,
      "location" -> 3.0
    )

    val broadcastThresholds =
      sc.broadcast(thresholds)

    println()
    println("Monitoring thresholds:")
    println("------------------------------------------")

    println(
      s"Frequency: > ${broadcastThresholds.value("frequency").toInt}"
    )

    println(
      f"Amount: > ₹${broadcastThresholds.value("amount")}%.2f"
    )

    println(
      s"Location: > ${broadcastThresholds.value("location").toInt}"
    )

    // ==================================================
    // STEP 4: READ TRANSACTIONS
    // ==================================================

    val inputPath =
      "data/input/transactions.csv"

    val lines =
      sc.textFile(inputPath)

    // ==================================================
    // STEP 5: REMOVE HEADER
    // ==================================================

    val dataLines =
      lines.filter { line =>
        !line.startsWith("transactionId")
      }

    // ==================================================
    // STEP 6: SAFE PARSING
    // ==================================================

    val parsedTransactions =
      dataLines.flatMap { line =>

        val fields =
          line.split(",", -1)

        if (fields.length != 7) {

          malformedTransactions.add(1)

          None

        } else {

          val fieldsValid =
            fields.forall(_.trim.nonEmpty)

          if (!fieldsValid) {

            malformedTransactions.add(1)

            None

          } else {

            try {

              Some(
                BankTransaction(
                  transactionId = fields(0),
                  accountId = fields(1),
                  customerId = fields(2),
                  transactionType = fields(3),
                  amount = fields(4).toDouble,
                  timestamp = fields(5),
                  location = fields(6)
                )
              )

            } catch {

              case _: NumberFormatException =>

                malformedTransactions.add(1)

                None
            }
          }
        }
      }

    // ==================================================
    // STEP 7: CACHE TRANSACTIONS
    // ==================================================

    val transactions =
      parsedTransactions.cache()

    // ==================================================
    // STEP 8: FORCE CACHE
    // ==================================================

    val totalTransactions =
      transactions.count()

    println()
    println("Input summary:")
    println("------------------------------------------")

    println(
      s"Total valid transactions: $totalTransactions"
    )

    println(
      s"Malformed transactions: ${malformedTransactions.value}"
    )

    // ==================================================
    // STEP 9: PAIR RDD - ACCOUNT FREQUENCY
    // ==================================================

    val accountPairs =
      transactions.map { transaction =>
        (transaction.accountId, 1)
      }

    val accountCounts =
      accountPairs.reduceByKey(_ + _)

    println()
    println("==================================================")
    println("       TRANSACTION FREQUENCY MONITORING")
    println("==================================================")

    accountCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, count) =>
          println(
            s"$accountId -> $count transactions"
          )
      }

    // ==================================================
    // STEP 10: HIGH FREQUENCY ALERTS
    // ==================================================

    val frequencyAlerts =
      accountCounts.filter {
        case (_, count) =>
          count >
            broadcastThresholds.value("frequency")
      }

    println()
    println("High-frequency account alerts:")
    println("------------------------------------------")

    frequencyAlerts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, count) =>
          println(
            s"ALERT -> $accountId has $count transactions"
          )
      }

    // ==================================================
    // STEP 11: HIGH AMOUNT MONITORING
    // ==================================================

    val amountAlerts =
      transactions.filter { transaction =>

        transaction.amount >
          broadcastThresholds.value("amount")
      }

    println()
    println("==================================================")
    println("       HIGH AMOUNT TRANSACTION MONITORING")
    println("==================================================")

    amountAlerts
      .collect()
      .sortBy(_.transactionId)
      .foreach { transaction =>

        println(
          f"ALERT -> ${transaction.transactionId} | " +
          s"Account: ${transaction.accountId} | " +
          f"Amount: ₹${transaction.amount}%.2f | " +
          s"Location: ${transaction.location}"
        )
      }

    // ==================================================
    // STEP 12: GEOGRAPHIC MONITORING
    // ==================================================

    val locationPairs =
      transactions.map { transaction =>
        (transaction.location, 1)
      }

    val locationCounts =
      locationPairs.reduceByKey(_ + _)

    println()
    println("==================================================")
    println("       GEOGRAPHIC TRANSACTION MONITORING")
    println("==================================================")

    locationCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (location, count) =>
          println(
            s"$location -> $count transactions"
          )
      }

    val geographicAlerts =
      locationCounts.filter {
        case (_, count) =>
          count >
            broadcastThresholds.value("location")
      }

    println()
    println("High-frequency location alerts:")
    println("------------------------------------------")

    geographicAlerts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (location, count) =>
          println(
            s"ALERT -> $location has $count transactions"
          )
      }

    // ==================================================
    // STEP 13: DATAFRAME CONVERSION
    // ==================================================

    val transactionDF =
      transactions.toDF()

    println()
    println("==================================================")
    println("       DATAFRAME ACCOUNT ANALYSIS")
    println("==================================================")

    // ==================================================
    // STEP 14: ACCOUNT STATISTICS
    // ==================================================

    val accountStatistics =
      transactionDF
        .groupBy("accountId")
        .agg(
          count("*").alias("transactionCount"),
          sum("amount").alias("totalAmount"),
          avg("amount").alias("averageAmount"),
          max("amount").alias("maximumAmount"),
          min("amount").alias("minimumAmount")
        )
        .orderBy("accountId")

    accountStatistics.show(
      truncate = false
    )

    // ==================================================
    // STEP 15: TRANSACTION TYPE ANALYSIS
    // ==================================================

    val transactionTypeStatistics =
      transactionDF
        .groupBy("transactionType")
        .agg(
          count("*").alias("transactionCount"),
          sum("amount").alias("totalAmount"),
          avg("amount").alias("averageAmount")
        )
        .orderBy("transactionType")

    println()
    println("Transaction type statistics:")
    println("------------------------------------------")

    transactionTypeStatistics.show(
      truncate = false
    )

    // ==================================================
    // STEP 16: LOCATION DATAFRAME ANALYSIS
    // ==================================================

    val locationStatistics =
      transactionDF
        .groupBy("location")
        .agg(
          count("*").alias("transactionCount"),
          sum("amount").alias("totalAmount")
        )
        .orderBy("location")

    println()
    println("Location statistics:")
    println("------------------------------------------")

    locationStatistics.show(
      truncate = false
    )

    // ==================================================
    // STEP 17: PARTITION INFORMATION
    // ==================================================

    println()
    println("==================================================")
    println("       PARTITION INFORMATION")
    println("==================================================")

    println(
      s"Original partitions: " +
      s"${transactions.getNumPartitions}"
    )

    val repartitioned =
      transactions.repartition(4)

    println(
      s"After repartition(4): " +
      s"${repartitioned.getNumPartitions}"
    )

    val coalesced =
      repartitioned.coalesce(2)

    println(
      s"After coalesce(2): " +
      s"${coalesced.getNumPartitions}"
    )

    // ==================================================
    // STEP 18: CALCULATE FINAL COUNTS
    // ==================================================

    val finalFrequencyAlertCount =
      frequencyAlerts.count()

    val finalAmountAlertCount =
      amountAlerts.count()

    val finalGeographicAlertCount =
      geographicAlerts.count()

    // ==================================================
    // STEP 19: FINAL SUMMARY
    // ==================================================

    println()
    println("==================================================")
    println("       MONITORING SUMMARY")
    println("==================================================")

    println(
      s"Valid transactions: $totalTransactions"
    )

    println(
      s"Malformed transactions: " +
      s"${malformedTransactions.value}"
    )

    println(
      s"High-frequency accounts: " +
      s"$finalFrequencyAlertCount"
    )

    println(
      s"High-amount transactions: " +
      s"$finalAmountAlertCount"
    )

    println(
      s"High-frequency locations: " +
      s"$finalGeographicAlertCount"
    )

    println()
    println("Final monitoring pipeline completed.")
    println("==================================================")

    // ==================================================
    // STEP 20: CLEANUP
    // ==================================================

    transactions.unpersist()

    broadcastThresholds.destroy()

    spark.stop()
  }
}
