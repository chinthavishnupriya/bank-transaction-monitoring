import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowProcessor {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // STEP 1: CREATE SPARK CONFIGURATION
    // ==================================================

    val conf = new SparkConf()
      .setAppName("Bank Transaction Window Monitoring")
      .setMaster("local[*]")
      .set(
        "spark.serializer",
        "org.apache.spark.serializer.JavaSerializer"
      )

    // ==================================================
    // STEP 2: CREATE STREAMING CONTEXT
    // ==================================================

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    // ==================================================
    // STEP 3: CREATE INPUT STREAM
    // ==================================================

    val inputPath = "data/input/window"

    val lines =
      streamingContext.textFileStream(inputPath)

    // ==================================================
    // STEP 4: REMOVE HEADER
    // ==================================================

    val dataLines =
      lines.filter { line =>
        !line.startsWith("transactionId")
      }

    // ==================================================
    // STEP 5: EXTRACT ACCOUNT ID
    // ==================================================

    val accountTransactions =
      dataLines.map { line =>

        val fields = line.split(",", -1)

        if (fields.length >= 2) {
          (fields(1), 1)
        } else {
          ("INVALID", 0)
        }
      }

    // ==================================================
    // STEP 6: CREATE 15-SECOND WINDOW
    // ==================================================

    val windowedTransactions =
      accountTransactions.window(
        Seconds(15),
        Seconds(5)
      )

    // ==================================================
    // STEP 7: COUNT TRANSACTIONS BY ACCOUNT
    // ==================================================

    val windowTransactionCounts =
      windowedTransactions.reduceByKey(_ + _)

    // ==================================================
    // STEP 8: DISPLAY WINDOW RESULTS
    // ==================================================

    windowTransactionCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("       15-SECOND TRANSACTION WINDOW")
        println("==========================================")

        val results =
          rdd.collect().sortBy(_._1)

        results.foreach {
          case (accountId, count) =>

            println(
              s"$accountId -> $count transactions"
            )
        }

        println("------------------------------------------")
        println(
          "Window duration: 15 seconds"
        )

        println(
          "Sliding interval: 5 seconds"
        )
      }
    }

    // ==================================================
    // STEP 9: START STREAMING
    // ==================================================

    println()
    println("==========================================")
    println("     WINDOW STREAMING STARTED")
    println("==========================================")

    println()
    println("Input directory:")
    println(inputPath)

    println()
    println(
      "Add CSV files into data/input/window/"
    )

    println()
    println(
      "Window = 15 seconds, Slide = 5 seconds"
    )

    println()
    println("Press Ctrl+C to stop.")

    streamingContext.start()

    // ==================================================
    // STEP 10: WAIT FOR STREAMING
    // ==================================================

    streamingContext.awaitTermination()
  }
}
