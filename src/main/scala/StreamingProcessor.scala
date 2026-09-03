import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingProcessor {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // STEP 1: CREATE SPARK CONFIGURATION
    // ==================================================

    val conf = new SparkConf()
      .setAppName("Bank Transaction Stateful Monitoring")
      .setMaster("local[*]")

    // ==================================================
    // STEP 2: CREATE STREAMING CONTEXT
    // ==================================================

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    // ==================================================
    // STEP 3: CHECKPOINT DIRECTORY
    // ==================================================

    streamingContext.checkpoint(
      "data/checkpoint"
    )

    // ==================================================
    // STEP 4: CREATE INPUT STREAM
    // ==================================================

    val inputPath = "data/input/stream"

    val lines =
      streamingContext.textFileStream(inputPath)

    // ==================================================
    // STEP 5: REMOVE HEADER
    // ==================================================

    val dataLines =
      lines.filter { line =>
        !line.startsWith("transactionId")
      }

    // ==================================================
    // STEP 6: EXTRACT ACCOUNT ID
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
    // STEP 7: STATE UPDATE FUNCTION
    // ==================================================

    val updateState =
      (newValues: Seq[Int], previousState: Option[Int]) => {

        val newCount =
          newValues.sum

        val oldCount =
          previousState.getOrElse(0)

        Some(oldCount + newCount)
      }

    // ==================================================
    // STEP 8: MAINTAIN STATE
    // ==================================================

    val accountState =
      accountTransactions.updateStateByKey(updateState)

    // ==================================================
    // STEP 9: DISPLAY CURRENT STATE
    // ==================================================

    accountState.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("       CURRENT ACCOUNT STATE")
        println("==========================================")

        rdd.foreach {
          case (accountId, totalCount) =>

            println(
              s"$accountId -> $totalCount transactions"
            )
        }
      }
    }

    // ==================================================
    // STEP 10: START STREAMING
    // ==================================================

    println()
    println("==========================================")
    println("   STATEFUL STREAMING STARTED")
    println("==========================================")

    println()
    println("Input directory:")
    println(inputPath)

    println()
    println("Waiting for transaction files...")
    println("Add CSV files into data/input/stream/")
    println("Press Ctrl+C to stop.")

    streamingContext.start()

    // ==================================================
    // STEP 11: WAIT FOR STREAMING TERMINATION
    // ==================================================

    streamingContext.awaitTermination()
  }
}
