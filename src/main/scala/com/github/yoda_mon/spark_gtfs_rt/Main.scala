package com.github.yoda_mon.spark_gtfs_rt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("streaming example")
      .getOrCreate()
    import sparkSession.implicits._

    // take 3 samples
    val batchDf = sparkSession.read
      .format("com.github.yoda_mon.spark_gtfs_rt")
      .option("gtfsRealtime.url", "https://km.bus-vision.jp/realtime/sankobus_vpos_update.bin")
      .load()

    batchDf.createTempView("test_flight")
    val samples = sparkSession.sql("SELECT * FROM test_flight ORDER BY RANDOM() LIMIT 3").toDF()
    samples.show()
    val sample_ids = samples.select($"id").as[String].collect()

    // Define Stream
    val streamingDf = sparkSession.readStream
      .format("com.github.yoda_mon.spark_gtfs_rt")
      .option("gtfsRealtime.url", "https://km.bus-vision.jp/realtime/sankobus_vpos_update.bin")
      .load()

    // streamingDf.printSchema()

    /*
    val query1 = streamingDf.writeStream
      .format("console")
      .queryName("no operation")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .outputMode(OutputMode.Append())
    */
    //
    val query2 = streamingDf.filter($"id".isin(sample_ids:_*))
      .writeStream
      .format("console")
      .queryName("select")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode(OutputMode.Update())

    // query1.start()
    query2.start().awaitTermination()

  }
}
