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

    val streamingDf = sparkSession.readStream
      .format("com.github.yoda_mon.spark_gtfs_rt")
      .load()

    streamingDf.printSchema()

    val query1 = streamingDf.writeStream
      .format("console")
      .queryName("no operation")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .outputMode(OutputMode.Append())

    val query2 = streamingDf.where("id == '223'")
      .writeStream
      .format("console")
      .queryName("select")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .outputMode(OutputMode.Update())

    // query1.start()
    query2.start().awaitTermination()

  }
}
