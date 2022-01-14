package com.github.yoda_mon.spark_gtfs_rt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("streaming example")
      .getOrCreate()

    val streamingDf = sparkSession.
      readStream.
      format("com.github.yoda_mon.spark_gtfs_rt")
      .load()



    val query = streamingDf.writeStream
      .format("console")
      .queryName("simple_source")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Append())

    query.start().awaitTermination()

  }
}
