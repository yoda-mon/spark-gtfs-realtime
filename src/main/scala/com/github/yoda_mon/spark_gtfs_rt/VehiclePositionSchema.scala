package com.github.yoda_mon.spark_gtfs_rt

import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}

object VehiclePositionSchema {
  def getSchema(): StructType = {
    StructType(Array(
      StructField("tripId",StringType),
      StructField("startTime",StringType),
      StructField("startDate", StringType),
      StructField("routeId", StringType),
      StructField("latitude", FloatType),
      StructField("longitude",FloatType),
      StructField("speed", FloatType),
      StructField("currentStopSequence", IntegerType),
      StructField("timestamp", LongType),
      StructField("stopId", StringType),
      StructField("id", StringType),
      StructField("label", StringType),
    ))
  }
}
