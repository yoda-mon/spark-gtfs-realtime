package com.github.yoda_mon.spark_gtfs_rt

import com.google.transit.realtime.GtfsRealtime.{FeedEntity, FeedMessage}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.URL
import java.util
import scala.jdk.CollectionConverters._

class DefaultSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table =
    new GTFSRealTimeTable()
}

class GTFSRealTimeTable extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = VehiclePositionSchema.getSchema()

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.MICRO_BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new SimpleScanBuilder()
}

class SimpleScanBuilder extends ScanBuilder {
  override def build(): Scan = new SimpleScan
}

class SimpleScan extends Scan {
  override def readSchema(): StructType = VehiclePositionSchema.getSchema()

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = new SimpleMicroBatchStream()
}

class SimpleOffset(value: Int) extends Offset {
  override def json(): String = s"""{"value":"$value"}"""
}

class SimpleMicroBatchStream extends MicroBatchStream {
  var latestOffsetValue = 0

  override def latestOffset(): Offset = {
    latestOffsetValue += 10
    new SimpleOffset(latestOffsetValue)
  }

  override def planInputPartitions(offset: Offset, offset1: Offset): Array[InputPartition] = Array(new SimplePartition)

  override def createReaderFactory(): PartitionReaderFactory = new GTFSRTPartitionReaderFactory()

  override def initialOffset(): Offset = new SimpleOffset(latestOffsetValue)

  override def deserializeOffset(s: String): Offset = new SimpleOffset(latestOffsetValue)

  override def commit(offset: Offset): Unit = {}

  override def stop(): Unit = {}
}


class SimplePartition extends InputPartition

class GTFSRTPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new GTFSRTPartitionReader
}

class GTFSRTPartitionReader extends PartitionReader[InternalRow] {

  var iterator: Iterator[FeedEntity] = null

  def next: Boolean = {
    if (iterator == null) {
      val url = new URL("https://km.bus-vision.jp/realtime/sankobus_vpos_update.bin")
      val feed = FeedMessage.parseFrom(url.openStream())
      val entityList = feed.getEntityList
      iterator = entityList.iterator().asScala
    }
    iterator.hasNext
  }

  def get: InternalRow = {
    val entity = iterator.next()
    val row = if (entity.hasVehicle) {
      val vp = VehiclePositionConverter(entity.getVehicle).toSeq
      InternalRow.fromSeq(vp)
    } else {
      InternalRow()
    }
    row
  }
  def close(): Unit = {}
}