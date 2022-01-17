package com.github.yoda_mon.spark_gtfs_rt

import com.google.transit.realtime.GtfsRealtime.VehiclePosition
import org.apache.spark.unsafe.types.UTF8String

case class VehiclePositionConverter(vp: VehiclePosition) {
  def toSeq: Seq[Any] = {
    val trip = vp.getTrip
    val tripId: String = trip.getTripId
    val startTime: String = trip.getStartTime
    val startDate: String = trip.getStartDate
    val routeId: String = trip.getRouteId

    val position = vp.getPosition
    val latitude: Float = position.getLatitude
    val longitude: Float = position.getLongitude
    val speed: Float = position.getSpeed

    val currentStopSequence: Int = vp.getCurrentStopSequence
    val timestamp: Long = vp.getTimestamp
    val stopId: String = vp.getStopId

    val vehicle = vp.getVehicle
    val id: String = vehicle.getId
    val label: String = vehicle.getLabel
    Seq(
      UTF8String.fromString(tripId), UTF8String.fromString(startTime),
      UTF8String.fromString(startDate), UTF8String.fromString(routeId),
      latitude, longitude, speed,
      currentStopSequence,
      timestamp,
      UTF8String.fromString(stopId),
      UTF8String.fromString(id), UTF8String.fromString(label)
    )
  }
}
