package com.github.wladox.component

import com.github.wladox.model.PositionReport
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}

object VehicleStatistics {

  case class VehicleInformation(time:Int,
                                vid:Int,
                                xWay:Int,
                                position:Int,
                                lane:Int,
                                direction:Byte,
                                speed:Double,
                                internalTime:Long,
                                prevLane: Int,
                                prevPos:Int)


  /**
    * Updates each vehicle's information. @see updateVehicleInformation
    *
    * @param input Keyed stream of vehicles with vehicleID as key, and VehicleInformation as value
    * @return
    */
  def update(input:DStream[(Int, PositionReport)]):DStream[(Int, (VehicleInformation, Boolean, Boolean, Double, Double, Int))] = {
    val vehicleReports  = StateSpec.function(updateLastReport _).numPartitions(2)
    input.mapWithState(vehicleReports)
  }

  /**
    * Updates latest vehicle state to calculate current  speed, whether the vechile is stopped.
    *
    * @param vid    Vehicle ID
    * @param value  Position report
    * @param state  previous position report, stop counter
    * @return stream with updated vehicle information (VID, (Positionreport, isStopped, isSegmentCrossing, subtractSpeed, addSpeed, vehicleCount))
    */
  def updateLastReport(vid:Int, value:Option[PositionReport], state:State[(PositionReport, Int)]):(Int, (VehicleInformation, Boolean, Boolean, Double, Double, Int)) = {

    val currentReport = value.get

    state.getOption() match {
      case Some(p) =>
        val reportsAreEqual = {
          p._1.xWay == currentReport.xWay && p._1.position == currentReport.position && p._1.direction == currentReport.direction
        }
        val newCounter  = if (reportsAreEqual) p._2 + 1 else 1
        val segmentCrossed = currentReport.segment != p._1.segment && currentReport.time/60 > p._1.time/60
        val vehicleCount = if (currentReport.time/60 != p._1.time/60) 1 else 0
        state.update((currentReport, newCounter))
        val isStopped = newCounter >= 4
        val vehInfo = VehicleInformation(currentReport.time,
          currentReport.vid,
          currentReport.xWay,
          currentReport.position,
          currentReport.lane,
          currentReport.direction,
          currentReport.speed,
          currentReport.internalTime,
          p._1.lane, p._1.position)
        (vid, (vehInfo, isStopped, segmentCrossed, p._1.speed, currentReport.speed, vehicleCount))
      case None =>
        state.update(currentReport, 1)
        val vehInfo = VehicleInformation(currentReport.time,
          currentReport.vid,
          currentReport.xWay,
          currentReport.position,
          currentReport.lane,
          currentReport.direction,
          currentReport.speed,
          currentReport.internalTime,
          -1, -1)
        (vid, (vehInfo, false, true, 0, currentReport.speed, 1))
    }

//    val previousPosition:VehicleInformation = state.getOption().getOrElse(
//      VehicleInformation(-1, -1, -1, -1, -1, stopped = false, segmentCrossing = false, -1, -1)
//    )

//    val segmentCrossed = currentReport.position/5280 != pPos/5280
//    val stopped     = newCounter >= 4
//    val newVehiclePosition = VehicleInformation(currentReport.time/60+1,currentReport.xWay.toShort, currentReport.position,
//      currentReport.direction, newCounter.toByte, stopped, segmentCrossed, currentReport.speed, currentReport.internalTime)
//
//    state.update(newVehiclePosition)

//    (vid, newVehiclePosition)

  }

}