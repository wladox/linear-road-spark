package com.github.wladox.component

import com.github.wladox.model.PositionReport
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}

object VehicleStatistics {

  case class VehicleInformation(minute:Int, xWay:Short, position:Int, direction:Byte, samePositionCounter:Byte,
                                stopped:Boolean, segmentCrossing:Boolean, speed:Double, internalTime:Long)

  /**
    * Updates latest vehicle state to calculate current speed, whether the vechile is stopped.
    *
    * @param vid    Vehicle ID
    * @param value  Position report
    * @param state  @see VehicleInformation
    * @return stream with updated vehicle information
    */
  def updateVehicleInformation(vid:Int, value:Option[PositionReport], state:State[VehicleInformation]):(Int, VehicleInformation)= {

    val currentReport = value.get

    val previousPosition:VehicleInformation = state.getOption().getOrElse(VehicleInformation(-1, -1, -1, -1, -1,
      stopped = false, segmentCrossing = false, -1, -1))
    val pXway   = previousPosition.xWay
    val pPos    = previousPosition.position
    val pDir    = previousPosition.direction
    val counter = previousPosition.samePositionCounter

    val reportsAreEqual = {
      pXway == currentReport.xWay && pPos == currentReport.position && pDir == currentReport.direction
    }

    val segmentCrossed = currentReport.position/5280 != pPos/5280

    val newCounter  = if (reportsAreEqual) counter + 1 else 1
    val stopped     = newCounter >= 4
    val newVehiclePosition = VehicleInformation(currentReport.time/60+1,currentReport.xWay.toShort, currentReport.position,
      currentReport.direction, newCounter.toByte, stopped, segmentCrossed, currentReport.speed, currentReport.internalTime)

    state.update(newVehiclePosition)

    (vid, newVehiclePosition)

  }

  /**
    * Updates each vehicle's information. @see updateVehicleInformation
    *
    * @param input Keyed stream of vehicles with vehicleID as key, and VehicleInformation as value
    * @return
    */
  def process(input:DStream[(Int, PositionReport)]):DStream[(Int, VehicleInformation)] = {
    val vehicleInformation  = StateSpec.function(updateVehicleInformation _)
    input.mapWithState(vehicleInformation)
  }

}