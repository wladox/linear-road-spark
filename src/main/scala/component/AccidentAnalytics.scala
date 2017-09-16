package component

import component.TrafficAnalytics.XWaySegDirMinute
import model.{PositionReport, XWayLanePosDirKey, XwaySegDir}
import org.apache.spark.streaming.State

object AccidentAnalytics {

  case class MinuteXwayLanePosDir(minute:Short, xWay: Int, lane:Byte, pos:Int, dir:Byte)
  case class MinXwayDir(minute:Short, xWay:Int, dir:Byte)
  case class Accident(minute:Short, pos:Int)
  case class VehicleStop(vid:Int)
  case class VehiclePosition(xWay:Short, lane:Byte, position:Int, direction:Byte)
  case class VehicleInformation(minute:Int, xWay:Short, position:Int, direction:Byte, samePositionCounter:Byte,
                                stopped:Boolean, segmentCrossing:Boolean, speed:Double, internalTime:Long)

  /**
    *
    * @param vid
    * @param value
    * @param state (Pos, Count) ? reduce memory consumption through storing only relevant fields
    * @return
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
    *
    * @param key MinuteXwayLanePosDir
    * @param info VehicleID
    * @param state Set containing stopped vehicle IDs
    * @return XwaySegDir, (minute, accidentExists)
    */
  def updateAccidents(key:XWaySegDirMinute, info:Option[(Int,Int,Boolean)], state:State[Map[Int, Set[Int]]]): (XWaySegDirMinute, Boolean) = {

    val vid       = info.get._1
    val position  = info.get._2
    val stopped   = info.get._3

    val minute = key.minute
    val accidentsMap  = state.getOption().getOrElse(Map[Int, Set[Int]]())

    if (stopped) {
      val stoppedVehicles = accidentsMap.getOrElse(position, Set())
      val newSet    = Set(vid) ++ stoppedVehicles
      val newState  = Map(position -> newSet) ++ (accidentsMap - position)
      state.update(newState)

      val hasAccident = newState.exists(r => {
        r._2.size >= 2
      })
      (key, hasAccident)
    } else { // clear accident

//      val wasInAccident = accidentsMap.exists(r => {
//        r._2.contains(vid)
//      })
      val newState = accidentsMap.filter(r => {
        !r._2.contains(vid)
      })

      state.update(newState)
      val hasAccident = newState.exists(r => {
        r._2.size >= 2
      })
      (key, hasAccident)
    }

    //val accExists = state.get().map

    //(XWaySegDirMinute(key.xWay, (vehicleInfo.position/5280).toByte, vehicleInfo.direction, vehicleInfo.minute), false)

  }

  case class XwayDir(xWay:Int, dir:Byte)

  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
//  def updateAccidents(key:XwayDir, value:Option[(Int, Boolean, PositionReport)], state:State[Map[Int, Boolean]]): (XwaySegDir, Seq[Boolean]) = {
//
//    val existing = state.getOption().getOrElse(Map[Int,Boolean]())
//    val segment = value.get._1
//    val accidentExists = value.get._2
//    val newState = existing ++ Map(segment -> accidentExists)
//
//    state.update(newState)
//
//    if (key.dir == 0) {
//      (XwaySegDir(key.xWay, segment.toByte, key.dir),for (i <- List.range(0,5)) yield newState.getOrElse(segment + i, false))
//    } else {
//      (XwaySegDir(key.xWay, segment.toByte, key.dir),for (i <- List.range(0,5)) yield newState.getOrElse(segment - i, false))
//    }
//  }
}

