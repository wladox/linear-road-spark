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

  /**
    *
    * @param vid
    * @param value
    * @param state (Pos, Count) ? reduce memory consumption through storing only relevant fields
    * @return
    */
  def updateVehicleInformation(vid:Int, value:Option[PositionReport], state:State[(VehiclePosition, Byte)]):Option[(MinuteXwayLanePosDir, Int)]= {

    val currentReport = value.get

    val previousPosition:(VehiclePosition, Byte) = state.getOption().getOrElse(VehiclePosition(-1, -1, -1, -1), 0)

    val pXway = previousPosition._1.xWay
    val pLane = previousPosition._1.lane
    val pPos = previousPosition._1.position
    val pDir = previousPosition._1.direction

    val counter:Byte = previousPosition._2

    val reportsAreEqual = {
      pXway == currentReport.xWay && pLane == currentReport.lane && pPos == currentReport.position && pDir == currentReport.direction
    }

    val newCounter = if (reportsAreEqual) counter + 1 else 1
    val newVehiclePosition = VehiclePosition(currentReport.xWay.toShort, currentReport.lane, currentReport.position, currentReport.direction)

    state.update((newVehiclePosition, newCounter.toByte))

    if (newCounter >= 4) {
      Some(MinuteXwayLanePosDir((currentReport.time/60+1).toShort, currentReport.xWay, currentReport.lane, currentReport.position, currentReport.direction), vid)
    } else {
      Some(MinuteXwayLanePosDir((currentReport.time/60+1).toShort, currentReport.xWay, currentReport.lane, currentReport.position, currentReport.direction), -1)
    }

  }

  /**
    *
    * @param key MinuteXwayLanePosDir
    * @param vid VehicleID
    * @param state Set containing stopped vehicle IDs
    * @return XwaySegDir, (minute, accidentExists)
    */
  def updateAccidents(key:MinuteXwayLanePosDir, vid:Option[Int], state:State[Set[Int]]): (XWaySegDirMinute, (Int, Boolean)) = {

    val stoppedVid = vid.get
    val currentState = state.getOption().getOrElse(Set[Int]())

    if (stoppedVid != -1) {
      val newState = Set(stoppedVid) ++ currentState
      state.update(newState)
      (XWaySegDirMinute(key.xWay, (key.pos/5280).toByte, key.dir, key.minute), (key.pos, newState.size >= 2))
    } else {
      (XWaySegDirMinute(key.xWay, (key.pos/5280).toByte, key.dir, key.minute), (key.pos, currentState.size >= 2))
    }


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

