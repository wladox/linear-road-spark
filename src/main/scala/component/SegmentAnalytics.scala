package component

import model.{PositionReport, XwaySegDir}
import org.apache.spark.streaming.State

object SegmentAnalytics {

  case class SegmentStatistics(numberOfVehicles:Int, sumOfSpeeds:Double, accidentExists:Boolean, toll:Double)

  /**
    * Checks if the segment of current position report is different in compare to the previous one.
    * Different segments indicate that the vehicle passed to the next segment.
    *
    * @param vid vehicle ID
    * @param newReport current position report
    * @param segment Previous segment number
    * @return (VID, segmentCrossed)
    */
  def update(vid: Int, newReport: Option[PositionReport], segment: State[Byte]): (PositionReport, Boolean) = {

    val oldSegment = segment.getOption().getOrElse(-1)
    if (newReport.get.segment != oldSegment) {
      segment.update(newReport.get.segment)
      (newReport.get, true)
    } else {
      (newReport.get, false)
    }

  }

  /**
    *
    * @param key
    * @param value Minute, Pos, Accident, Count, Speed
    * @param state (Minute -> (Position, Count, Speed))
    * @return
    */
  def updateTolls(key:XwaySegDir, value:Option[(Short, Boolean, Int, Double)], state:State[Map[Short, SegmentStatistics]]): (XwaySegDir, SegmentStatistics) = {

    val currentState = state.getOption().getOrElse(Map[Short, SegmentStatistics]())

    val minute = value.get._1
    val hasAccidents = value.get._2
    val count = value.get._3
    val speed = value.get._4
    val lav  = speed / count
    val toll = if (lav >= 40 || count <= 50) 0 else 2 * math.pow(count - 50, 2)

    val currentStatistics:SegmentStatistics = currentState.getOrElse(minute, SegmentStatistics(0, 0, false, 0))

    val newState = currentState ++ Map(minute -> SegmentStatistics(currentStatistics.numberOfVehicles+count, speed, hasAccidents, toll))
    state.update(newState)

    /*if (vehicleInAccident) {
    } else {
      val newState = currentState ++ Map(minute -> SegmentStatistics(count, speed, false))
      state.update(newState)
    }*/

    val prevMinute = minute-1
    val s = newState.getOrElse(prevMinute.toShort, SegmentStatistics(0, 0, false, 0))
    (key, s)


  }

}