package component

import model.{PositionReport, XwaySegDir}
import org.apache.spark.streaming.State

object SegmentStatistics {

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
    * @param state
    * @return
    */
  def updateTolls(key:XwaySegDir, value:Option[(Short, Int, Boolean, Int, Double)], state:State[Map[Short, (Int, Int, Double)]]): (XwaySegDir, (Boolean, Int, Double)) = {

    val currentState = state.getOption().getOrElse(Map[Short, (Int, Int, Double)]())
    val minute = value.get._1
    val position = value.get._2
    val vehicleInAccident = value.get._3
    val count = value.get._4
    val speed = value.get._5

    if (vehicleInAccident) {
      val newState = currentState ++ Map(minute -> (position, count, speed))
      state.update(newState)
    } else {
      val newState = currentState ++ Map(minute -> (-1, count, speed))
      state.update(newState)
    }

    val prevMinute = minute - 1
    val segmentStat = state.get().getOrElse(prevMinute.toShort, (-1,-1,-1.0))
    (key, (segmentStat._1 != -1, segmentStat._2, segmentStat._3))


  }

}