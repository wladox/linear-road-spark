package com.github.wladox.component

import com.github.wladox.model.XwaySegDir
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.{State, StateSpec}

object SegmentAnalytics {

  case class SegmentStatistics(numberOfVehicles:Int, sumOfSpeeds:Double, accidentExists:Boolean, toll:Double)

  def update(accidents: DStream[(XWaySegDirMinute, Boolean)],
             numberOfVehicles: DStream[(XWaySegDirMinute, Int)],
             avgVelocitiesPerMinute: MapWithStateDStream[XWaySegDirMinute, (Double, Int), (Double, Int),
               (XWaySegDirMinute, Double)]):MapWithStateDStream[XwaySegDir, (Short, Boolean, Int, Double),
    Map[Short, SegmentStatistics], (XwaySegDir, SegmentStatistics)] = {

    val segmentState = StateSpec.function(updateTolls _)

    accidents
      .join(numberOfVehicles)
      .join(avgVelocitiesPerMinute)
      .map(r => {
        val key = r._1
        val hasAccidents = r._2._1._1
        val count = r._2._1._2
        val speed = r._2._2
        (XwaySegDir(key.xWay, key.seg, key.dir), (key.minute.toShort, hasAccidents, count, speed))
      })
      .mapWithState(segmentState)
  }


  /**
    * Updates segment statistics per expressway, segment and direction. The state is maintained as a map with minutes used keys.
    *
    * @param key Key of the state object
    * @param value Tuple(Minute, Pos, Accident, Count, Speed)
    * @param state Map(Minute -> (Position, Count, Speed))
    * @return updated segment statistics for the given key
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