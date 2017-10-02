package com.github.wladox.component

import com.github.wladox.component.TrafficAnalytics.XWaySegDirMinute
import com.github.wladox.component.VehicleStatistics.VehicleInformation
import com.github.wladox.model.{PositionReport, XwaySegDir}
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.DStream

/**
  * This class provides functionality to maintain latest vehicle's state and accident detection
  */

object AccidentAnalytics {

  case class MinuteXwayLanePosDir(minute:Short, xWay: Int, lane:Byte, pos:Int, dir:Byte)

  /**
    *
    * @param input Keyed stream of vehicles with vehicleID as key, and VehicleInformation as value
    * @return
    */
  def process(input:DStream[(Int, VehicleInformation)]):DStream[(XWaySegDirMinute, Boolean)] = {

    val accidentsState = StateSpec.function(updateAccidents _)

    input.map(r => {
      (XwaySegDir(r._2.xWay, (r._2.position/5280).toByte, r._2.direction), (r._1, r._2.position, r._2.stopped, r._2.minute))
    })
    .mapWithState(accidentsState)
    .reduceByKey((c1, c2) => {
      c1 || c2
    })
  }


  /**
    *
    * Updates information about accidents on a given expressway, segment and direction.
    * Maintains a Map in which position of an accident is the key, and the list containing vehicle IDs involved in that accident as the value.
    *
    * @param key Expressway,Segment,Direction
    * @param value Tuple consisting of (vehicleId, position, stopped flag, minute)
    * @param state Set containing IDs of stopped vehicles
    * @return Tuple with XWaySegDirMinute and a flag signaling the existence of accidents
    *
    */
  def updateAccidents(key:XwaySegDir, value:Option[(Int,Int,Boolean,Int)], state:State[Map[Int, Set[Int]]]): (XWaySegDirMinute, Boolean) = {

    val vid       = value.get._1
    val position  = value.get._2
    val stopped   = value.get._3
    val minute    = value.get._4

    val accidentsMap  = state.getOption().getOrElse(Map[Int, Set[Int]]())

    if (stopped) {
      val stoppedVehicles = accidentsMap.getOrElse(position, Set())
      val newSet    = Set(vid) ++ stoppedVehicles
      val newState  = Map(position -> newSet) ++ (accidentsMap - position)
      state.update(newState)

      val hasAccident = newState.exists(r => {
        r._2.size >= 2
      })
      (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), hasAccident)
    } else {
      // clear accident
      val newState = accidentsMap.filter(r => {
        !r._2.contains(vid)
      })

      state.update(newState)
      val hasAccident = newState.exists(r => {
        r._2.size >= 2
      })
      (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), hasAccident)
    }

  }

}

