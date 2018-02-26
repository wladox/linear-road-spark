package com.github.wladox.component

import com.github.wladox.LinearRoadBenchmark.XwayDir
import com.github.wladox.component.VehicleStatistics.VehicleInformation
import com.github.wladox.model.XwaySegDir
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.{State, StateSpec}

/**
  * Created by wladox on 04.01.17.
  */
object TrafficAnalytics {

  case class CarSpeed(vid:Int, spd:Int)
  case class TollNotification(typ:Int, vid:Int, time: Long, emit: Long, spd: Double, toll: Double, nov:Int) {
    override def toString =
      "TOLL: (type: " + typ + ", time: " + time + ", emit: "+ emit + ", vehicle: " + vid + ", speed: " + spd + ", toll: " + toll + ", nov: " + nov + ")"
  }
  case class CarStoppedEvent(vid:Int, pos: Int)
  case class PositionReportHistory(xWay:Int, lane:Short, pos:Int, dir:Byte, count:Int)
  case class LAVEvent(key: XWaySegDirMinute, state:Double) {
    override def toString: String = s"AVG VELOCITY on $key is $state"
  }
  case class NOVEvent(key: XWaySegDirMinute, nov:Int) {
    override def toString: String = s"NOV: $key -> $nov"
  }

  case class XWaySegDirMinute(xWay:Int, seg:Byte, dir:Byte, minute: Int) {
    override def toString: String = minute + "." + xWay + "." + seg + "." + dir
  }

  case class  XwaySegDirVidMin(xWay:Int, seg:Byte, dir:Byte, vid:Int, minute:Short)

  /**
    * Counts vehicles per minute.
    *
    * @param key - XWaySegDirMinute - (XWay, Segment, Direction, Minute)
    * @param value - number of new vehicles in the batch
    * @param state - current number of vehicles on an expressway,segment,direction and minute
    * @return - updated number of vehicles
    */
  def vehicleCountPerMinute(key: XWaySegDirMinute, value:Option[Int], state:State[Int]) : (XWaySegDirMinute, Int) = {
    val currentCount = state.getOption().getOrElse(0)
    val newCount = currentCount + value.get
    state.update(newCount)
    (XWaySegDirMinute(key.xWay, key.seg, key.dir, key.minute), newCount)
  }

  /**
    * Updates velocity averages per minute. In each batch of tuples it receives the sum of vehicle average speeds on
    * a given expressway, segment and direction, and the corresponding number of vehicles in that batch. To produce the
    * output, it separately updates the sum and the count, and finally computes the average.
    *
    * @param key - XWaySegDirMinute - (XWay, Segment, Direction, Minute)
    * @param value - Sum of velocities of all vehicles and the number of vehicles
    * @param state - Sum of average velocities, Sum of vehicles
    * @return - Average velocity per Expressway,Segment,Direction,Minute
    */
  def vehicleAvgSpd(key: XwayDir, value:Option[(Int,Int,Int,Int,Int,Int)], state:State[Map[(Int, Int), Double]]):(Int,(Int,Int,Double,Int)) = {

    val vid     = value.get._1
    val segment = value.get._2
    val minute  = value.get._3
    val nov   = value.get._4
    val posReportSum = value.get._5
    val totalSpeed = value.get._6.toDouble


    val currentState = state.getOption().getOrElse(Map())
    //val newSpeed = if (currentState.contains((segment,minute))) currentState((segment,minute)) + speed else speed
    val newSpeed = totalSpeed / posReportSum
    val newState = currentState + ((segment,minute) -> newSpeed)

    state.update(newState)

    val from = minute - 1
    val to = minute - 6
    val fiveMinSum = for (i <- from until to) yield {
      if (newState.contains((segment, i))) newState((segment,i)) else 0
    }

    (vid, (segment, minute, fiveMinSum.sum/5, nov))
  }

  /**
    *
    * @param key
    * @param value (Vid, segment, minute, speedSum, positionReportsSum, NOV)
    * @param state Map([Segment,Minute] -> AVG)
    * @return
    */
  def minuteAvgSpeed(key: XwayDir, value:Option[(Int,Int,Int,Double,Int,Int)], state:State[Map[(Int, Int), Double]]):(Int, Double, Int) = {

    val vid     = value.get._1
    val segment = value.get._2
    val minute  = value.get._3
    val speedSum   = value.get._4
    val posRepSum = value.get._5
    val nov = value.get._6
    val avg = speedSum/posRepSum

    val currentState = state.getOption().getOrElse(Map())
    val newState = currentState + ((segment,minute) -> avg)

    state.update(newState)

    val from = minute - 1
    val to = minute - 6
    val fiveMinSum = for (i <- from until to) yield {
      if (newState.contains((segment, i))) newState((segment,i)) else 0
    }

    (vid, fiveMinSum.sum/5, nov)
  }


  /*def vehicleAvgSpd(key: XWaySegDirMinute, value:Option[(Double,Int)], state:State[(Double,Int)]):(XWaySegDirMinute, Double) = {
    val currentSpd = value.get

    state.getOption() match {
      case Some(s) =>
        val newSpeed = s._1 + currentSpd._1
        val newCount = s._2 + currentSpd._2
        state.update((newSpeed, newCount))
        (key, newSpeed/newCount)
      case None =>
        state.update(currentSpd)
        (key, currentSpd._1/currentSpd._2)
    }
  }*/

  /**
    * Updates sum of vehicle speeds per minute.
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def spdSumPerMinute(key:XWaySegDirMinute, value:Option[(Double, Double)], state:State[Double]):(XWaySegDirMinute, Double) = {

    val oldAvg = state.getOption().getOrElse(0.0)
    val newState = oldAvg - value.get._1 + value.get._2
    state.update(newState)
    (XWaySegDirMinute(key.xWay, key.seg, key.dir, key.minute), newState)

  }

  /**
    * Lates average velocity
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def lav(key:XwaySegDir, value:Option[(Int, Double)], state:State[collection.immutable.Map[Int, Double]]):(XWaySegDirMinute, Double) = {

    val minute = value.get._1
    val speed = value.get._2

    state.getOption() match {
      case Some(s) =>
        if (minute <= 5) {
          val newMap = collection.immutable.Map[Int, Double](minute -> speed) ++ s
          state.update(newMap)
        } else {
          val newMap = collection.immutable.Map[Int, Double](minute -> speed) ++ s.slice(minute-5, minute-1)
          state.update(newMap)
        }
        val speeds = for (i <- minute-5 until minute if i > 0 && s.contains(i.toShort)) yield s(i.toShort)
        if (speeds.nonEmpty) {
          (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), speeds.sum / speeds.length)
        } else {
          (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), 0.0)
        }
      case None =>
        // first minute of simulation, thus no LAV exists
        val newMap = collection.immutable.Map[Int, Double](minute -> speed)
        state.update(newMap)
        (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), 0.0)
    }

  }

  /**
    * Counts vehicles in the current batch that crossed into a new segment.
    *
    * @param input Keyed stream of vehicles with vehicleID as key, and VehicleInformation as value
    * @return number of new vehicles per Xway, Segment, Direction and Minute
    */
  /*def countVehiclesInBatch(input:DStream[(Int, VehicleInformation)]):DStream[(XWaySegDirMinute, Int)] = {
    input
      .map(r => (XWaySegDirMinute(r._2.xWay, (r._2.position/5280).toByte, r._2.direction, r._2.minute), if (r._2.segmentCrossing) 1 else 0))
      .reduceByKey((x,y) => x + y)
  }*/

  /**
    * Calculates the average velocity of all vehicles in the current batch per Xway,Segment,Direction and minute
    * by building the sum of the speeds and of the number of vehicles.
    *
    * @param input Keyed stream of vehicles with vehicleID as key, and VehicleInformation as value
    * @return @see vehicleAvgSpd
    */
  /*def averageVelocities(input:DStream[(Int, VehicleInformation)]):MapWithStateDStream[XWaySegDirMinute, (Double, Int), (Double, Int), (XWaySegDirMinute, Double)] = {
    val state = StateSpec.function(vehicleAvgSpd _)

    input.map(r => {
      val info = r._2
      (XWaySegDirMinute(info.xWay, (info.position/5280).toByte, info.direction, info.minute), (info.speed, 1))
    })
    .reduceByKey((t1,t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })
    .mapWithState(state)
  }*/

}
