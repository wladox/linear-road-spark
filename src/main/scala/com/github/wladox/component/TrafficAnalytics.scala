package com.github.wladox.component

import com.github.wladox.XwayDir
import com.github.wladox.model.XwaySegDir
import org.apache.spark.streaming.State

/**
  * Created by Wladimir Postnikov on 04.01.17.
  */

case class XWaySegDirMinute(xWay:Byte, seg:Byte, dir:Byte, minute: Short) {

  override def toString: String = xWay + "." + seg + "." + dir + "." + minute

  def canEqual(a: Any): Boolean = a.isInstanceOf[XWaySegDirMinute]

  override def equals(that: Any): Boolean =
    that match {
      case that: XWaySegDirMinute => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + xWay.toInt
    result = prime * result + seg.toInt
    result = prime * result + dir.toInt
    result = prime * result + minute.toInt
    result
  }

}

object TrafficAnalytics {

  /**
    * Counts vehicles per minute.
    *
    * @param key - XWaySegDirMinute - (XWay, Segment, Direction, Minute)
    * @param value - number of new vehicles in the batch
    * @param state - current number of vehicles on an expressway,segment,direction and minute
    * @return - updated number of vehicles
    */
  def vehicleCount(key: XWaySegDirMinute, value:Option[Int], state:State[Int]) : (XWaySegDirMinute, Int) = {

    val newCount = state.getOption() match {
      case Some(s) => s + value.get
      case None => value.get
    }
    state.update(newCount)
    (key, newCount)
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

  /**
    * Updates sum of vehicle speeds per minute.
    *
    * @param key XWaySegDirMinute
    * @param value (Vid, Speed)
    * @param state Count of all emitted reports, Sum of all speeds
    * @return
    */
  def spdSumPerMinute(key:XWaySegDirMinute, value:Option[(Int, Int, Int)], state:State[(Int, Int)]):Option[(XwaySegDir, (Int, Int, Int, Int))] = {

    if (key.seg == 63 && key.dir == 0 && key.minute == 19)
      System.out.println()

    def updateSegmentStatistics(tuple:(Int,Int,Int)):Option[(XwaySegDir, (Int, Int, Int, Int))] = {
      val newState = state.getOption() match {
        case Some(s) => (s._1 + 1, s._2 + tuple._2)
        case None => (1, tuple._2)
      }

      state.update(newState)

      Some(XwaySegDir(key.xWay, key.seg, key.dir), (tuple._1, newState._1, newState._2, tuple._3))
    }

    value match {
      case Some(v) => updateSegmentStatistics(v)
      case _ if state.isTimingOut() => None
    }

  }

  /**
    * Lates average velocity
    *
    * @param key
    * @param value (VID, Speed, Time)
    * @param state Map(Minute -> (Speed, Count)
    * @return
    */
  def lav(key:XwaySegDir, value:Option[(Int, Int, Int)], state:State[Array[(Int,Int)]]):((Int,Int,Int), Int) = {

    val vid     = value.get._1
    val speed   = value.get._2
    val minute  = value.get._3/60+1

    val newState = state.getOption() match {
      case Some(s) =>
        s(minute) = (s(minute)._1 + speed, s(minute)._2 + 1)
        s
      case None =>
        // first minute of simulation, thus no LAV exists
        val arr = Array.fill(181)((0,0))
        arr(minute) = (speed, 1)
        arr
    }

    state.update(newState)

    val statistics = for (i <- minute-5 until minute if i > 0) yield newState(i)

    if (statistics.nonEmpty) {
      if (vid == 34753 && value.get._3 == 2400)
        System.out.print()
      val lav = statistics.foldLeft((0, 0)) { case ((t1speed, t1count), (t2speed, t2count)) => (t1speed + t2speed, t1count + t2count) }

      ((vid, value.get._3, key.xWay), math.round(lav._1/lav._2.toFloat))
    } else {
      ((vid, value.get._3, key.xWay), 0)
    }

  }

  def updateNOV(key:XwaySegDir, value:Option[(Int, Int, Int)], state:State[Map[String, Set[Int]]]):((Int,Int,Int), Int) = {

    val vid   = value.get._1
    val time  = value.get._3

    val minute = (time/60+1).toString

    val newState = state.getOption() match {
      case Some(s) => if (s.contains(minute)) {
        val newSet = Set(vid) ++ s(minute)
        s ++ Map(minute -> newSet)
      } else {
        s ++ Map(minute -> Set(vid))
      }
      case None => Map(minute -> Set(vid))
    }

    state.update(newState)

    val prevKey = (time/60).toString
    val prevMinSet = newState.getOrElse(prevKey, Set())

    if (vid == 23574 && time == 1519)
      System.out.print()
    ((vid, time, key.xWay), prevMinSet.size)

  }

}
