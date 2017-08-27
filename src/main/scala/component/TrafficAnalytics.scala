package component

import model._
import org.apache.spark.streaming.State
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by root on 04.01.17.
  */
object TrafficAnalytics {

  case class CarSpeed(vid:Int, spd:Int)
  case class TollNotification(typ:Int, vid:Int, time: Long, emit: Long, spd: Double, toll: Double, nov:Int) {
    override def toString =
      "TOLL: (type: " + typ + ", time: " + time + ", emit: "+ emit + ", vehicle: " + vid + ", speed: " + spd + ", toll: " + toll + ", nov: " + nov + ")"
  }
  case class CarStoppedEvent(vid:Int, pos: Int)
  case class PositionReportHistory(xWay:Int, lane:Short, pos:Int, dir:Byte, count:Int)
  case class LAVEvent(key: MinuteXWaySegDir, state:Double) {
    override def toString: String = s"AVG VELOCITY on $key is $state"
  }
  case class NOVEvent(key: MinuteXWaySegDir, nov:Int) {
    override def toString: String = s"NOV: $key -> $nov"
  }

  case class MinuteXWaySegDir(minute: Short, xWay:Int, seg:Byte, dir:Byte) {
    override def toString: String = minute + "." + xWay + "." + seg + "." + dir
  }

  case class  XwaySegDirVidMin(xWay:Int, seg:Byte, dir:Byte, vid:Int, minute:Short)

  /**
    * Counts vehicles per minute.
    *
    * @param key
    * @param vid
    * @param vehicles
    * @return
    */
  def vehicleCountPerMinute(key: MinuteXWaySegDir, vid:Option[Int], vehicles:State[Set[Int]]) : (MinuteXWaySegDir, Int) = {
    val currentSet = vehicles.getOption().getOrElse(Set[Int]())
    val newSet = currentSet + vid.get
    vehicles.update(newSet)
    (key, newSet.size)
  }

  /**
    * Updates each vehicle's speed per minute.
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def vehicleAvgSpd(key: XwaySegDirVidMin, value:Option[Double], state:State[Double]):(MinuteXWaySegDir, (Double, Double)) = {
    val currentSpd = value.get
    if (state.exists()) {
      val oldSpd = state.get()
      val avg = (oldSpd + currentSpd) / 2
      state.update(avg)
      (MinuteXWaySegDir(key.minute, key.xWay, key.seg, key.dir), (oldSpd, currentSpd) )
    } else {
      state.update(currentSpd)
      (MinuteXWaySegDir(key.minute, key.xWay, key.seg, key.dir), (0.0, currentSpd) )
    }
//    val newSpds = value.get + current
//    state.update(newSpds)
//    val avgSpd = newSpds.sum / newSpds.length
//    (MinuteXWaySegDir(key.minute, key.xWay, key.seg, key.dir), avgSpd )
  }

  /**
    * Updates sum of vehicle speeds per minute.
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def spdSumPerMinute(key:MinuteXWaySegDir, value:Option[(Double, Double)], state:State[Double]):(MinuteXWaySegDir, Double) = {
    if (state.exists()) {
      val oldAvg = state.get()
      val newState = oldAvg - value.get._1 + value.get._2
      state.update(newState)
      (key, newState)
    } else {
      state.update(value.get._2)
      (key, value.get._2)
    }

  }

  /**
    * Lates average velocity
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def lav(key:XwaySegDir, value:Option[(Short, Double)], state:State[collection.immutable.Map[Short, Double]]):(MinuteXWaySegDir, Double) = {

    val minute = value.get._1
    val speed = value.get._2

    if (state.exists()) {
      val map = state.get()
      if (minute <= 5) {
        val newMap = collection.immutable.Map[Short, Double](minute -> speed) ++ map
        state.update(newMap)
      } else {
        val newMap = collection.immutable.Map[Short, Double](minute -> speed) ++ map.slice(minute-5, minute-1)
        state.update(newMap)
      }
      val speeds = for (i <- minute-5 until minute if i > 0 && map.contains(i.toShort)) yield map(i.toShort)
      if (speeds.nonEmpty) {
        (MinuteXWaySegDir(minute, key.xWay, key.segment, key.direction), speeds.sum / speeds.length)
      } else {
        (MinuteXWaySegDir(minute, key.xWay, key.segment, key.direction), 0.0)
      }

    } else {
      // first minute of simulation, thus no LAV exists
      val newMap = collection.immutable.Map[Short, Double](minute -> speed)
      state.update(newMap)
      (MinuteXWaySegDir(minute, key.xWay, key.segment, key.direction), 0.0)
    }

  }

  /**
    *
    * @param allEvents
    * @return
    */
//  def vidAverageSpeedPerMin(allEvents: DStream[LREvent]):DStream[(MinuteXwaySegDirVIDKey, Double)] = {
//
//    allEvents
//      .map(event => (MinuteXwaySegDirVIDKey(event.vid, event.time / 60 +1, event.xWay, event.seg, event.dir), event.speed))
//      .combineByKey(
//        x => (x, 1),
//        (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
//        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2),
//        new HashPartitioner(2)
//      )
//      .map(r => (r._1, (r._2._1 / r._2._2.toDouble)))
//
//  }

  /**
    *
    * @param avgsv
    * @return
    */
//  def speedAVG(avgsv: DStream[(MinuteXwaySegDirVIDKey, Double)]):DStream[(MinuteXWaySegDir, Double)]= {
//
//    avgsv
//      .window(Minutes(5), Seconds(1))
//      .map(e => {
//        ( MinuteXWaySegDir(e._1.minute, e._1.xWay, e._1.seg, e._1.dir), e._2 )
//      })
//      .combineByKey(
//        value => (value, 1),
//        (acc: (Double, Int), value) => (acc._1 + value, acc._2 + 1),
//        (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2),
//        new HashPartitioner(2)
//      )
//      .map(r => (r._1, r._2._1 / r._2._2.toDouble))
//
//  }

  /**
    *
    * @param allEvents
    * @return
    */
  def numOfVehicles(allEvents: DStream[Event]):DStream[(MinuteXWaySegDir, Int)] = {
    allEvents
      .map(event => {
        (MinuteXWaySegDir((event.time / 60 + 1).toShort, event.xWay, event.segment, event.direction), 1)
      })
      .reduceByKey((val1, val2) => val1 + val2)
  }

  /**
    *
    * @param lav
    * @param nov
    * @param crossing
    * @return
    */
//  def tollNotification(
//                        lav:DStream[(MinuteXWaySegDir, Double)],
//                        nov:DStream[(MinuteXWaySegDir, Int)],
//                        crossing:DStream[Option[SegmentCrossingEvent]]
//  ):DStream[TollNotification] = {
//
//    val toNotify = crossing
//      .filter(event => event.get.notification)
//      .map(event => (MinuteXWaySegDir(event.get.minute, event.get.xWay, event.get.seg, event.get.dir), event.get.vid))
//
//    toNotify
//      .join(lav)
//      .join(nov)
//      .map(record => {
//
//        val lav = record._2._1._2
//        val nov = record._2._2
//        val vid = record._2._1._1
//
//        if (lav >= 40 || nov <= 50){
//          val toll = 0.0
//          TollNotification(0, vid, record._1.minute, System.currentTimeMillis(), lav, toll, nov)
//        } else {
//          val toll = 2 * Math.pow(nov - 50, 2)
//          TollNotification(0, vid, record._1.minute, System.currentTimeMillis(), lav, toll, nov)
//        }
//      })
//      .filter(toll => toll.toll != 0.0)
//  }

  /**
    *
    * @param events
    */
//  def accidentDetection(events: DStream[LREvent]):Unit = {
//
//    val prState = StateSpec.function(positionReportsMappingFunction _)
//    val stoppedCars =
//      events
//      .map(ps => (ps.vid, PositionReport(ps.time, ps.vid, ps.speed, ps.xWay, ps.lane, ps.dir, ps.seg, ps.pos)))
//      .mapWithState(prState)
//      .filter(e => e.isDefined)
//      .map(e => (e.get._1, e.get._2))
//
//  }

  /**
    * Stores the current counter of equal consecutive position reports indicating the vehicle is stopped
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def positionReportsMappingFunction(key:Int, value:Option[PositionReport], state:State[PositionReportHistory]):Option[(XWayLanePosDirKey, Int)] = {
    val newState = value.get
    if (state.getOption().isDefined && newState.xWay == state.get().xWay
        && newState.lane == state.get().lane
        && newState.position == state.get().pos
        && newState.direction == state.get().dir
        && state.get().count >= 3) {
          state.update(PositionReportHistory(newState.xWay, newState.lane, newState.position, newState.direction, state.get().count+1))
          (XWayLanePosDirKey(newState.xWay, newState.lane, newState.position, newState.direction), newState.vid)
    } else {
      state.update(PositionReportHistory(newState.xWay, newState.lane, newState.position, newState.direction, 1))
    }
    None
  }

}
