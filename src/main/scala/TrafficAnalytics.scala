import model._
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec}

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
  def speedAVG(avgsv: DStream[(MinuteXwaySegDirVIDKey, Double)]):DStream[(MinuteXWaySegDirKey, Double)]= {

    avgsv
      .window(Minutes(5), Seconds(1))
      .map(e => {
        ( MinuteXWaySegDirKey(e._1.minute, e._1.xWay, e._1.seg, e._1.dir), e._2 )
      })
      .combineByKey(
        value => (value, 1),
        (acc: (Double, Int), value) => (acc._1 + value, acc._2 + 1),
        (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2),
        new HashPartitioner(2)
      )
      .map(r => (r._1, r._2._1 / r._2._2.toDouble))

  }

  /**
    *
    * @param allEvents
    * @return
    */
  def numOfVehicles(allEvents: DStream[LREvent]):DStream[(MinuteXWaySegDirKey, Int)] = {
    allEvents
      .map(event => {
        (MinuteXWaySegDirKey(event.time / 60 + 1, event.xWay, event.seg, event.dir), 1)
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
  def tollNotification(
                        lav:DStream[(MinuteXWaySegDirKey, Double)],
                        nov:DStream[(MinuteXWaySegDirKey, Int)],
                        crossing:DStream[Option[SegmentCrossingEvent]]
  ):DStream[TollNotification] = {

    val toNotify = crossing
      .filter(event => event.get.notification)
      .map(event => (MinuteXWaySegDirKey(event.get.minute, event.get.xWay, event.get.seg, event.get.dir), event.get.vid))

    toNotify
      .join(lav)
      .join(nov)
      .map(record => {

        val lav = record._2._1._2
        val nov = record._2._2
        val vid = record._2._1._1

        if (lav >= 40 || nov <= 50){
          val toll = 0.0
          TollNotification(0, vid, record._1.minute, System.currentTimeMillis(), lav, toll, nov)
        } else {
          val toll = 2 * Math.pow(nov - 50, 2)
          TollNotification(0, vid, record._1.minute, System.currentTimeMillis(), lav, toll, nov)
        }
      })
      .filter(toll => toll.toll != 0.0)
  }

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
        && newState.pos == state.get().pos
        && newState.dir == state.get().dir
        && state.get().count >= 3) {
          state.update(PositionReportHistory(newState.xWay, newState.lane, newState.pos, newState.dir, state.get().count+1))
          (XWayLanePosDirKey(newState.xWay, newState.lane, newState.pos, newState.dir), newState.vid)
    } else {
      state.update(PositionReportHistory(newState.xWay, newState.lane, newState.pos, newState.dir, 1))
    }
    None
  }

}
