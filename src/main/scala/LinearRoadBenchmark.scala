import model._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object LinearRoadBenchmark {

  def main(args: Array[String]): Unit = {

    /*if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }*/

    val conf = new SparkConf()
      .setMaster("local[2]") // use always "local[n]" locally
      .setAppName("LinearRoadBenchmark")

    // initialize spark context
    val sc = new SparkContext(conf)

    // initialize spark streaming context
    val ssc = new StreamingContext(sc, Seconds(5))

    // provide checkpointing directory
    ssc.checkpoint("checkpoint")

    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // define a map of interesting topics
    val topics = Array("linear-road")

    // read elements from Kafka
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val events = stream.map(record => tokenize(record.value())).map(event => (event.eventType, event))

    //val events = ssc.socketTextStream("localhost",1234)

    // POSITION REPORTS
    //val positionReports = events.map(tokenize).map(event => (event.eventType, event)).filter(e => e._1 == 0).map(e => e._2).cache()

    val positionReports = events.filter(e => e._1 == 0).map(e => e._2).transform((rdd, time) => {
      rdd.map(event => LREvent(event.eventType, event.time, event.vid, event.speed, event.xWay, event.lane, event.dir, event.seg, event.pos, event.qid, event.day, time.milliseconds))
    })
    .cache()

    // ACCOUNT BALANCE REQUESTS =====================================
    val accBalState = StateSpec.function(getAccountBalance _)
    val accountBalanceRequests = events.filter(e => e._1 == 2).transform((rdd, time) => {
      rdd.map(e => (e._2.vid, AccountBalanceRequest(2, e._2.time, e._2.vid, e._2.qid, time.milliseconds)))
    }).mapWithState(accBalState)


    // DAILY EXPENDITURE REQUESTS =====================================
    val dailyExpenditureState = StateSpec.function(getDailyExpenditure _)
    val dailyExpenditures = events.filter(e => e._1 == 3).transform((rdd, time) => {
      rdd.map(e => (e._2.vid, DailyExpenditureRequest(3, e._2.time, e._2.vid, e._2.qid, e._2.xWay, e._2.day, time.milliseconds))
    }).mapWithState(dailyExpenditureState)

    // SEGMENT CROSSINGS =====================================
    val segCrossSpec = StateSpec.function(segmentCrossing _)
    val segCrossingVehicles = positionReports
      .map(keyByVehicle)
      .mapWithState(segCrossSpec)
      .filter(r => r._2)

    // ACCIDENT DETECTION =====================================
    val stopPredicate = StateSpec.function(stopDetection _)
    val accPredicate  = StateSpec.function(accidentDetection _)

    val accidents = positionReports
      .filter(r => isOnTravelLane(r.lane))
      .map(keyByVehicle)
      .mapWithState(stopPredicate)
      .map(r => (XWayLaneDir(r.get._1.xWay, r.get._1.lane, r.get._1.dir), (r.get._1.pos, r.get._2, r.get._1.time, r.get._1.vid)))
      .mapWithState(accPredicate)

    // TOLL NOTIFICATION =====================================
    val carAvgSpec = StateSpec.function(updateCarSpd _)
    val minAvgSpec = StateSpec.function(updateMinuteAvgs _)
    val LAVSpec = StateSpec.function(updateLAV _)
    val tollAssessment = StateSpec.function(updateAccountBalance _)
    val NOVSpec = StateSpec.function(updateNOV _)

    val carAvgs = positionReports
      .map(r => (XwaySegmentDirectionVidMinKey(r.xWay, r.seg, r.dir, r.vid, (r.time / 60) + 1), (r.speed, r.time, r.internalTime)))
      .mapWithState(carAvgSpec) // key -> XwaySegmentDirectionVidMinKey, value -> (newAvgVel, oldAvgVel)
      .map(r => {
        (MinuteXWaySegDirKey(r._1.min, r._1.xWay, r._1.seg, r._1.dir), (r._1.spd, r._2, r._1.vid, r._1.time, r._1.internalTime))
      })
      .mapWithState(minAvgSpec)// velocities of all vehicles in a minute
      .map(r => {
      (XwaySegDirKey(r._2.xWay, r._2.seg, r._2.dir), (r._2.min, r._2.spd, r._2.count, r._1, r._2.time, r._2.internalTime)) // (min, speed, count, vid, time)
      })
      .mapWithState(LAVSpec)
      .join(accidents)
      .mapWithState(tollAssessment)
      .foreachRDD(rdd => rdd.saveAsTextFile("output/tollalerts"))

    ssc.start()
    ssc.awaitTermination()

  }

  case class XwaySegDirLanePos(xWay:Int, seg:Byte, dir:Byte, lane:Byte, pos:Int)
  case class XwayDirPosMin(xWay:Int,dir:Byte, pos:Int, min:Int)
  case class VehicleStop(vid:Int, time:Int, xWay:Int, lane:Byte, pos:Int, dir:Byte)
  case class AccidentInSegment(min:Int, xWay:Int, seg:Int, dir:Byte) {
    override def toString: String = s"$min minute: Accident occured on xWay: $xWay segment $seg direction $dir"
  }
  case class XWayLaneDir(xWay:Int, lane:Byte, dir:Byte)
  case class AccidentNotification(typ:Int, time:Int, emit:Long, seg:Int) {
    override def toString: String = "ACCIDENT NOTIFICATIOn"
  }
  case class XwayDir(xWay:Int, dir:Byte)
  case class Stop(vid:Int, time:Int, xWay:Int, lane:Byte, pos:Int, dir:Byte)
  case class Accident(time:Int, xWay: Int, pos:Int, dir:Byte, stop1:Option[Stop], stop2:Option[Stop])
  case class CarAvgSpeed(vid:Int, min:Int, xWay:Int, seg:Byte, dir:Byte, spd:Double, time:Int, internalTime:Long)
  case class MinAvgSpeed(min:Int, xWay:Int, seg:Byte, dir:Byte, spd:Double, count:Int, time:Int, internalTime:Long) {
    override def toString: String = s"AVG speed in $min minute, xWay: $xWay segment $seg direction $dir = $spd of $count vehicles"
  }
  case class TollNotification(typ:Byte, vid:Int, time:Int, emit:Long, spd:Double, toll:Double, internalTime:Long, response:Long) {
    override def toString: String = s"TOLL NOTIFICATION for VID: $vid, time: $time, speed: $spd, toll: $toll, RESPONSE TIME: $response"
  }
  case class AccountBalanceRequest(typ:Int, time:Int, vid:Int, qid:Int, internalTime:Long)
  case class AccountBalanceReport(typ:Int, time:Int, emit:Long, qid:Int, bal:Double, resultTime:Long, responseTime:Long)

  case class DailyExpenditureRequest(typ:Int, time:Int, vid:Int, qid:Int, xWay:Int, day:Int, internalTime:Long)
  case class DailyExpenditureReport(typ:Int, time:Int, emit:Long, qid:Int, bal:Double, reponseTime:Long)
  case class VidXwayDay(vid:Int, xWay:Int, day:Int)

  case class  XwaySegmentDirectionVidMinKey(xWay:Int, seg:Byte, dir:Byte, vid:Int, minute:Int) {
    override def toString: String = s"$xWay.$seg.$dir, VID $vid, Min $minute"
  }

  def updateNOV(key: MinuteXWaySegDirKey, value:Option[Int], state:State[Int]):(MinuteXWaySegDirKey, Int) = {

    val s = state.getOption().getOrElse(0)
    val newState = s+value.get
    state.update(newState)
    (key, newState)

  }

  /**
    *
    * @param key (Min, Speed, Count, VID, time)
    * @param value
    * @param state
    * @return
    */
  def updateLAV(key: XwaySegDirKey, value:Option[(Int, Double, Int, Int, Int, Long)], state: State[mutable.Map[Int, (Double, Int)]]): (Int, TollNotification) = {

    val min = value.get._1
    val spd = value.get._2
    val vid = value.get._4
    val time = value.get._5
    val internalTime = value.get._6

    if (state.exists()) {
      val map = state.get()
      val count = value.get._3
      map.update(min, (spd, count))
      state.update(map)
    } else {
      val count = value.get._3
      val map = mutable.Map[Int, (Double, Int)](min -> (spd, count))
      state.update(map)
    }

    val m1 = min - 1
    val m2 = min - 2
    val m3 = min - 3
    val m4 = min - 4
    val m5 = min - 5

    val sum = state.get().getOrElse(m1, (0.0, 0))._1
    + state.get().getOrElse(m2, (0.0, 0))._1
    + state.get().getOrElse(m3, (0.0, 0))._1
    + state.get().getOrElse(m4, (0.0, 0))._1
    + state.get().getOrElse(m5, (0.0, 0))._1

    val count = min match {
      case x if x > 5 => 5
      case 5 => 4
      case 4 => 3
      case 3 => 2
      case 2 => 1
      case 1 => 1
    }

    val LAV = BigDecimal(sum / count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val NOV = state.get().getOrElse(m1, (0.0, 0))._2

    val currentTime = System.currentTimeMillis()
    if (LAV >= 40 || NOV <= 50) {
      (vid, TollNotification(0, vid, time, currentTime, LAV, 0, internalTime, currentTime - internalTime))
    }
    else {
      val toll = 2 * math.pow(NOV - 50, 2)
      val rounded = BigDecimal(toll).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (vid, TollNotification(0, vid, time, currentTime, LAV, rounded, internalTime, currentTime - internalTime))
    }

  }

  /**
    * This function returns average velocity per Xway, Segment, Direction, Minute
    *
    * @param key MinuteXWaySegDirKey
    * @param value (Spd, Delta, VID, Time)
    * @param state (TotalSpeed, Cars)
    * @return
    */
  def updateMinuteAvgs(key: MinuteXWaySegDirKey, value: Option[(Double, Double, Int, Int, Long)], state: State[(Double, Int)]): (Int, MinAvgSpeed) = {

    val spd = value.get._1
    val delta = value.get._2
    val time = value.get._4
    val internalTime = value.get._5

    if (state.exists()) {
      if (delta == -1.0) {
        val newAVGSpd = state.get()._1 + spd
        val count = state.get()._2 + 1
        state.update((newAVGSpd, count))
      } else {
        val newAVGSpd = state.get()._1 + spd - delta
        val count = state.get()._2
        state.update((newAVGSpd, count))
      }
    } else {
      state.update((spd, 1))
    }
    val avg = state.get()._1 / state.get()._2
    (value.get._3, MinAvgSpeed(key.minute, key.xWay, key.seg, key.dir, avg, state.get()._2, time, internalTime))

  }

  /**
    *
    * @param key XwaySegmentDirectionVidMinKey
    * @param value vehicle average velocity per minute, internal time
    * @param state Speed
    * @return Key-> MinuteXWaySegDirKey, Value ->(new avg velocity, velocity to subtract, counter)
    */
  def updateCarSpd(key: XwaySegmentDirectionVidMinKey, value: Option[(Double, Int, Long)], state: State[Double]): (CarAvgSpeed, Double) = {

    val time = value.get._2
    val currentSpd = value.get._1
    val internalTime = value.get._3
    if (state.exists()) {
      val spd = state.get()
      state.update((spd + currentSpd) / 2.0)
      (CarAvgSpeed(key.vid, key.minute, key.xWay, key.seg, key.dir, state.get(), time, internalTime), spd)
    } else {
      state.update(currentSpd)
      (CarAvgSpeed(key.vid, key.minute, key.xWay, key.seg, key.dir, state.get(), time, internalTime), -1.0)
    }

  }


  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def segmentCrossing(key: Int, value: Option[PositionReport], state: State[PositionReport]): (PositionReport, Boolean) = {

    if (state.exists() && value.get.seg != state.get.seg) {
        state.update(value.get)
        (value.get, true)
    } else {
      state.update(value.get)
      (value.get, false)
    }

  }

  /**
    *
    * @param key Vehicle ID
    * @param value Position report of the vehicle
    * @param state Tuple consisting of the last position report and a counter of equal position reports
    * @return VehicleStop Tuple indicating a vehicle being stopped at a particular position.
    */
  def stopDetection(key: Int, value:Option[PositionReport], state:State[(PositionReport, Int)]):Option[(PositionReport, Option[Stop])] = {

    val currRep = value.get

    if (state.exists()) {
      val prevRep = state.get()._1        // get last position report
      val counter = state.get()._2        // get count of equal position reports
      if (prevRep.xWay == currRep.xWay && prevRep.dir == currRep.dir && prevRep.lane == currRep.lane && prevRep.pos == currRep.pos) {
        state.update((currRep, counter + 1))
      } else {
        state.update((currRep, 1))
      }
    } else {
      state.update((currRep, 1))
    }

    val counter = state.get()._2
    if (counter >= 4)
      Some(currRep, Some(Stop(currRep.vid, currRep.time, currRep.xWay, currRep.lane, currRep.pos, currRep.dir))) // vehicle stopped
    else
      Some(currRep, None) // vehicle is moving

  }

  /**
    *
    * @param key XWay Lane Direction
    * @param value (Position, Stop, Time, VID)
    * @param state (Map[Minute -> Accident])
    * @return
    */
  def accidentDetection(key: XWayLaneDir, value:Option[(Int, Option[Stop], Int, Int)], state:State[mutable.Map[Int, Accident]]):(Int, Option[AccidentNotification]) = {

    val pos = value.get._1
    val segment = pos / 5280
    val time = value.get._3
    if (value.get._2.isDefined) { // stopped vehicle
      val stop = value.get._2.get
      if (state.exists()) {
        val accidents = state.getOption().getOrElse(mutable.Map[Int, Accident]())
        val accident = accidents.get(segment)

        if (accident.isDefined && accident.get.stop2.isEmpty) {
          val acc = Accident(stop.time, stop.xWay, stop.pos, stop.dir, accident.get.stop1, Some(stop))
          state.update(accidents + (segment -> acc))
        } else {
          val acc = Accident(stop.time, stop.xWay, stop.pos, stop.dir, Some(stop), None)
          state.update(accidents + (segment -> acc))
        }
      } else {
        val firstState = mutable.Map[Int, Accident](segment -> Accident(stop.time, stop.xWay, stop.pos, stop.dir, Some(stop), None))
        state.update(firstState)
      }

    } else { // moving vehicle
      if (state.exists()) {
        val accidents = state.getOption().getOrElse(mutable.Map[Int, Accident]())
        val notificationRange = (segment to segment+4).toSet
        if (accidents.keys.toSet.intersect(notificationRange).nonEmpty) {  // check if it is in notification range
          for (acc <- accidents.values) {
            if ((acc.time / 60 + 1) < (time / 60 + 1)) { // check if time is the next minute after accident occured
              // notify
              return (value.get._4, Some(AccidentNotification(1, time, System.currentTimeMillis(), acc.pos / 5280)))
            }
          }
        }
      }
    }

    (value.get._4, None)

  }


  def updateAccountBalance(key: Int, value:Option[(TollNotification, Option[AccidentNotification])], state:State[Double]):TollNotification = {

    val balance = state.getOption().getOrElse(0.0)
    val toll = value.get._1
    if (value.get._2.isDefined) {
      state.update(balance)
      TollNotification(0, toll.vid, toll.time, toll.emit, toll.spd, 0, toll.internalTime, toll.emit - toll.internalTime)
    } else {
      state.update(balance + toll.toll)
      toll
    }

  }

  /**
    *
    * @param key VID
    * @param value (time, internal time, QID)
    * @param state Account balance
    * @return AccountBalanceReport
    */
  def getAccountBalance(key:Int, value:Option[AccountBalanceRequest], state:State[Double]):AccountBalanceReport = {
    val balance = state.getOption().getOrElse(0.0)
    val request = value.get
    val emit = System.currentTimeMillis()
    AccountBalanceReport(2, request.time, emit, request.qid, balance, -1, emit - request.internalTime)
  }

  /**
    *
    * @param key VehicleID, xWay, Day
    * @param value (time, QID, internal time)
    * @param state Daily Expenditure
    * @return DailyExpenditureReport
    */
  def getDailyExpenditure(key:VidXwayDay, value:Option[(Int, Int, Long)], state:State[Double]):DailyExpenditureReport = {

    val time = value.get._1
    val qid = value.get._2
    val internalTime = value.get._3
    val emit = System.currentTimeMillis()

    DailyExpenditureReport(3, time, emit, qid, state.get(), emit - internalTime)
  }
  /**
    * This method converts a raw string into an LREvent object
    *
    * @param line String
    * @return LREvent
    */
  def tokenize(line:String):LREvent = {

    val recArray = line.split(",")
    LREvent(
      recArray(0).toInt,
      recArray(1).toInt,
      recArray(2).toInt,
      recArray(3).toDouble,
      recArray(4).toShort,
      recArray(5).toByte,
      recArray(6).toByte,
      recArray(7).toByte,
      recArray(8).toInt,
      recArray(9).toInt,
      recArray(14).toInt,
      -1)

  }

  def isOnTravelLane(lane:Int):Boolean = {
    lane > 0 && lane < 4
  }

  def calculateToll(lav:Double, nov:Int):Double = {
    if (lav >= 40 || nov <= 50) 0.0
    else scala.math.pow(2*(nov - 50), 2)
  }

  def keyByVehicle(event:LREvent):(Int, PositionReport) = {
    (event.vid, PositionReport(event.time, event.vid, event.speed, event.xWay, event.lane, event.dir, event.seg, event.pos))
  }
}
