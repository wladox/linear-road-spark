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
    val ssc = new StreamingContext(sc, Seconds(1))
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

    // TOLL NOTIFICATION START
    val positionReports = events.filter(e => e._1 == 0).map(e => e._2).cache()

    val carAvgSpec = StateSpec.function(updateCarSpd _)
    val minAvgSpec = StateSpec.function(updateMinuteAvgs _)
    val LAVSpec = StateSpec.function(updateLAV _)
    val NOVSpec = StateSpec.function(updateNOV _)

    val carAvgs = positionReports
      //.filter(r => r.seg == 46 && r.dir == 0) // dont forget to remove !!!
      .map(prep => (XwaySegmentDirectionVidMinKey(prep.xWay, prep.seg, prep.dir, prep.vid, (prep.time / 60) + 1), prep.speed))
      .mapWithState(carAvgSpec) // key -> XwaySegmentDirectionVidMinKey, value -> (newAvgVel, oldAvgVel)
      /*.map(r => {
        (MinuteXWaySegDirKey(r._1.minute, r._1.xWay, r._1.seg, r._1.dir), (r._2._1, r._2._2))
      })*/ // velocities of all vehicles in a minute
      /*.reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }) // avg velocities of all vehicles in a minute , count of all vehicles in a minute
      .map(r => (r._1, r._2._1 / r._2._2)) // key -> MinuteXWaySegDirKey , value -> avg in that minute
      .foreachRDD(rdd => rdd.foreach(println))
      */

    val minuteAvgs = carAvgs
      .reduceByKey((c1, c2) => {
        (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
      })
      .mapWithState(minAvgSpec)

    val LAV = minuteAvgs
      .mapWithState(LAVSpec)

    val NOV = carAvgs
      .map(r => (r._1, r._2._3))
      .reduceByKey((t1, t2) => {
        t1 + t2
      })
      .mapWithState(NOVSpec)

    val segCrossSpec = StateSpec.function(segmentCrossing _)
    val segCrossings = positionReports
      .map(ps => {
        (ps.vid, PositionReport(ps.time, ps.vid, ps.speed, ps.xWay, ps.lane, ps.dir, ps.seg, ps.pos))
      })
      .mapWithState(segCrossSpec)
      .filter(r => r._2)

    val tolls = segCrossings
      .map(r => {
        (MinuteXWaySegDirKey(r._1.time / 60 + 1, r._1.xWay, r._1.seg, r._1.dir), r._1.vid)
      })
      .join(LAV)
      .join(NOV)
        .map(r => {
          val toll = calculateToll(r._2._1._2, r._2._2)
          TollNotification(r._2._1._1, r._1.minute, System.currentTimeMillis(), r._2._1._2, toll)
        })
      .foreachRDD(rdd => rdd.foreach(println))



        /*.map(r => {
          val key = r.get._1
          val value = r.get._2
          val mappedMinute = key.minute - key.minute % 6
          (MinuteXWaySegDirKey(mappedMinute, key.xWay, key.seg, key.dir), value)
        })
        .reduceByKey((t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2)
        })
        .map(r => (XwaySegDirKey(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute, r._2._1))) // value (Minute, Speed)*/


//
//    val carsAVGs  = TrafficAnalytics.vidAverageSpeedPerMin(positionReports)
//    val LAV       = TrafficAnalytics.speedAVG(carsAVGs)
//    val NOV       = TrafficAnalytics.numOfVehicles(positionReports)
//
//    val tollNotifications = TrafficAnalytics.tollNotification(LAV, NOV, toNotify)
//    tollNotifications.foreachRDD(rdd => rdd.saveAsTextFile("tollalerts"))
//
//    val stoppedCars = TrafficAnalytics.accidentDetection(positionReports)

    // ===========================================

    //case class AccountBalanceRequest(typ:Int, time: Int, vid:Int, qid:Int)

    // ACCOUNT BALANCES QUERIES
    /*val accountBalanceRequests = events
      .filter(e => e._1 == 2)
      .map(e => AccountBalanceRequest(2, e._2.time, e._2.vid, e._2.qid)) */

    //AccountBalanceAnalytics.accountBalance(accountBalanceRequests).print()


    // ===========================================

    // DAILY EXPENDITURES QUERIES
    /*val dailyExpenditureQueries = events
      .filter(e => e._1 == 3)*/


    // ===========================================

    ssc.start()
    ssc.awaitTermination()

  }

  def calculateToll(lav:Double, nov:Int):Double = {
    if (lav >= 40 || nov <= 50) 0.0
    else scala.math.pow(2*(nov - 50), 2)
  }

  case class  XwaySegmentDirectionVidMinKey(xWay:Int, seg:Int, dir:Byte, vid:Int, minute:Int) {
    override def toString: String = s"$xWay.$seg.$dir, VID $vid, Min $minute"
  }

  def updateNOV(key: MinuteXWaySegDirKey, value:Option[Int], state:State[Int]):(MinuteXWaySegDirKey, Int) = {

    val s = state.getOption().getOrElse(0)
    val newState = s+value.get
    state.update(newState)
    (key, newState)

  }

  def updateLAV(key: XwaySegDirKey, value:Option[(Int, Double)], state: State[LAVState]): (MinuteXWaySegDirKey, Double) = {

    val currentState = state.getOption().getOrElse(LAVState(mutable.MutableList(), 0, 0, 0, 0))
    val minute = value.get._1
    val spd = value.get._2

    // when first event
    if (currentState.count == 0) {

      val updatedState = LAVState(mutable.MutableList(spd), 1, spd, minute, minute)
      state.update(updatedState)

    } else if (currentState.count > 0 && currentState.count < 6) {

      if (currentState.start == minute) {
        // update sum, value in the sequence
        val newValues = mutable.MutableList(spd)
        val updatedState = LAVState(newValues, 1, spd, minute, minute)
        state.update(updatedState)

      } else if (currentState.end == minute) {

        val lastVal = currentState.values.last
        val newSum = currentState.values.sum - lastVal + spd
        val newValues = currentState.values.dropRight(1) += spd
        val updatedState = LAVState(newValues, currentState.count, newSum, currentState.start, currentState.end)
        state.update(updatedState)

      } else if (currentState.end < minute){
        val newSum = currentState.values.sum + spd
        val newValues = currentState.values += spd
        val updatedState = LAVState(newValues, currentState.count + 1, newSum, currentState.start, minute)
        state.update(updatedState)
      }
    } else if (currentState.count == 6) {

      if (currentState.end == minute) {
        // update sum, value in the sequence
        val lastVal = currentState.values.last
        val newSum = currentState.values.sum - lastVal + spd
        val newValues = currentState.values.dropRight(1) += spd
        val updatedState = LAVState(newValues, currentState.count, newSum, currentState.start, minute)
        state.update(updatedState)
      } else if (currentState.end < minute) {
        val oldestValue = currentState.values.head
        val newSum = currentState.values.sum + spd - oldestValue
        val newValues = currentState.values.tail += spd
        val updatedState = LAVState(newValues, currentState.count, newSum, currentState.start + 1, minute)
        state.update(updatedState)
      }

    }

    val avg = Math.round((state.get().values.sum - state.get().values.last) / (state.get().count-1)*100)/100.00
    //Some(LAVEvent(MinuteXWaySegDirKey(minute, key.xWay, key.segment, key.direction.toByte), state.get()))
    (MinuteXWaySegDirKey(minute, key.xWay, key.segment, key.direction.toByte), avg)
  }

  /**
    * This function returns average velocity per Xway, Segment, Direction, Minute
    *
    * @param key MinuteXWaySegDirKey
    * @param value (newValue, oldValue, countIncr)
    * @param state (TotalSpeed, Cars)
    * @return
    */
  def updateMinuteAvgs(key: MinuteXWaySegDirKey, value: Option[(Double, Double, Int)], state: State[(Double, Int)]): (XwaySegDirKey, (Int, Double)) = {

    val previous = state.getOption.getOrElse(0.0, 0)
    val newValue = value.get._1
    val oldValue = value.get._2
    val count = value.get._3
    val newState = (previous._1 - oldValue + newValue, count + previous._2)
    val avg = newState._1 / newState._2
    state.update(newState)
    (XwaySegDirKey(key.xWay, key.seg, key.dir), (key.minute, avg))

  }

  /**
    *
    * @param key XwaySegmentDirectionVidMinKey
    * @param value vehicle average velocity per minute
    * @param state Speed
    * @return Key-> MinuteXWaySegDirKey, Value ->(new avg velocity, velocity to subtract, counter)
    */
  def updateCarSpd(key: XwaySegmentDirectionVidMinKey, value: Option[Double], state: State[Double]): (MinuteXWaySegDirKey, (Double, Double, Int)) = {

    //      println(s">>> key       = $key")
    //      println(s">>> value     = $value")
    //      println(s">>> state     = $state")

    val previous = state.getOption.getOrElse(-1.0)

    if (previous == -1.0) {
      state.update(value.get)
      (MinuteXWaySegDirKey(key.minute, key.xWay, key.seg, key.dir), (value.get, 0.0, 1))
    } else {
      val avg = (previous + value.get) / 2.0
      state.update(avg)
      (MinuteXWaySegDirKey(key.minute, key.xWay, key.seg, key.dir), (avg, previous, 0))
    }

  }

  def segmentCrossing(key: Int, value: Option[PositionReport], state: State[PositionReport]): (PositionReport, Boolean) = {

    if (state.exists() && value.get.seg != state.get.seg) {
        state.update(value.get)
        (value.get, true)
    } else {
      state.update(value.get)
      (value.get, false)
    }

  }

  def tokenize(line:String):LREvent = {

    val recArray = line.split(",")
    LREvent(
      recArray(0).toInt,
      recArray(1).toInt,
      recArray(2).toInt,
      recArray(3).toDouble,
      recArray(4).toShort,
      recArray(5).toShort,
      recArray(6).toByte,
      recArray(7).toByte,
      recArray(8).toInt,
      recArray(9).toInt,
      recArray(14))

  }

}
