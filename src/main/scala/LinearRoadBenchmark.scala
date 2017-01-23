import model._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Queue

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
    val carsSpec = StateSpec.function(updateCarSpd _)
    val cars = positionReports
      .map(prep => (XwaySegmentDirectionVidMinKey(prep.xWay, prep.seg, prep.dir, prep.vid, (prep.time / 60) + 1), prep.speed))
      .mapWithState(carsSpec)
      .map(r => {
        val record = r.get
        (MinuteXWaySegDirKey(record._1.minute, record._1.xWay, record._1.seg, record._1.dir), (record._2._1, record._2._2))
      })
      .reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })

    val avgSpec = StateSpec.function(updateAvgs _)
    val avgs = cars.mapWithState(avgSpec)

    val LAVSpec = StateSpec.function(updateLAV _)
    val LAV = avgs
        .map(r => {
          val key = r.get._1
          val value = r.get._2
          val mappedMinute = key.minute - key.minute % 6
          (MinuteXWaySegDirKey(mappedMinute, key.xWay, key.seg, key.dir), value)
        })
        .reduceByKey((t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2)
        })
        //.map(r => (XwaySegmentDirectionKey(r.get._1.xWay, r.get._1.seg, r.get._1.dir), r.get._2._1))
        .mapWithState(LAVSpec)
        .foreachRDD(rdd => rdd.foreach(println))

//    val spec = StateSpec.function(segmentCrossing _)
//    val toNotify = positionReports
//      .map(ps => (ps.vid, PositionReport(ps.time, ps.vid, ps.speed, ps.xWay, ps.lane, ps.dir, ps.seg, ps.pos)))
//      .mapWithState(spec).filter(segCrossingEvent => segCrossingEvent.get.notification)
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

  case class  XwaySegmentDirectionVidMinKey(xWay:Int, seg:Int, dir:Byte, vid:Int, minute:Int)

  def updateLAV(key: MinuteXWaySegDirKey, value:Option[(Double, Int)], state: State[(Double, Int)]): Option[(XwaySegmentDirectionKey, Double)] = {

    val current = state.getOption().getOrElse(0.0, 0)
    val next = (current._1 + value.get._1, current._2 + value.get._2)
    val avg = value.get._1 / value.get._2
    state.update(next)
    Some((XwaySegmentDirectionKey(key.xWay, key.seg, key.dir), avg))

  }

  /**
    *
    * @param key
    * @param value
    * @param state (TotalSpeed, Cars)
    * @return
    */
  def updateAvgs(key: MinuteXWaySegDirKey, value: Option[(Double, Int)], state: State[(Double, Int)]): Option[(MinuteXWaySegDirKey, (Double, Int))] = {

    val previous = state.getOption.getOrElse((0.0, 0))
    val newState = value.get
    val next = (previous._1 + newState._1, previous._2 + newState._2)
    state.update(next)
    Some((key, next))

  }

  /**
    *
    * @param key
    * @param value
    * @param state (Speed, Count)
    * @return
    */
  def updateCarSpd(key: XwaySegmentDirectionVidMinKey, value: Option[Double], state: State[(Double, Int)]): Option[(XwaySegmentDirectionVidMinKey, (Double, Int))] = {

    //      println(s">>> key       = $key")
    //      println(s">>> value     = $value")
    //      println(s">>> state     = $state")

    val previous = state.getOption.getOrElse((0.0, 0))
    val next = (previous._1 + value.get, previous._2 + 1)

    state.update(next)
    Some((key, next))

  }

  def segmentCrossing(key: Int, value: Option[PositionReport], state: State[PositionReport]): Option[SegmentCrossingEvent] = {

    val existingEvent = state.getOption()

    if (existingEvent.isDefined && value.get.seg != existingEvent.get.seg && value.get.lane != 4) {
        state.update(value.get)
        Option(SegmentCrossingEvent(value.get.vid, value.get.time / 60 + 1, value.get.xWay, value.get.seg, value.get.dir, notification = true))
    } else {
      state.update(value.get)
      Option(SegmentCrossingEvent(value.get.vid, value.get.time / 60 + 1, value.get.xWay, value.get.seg, value.get.dir, notification = false))
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
