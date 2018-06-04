package com.github.wladox

import java.util

import com.github.wladox.component.{TrafficAnalytics, VehicleStatistics, XWaySegDirMinute}
import com.github.wladox.model.{Event, PositionReport, XwaySegDir}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StateSpec._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


case class XwayDir(xWay:Int, dir:Int) {
  def canEqual(a: Any): Boolean = a.isInstanceOf[XwayDir]

  override def equals(that: Any): Boolean =
    that match {
      case that: XwayDir => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + xWay
    result = prime * result + dir
    result
  }
}
case class TollNotification(vid:Int, time:Long, emit:Long, spd:Int, toll:Int) {
  override def toString: String = s"0,$vid,$time,$emit,$spd,$toll"
}
case class TollHistory(vid:Int, day: Int, xWay: Int, toll:Int)

/**
  * The entry point to run the benchmark.
  */
object LinearRoadBenchmark {

  val POSITION_REPORT           = 0
  val ACCOUNT_BALANCE_REQUEST   = 2
  val DAILY_EXPENDITURE_REQUEST = 3

  def functionToCreateContext(host: String, port: Int, checkpointDirectory: String): StreamingContext = {

    val conf = new SparkConf()
      //.setMaster("spark://"+host+":"+port) // use always "local[n]" locally where n is # of cores; host+":"+port otherwise
      .setMaster("local[*]")
      .setAppName("LinearRoadBenchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.executor.memory", "4g")
      //.set("spark.default.parallelism", "1")
      //.set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591
      //.set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.memory.fraction", "0.3")

    conf.registerKryoClasses(
      Array(classOf[Event], classOf[PositionReport], classOf[XwayDir], classOf[XWaySegDirMinute], classOf[XwaySegDir])
    )

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: LinearRoadBenchmark <hostname> <port> <checkpointDir> <inputTopic> <outputTopic> <tollHistory> <kafkaBroker>")
      System.exit(1)
    }

    val host          = args(0)
    val port          = args(1).toInt
    val checkpointDir = args(2)
    val inputTopic    = args(3)
    val outputTopic   = args(4)
    val history       = args(5)
    val broker        = args(6)
    val groupId       = "linear-road-app"

    // create streaming context
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => functionToCreateContext(host, port, checkpointDir))

    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // create direct stream
    val streams = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(inputTopic),
        kafkaParams)
      )

    // create Kafka producer props and broadcast them to workers
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producerParams = ssc.sparkContext.broadcast(props)

    val historicalTolls = ssc.sparkContext.textFile(history)
      .map(r => {
        val arr = r.split(",")
        ((arr(0).toInt, arr(2).toByte), arr(3).toByte)
      })
      .groupByKey()
      .mapValues(_.toArray)
      .collectAsMap()

    val historyMap = ssc.sparkContext.broadcast(historicalTolls)

    val events = streams
      .filter(v => v.value().trim().nonEmpty)
      .map(deserializeEvent)
      .cache()


    // ##################### DAILY EXPENDITURES RESPONSES ######################
    val daily = events
      .filter(_.typ == DAILY_EXPENDITURE_REQUEST)
      .map(e => {
        val tolls = historyMap.value((e.vid, e.xWay))
        val emit = math.round((System.currentTimeMillis() - e.internalTime)/1000f)
        s"3,${e.time},1,${e.qid},${tolls(e.day-1)}"
      })

    val positionReports = events
      .filter(e => e.typ == POSITION_REPORT)
      .map(toPositionReport)

    // ##################### ACCIDENT DETECTION ######################

    val accidents = positionReports
      .mapWithState(function(VehicleStatistics.updateLastReport _))
      .mapWithState(function(VehicleStatistics.updateStoppedVehicles _))
      .mapWithState(function(VehicleStatistics.updateAccidents _))
      //.foreachRDD(rdd => println("RDD size " + rdd.count()))


    // ##################### ACCIDENT NOTIFICATION ######################
    val accidentAlerts = accidents
      .filter(v => v._2._1.isCrossing && v._2._1.lane != 4 &&  v._2._2 != -1)
      .map(r => {
        val time = r._2._1.time
        val emit = math.round((System.currentTimeMillis() - r._2._1.internalTime)/1000f)
        val report = r._2._1
        s"1,$time,1,${report.xWay},${r._2._2},${report.direction},${report.vid}"
      })

    val keyedReports = positionReports
      .map(r => (XwaySegDir(r._2.xWay, r._2.segment, r._2.direction), (r._2.vid, r._2.speed, r._2.time)))
      .cache()

    // ##################### NUMBER OF VEHICLES ######################
    val NOV = keyedReports.mapWithState(function(TrafficAnalytics.updateNOV _))

    // ##################### AVERAGE VELOCITY ######################
    val LAV = keyedReports.mapWithState(function(TrafficAnalytics.lav _))

    // ##################### TOLL CALCULATION / NOTIFICATION ######################
    val segmentStats = accidents.join(LAV).join(NOV).map(r => (r._1,(r._2._1._1._2, r._2._1._2, r._2._2, r._2._1._1._1.isCrossing)))

    events
      .filter(e => e.typ == POSITION_REPORT || e.typ == ACCOUNT_BALANCE_REQUEST)
      .map(e => ((e.vid, e.time.toInt, e.xWay.toInt), e))
      .join(segmentStats)
      .map(r => {
        val report = r._2._1
        if (report.typ == POSITION_REPORT) {

          val isAccident  = r._2._2._1 != -1
          val lav         = r._2._2._2
          val nov         = r._2._2._3
          val isCrossing  = r._2._2._4
          val toll        = calculateToll(lav, nov, isAccident)

          val lane        = report.lane
          val emit        = math.round((System.currentTimeMillis() - report.internalTime)/1000f)

          if (report.vid == 16265 && report.time == 1150)
            System.out.print()

          ((report.vid, report.xWay), s"notify,${report.time},${report.internalTime},$lav,$toll,$lane,$isCrossing")
        } else {
          val qid = report.qid
          if (report.time == 1698 && qid.equals("5846"))
            System.out.println()
          ((report.vid, report.xWay), s"get,${report.time},${report.internalTime},$qid")
        }
      })
      .mapWithState(function(updateAccountBalance _))
      .filter(r => r.isDefined)
      .map(_.get)
      .union(accidentAlerts)
      .union(daily)
      //.checkpoint(Seconds(4))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val props = producerParams.value
          val producer = new KafkaProducer[String, String](props)
          partition.foreach( record => {
            val data = record
            val message = new ProducerRecord[String, String](outputTopic, null, data)
            producer.send(message)
          } )
          producer.close()
        })
      })


    ssc.start()
    ssc.awaitTermination()
  }

  def updateAccountBalance(key: (Int, Byte), value:Option[String], state:State[List[Int]]):Option[String] = {

    val arr = value.get.split(",")
    val tolls = state.getOption().getOrElse(List[Int]()) // previous toll, current balance, time
    val time        = arr(1).toInt
    val intTime     = arr(2).toLong
    val emit        = math.round((System.currentTimeMillis() - intTime)/1000f)

    val response = if (arr(0) == "notify") {

      val lav         = arr(3)
      val toll        = arr(4).toInt
      val lane        = arr(5).toByte
      val isCrossing  = arr(6).toBoolean

      //currentBalance(2) = time

      if (isCrossing && lane != 4) {
        //currentBalance(1) += currentBalance(0)
        //currentBalance(0) = toll
        val newTolls = tolls ++ List(toll)
        state.update(newTolls)
        Some(s"0,${key._1},$time,1,$lav,$toll")
      } else {
        None
      }

    } else {

      val qid         = arr(3)
      //val resultTime  = currentBalance(2)
      val bal         = tolls.sum

      if (qid.equals("5846"))
        System.out.println()

      Some(s"2,$time,1,$time,$qid,$bal")

    }

    response
  }

  /*def updateNumberOfPositionReports(key: XwayDir, value:Option[(PositionReport, Boolean)], state:State[Map[Int, Array[(Int,Int,Int)]]]):(XwayDir, (Int,Int,Int,Int,Int,Int)) = {

    val minute = value.get._1.time/60
    val segment = value.get._1.segment
    val vid = value.get._1.vid
    val segmentCrossing = value.get._2
    val speed = value.get._1.speed

    val map = state.getOption().getOrElse(Map())

    val newMap = if (map.contains(minute)) {
      val currentStat = map(minute)(segment) //(NOV, TotalCount, TotalSpeed)
      val newStat = if (segmentCrossing) {
        (currentStat._1+1, currentStat._2+1, currentStat._3+speed)
      } else {
        (currentStat._1, currentStat._2+1, currentStat._3+speed)
      }

      map(minute)(segment) = newStat
      map

    } else {
      val arr = Array.fill(100)((0,0,0))
      arr(segment) = (1, 1, speed)
      map + (minute -> arr)
    }

    state.update(newMap)

    val output = newMap(minute)(segment)

    (key, (vid, segment, minute, output._1, output._2, output._3))
  }*/

  /*def updateStoppedVehicles(key: XwaySegDir, value:Option[(PositionReport, Boolean, Boolean)],
                            state:State[Map[Int, Set[PositionReport]]]):(XWaySegDirMinute, (Int, Boolean)) = {


    val positionReport = value.get._1
    val isStopped = value.get._2
    val minute = positionReport.time/60

    val s = state.getOption().getOrElse(Map())

    val newMap = if (isStopped) {
      val newSet = s.getOrElse(minute, Set()) + positionReport
      s ++ Map[Int, Set[PositionReport]](minute -> newSet)
    } else s.mapValues(r => r.filter(!_.equals(positionReport)))

    state.update(newMap)

    val currentMinute = positionReport.time/60
    val previousMinute = positionReport.time/60-1

    val isAccident = if (isStopped)
        newMap(currentMinute).size >= 2 || newMap.getOrElse(previousMinute, Set()).size >= 2
      else {
        val accidentInRange = key.direction match {
          case 0 =>
            (0 to 4).map(i => newMap.get((positionReport.segment.toInt + i, previousMinute)).isDefined
              && newMap((positionReport.segment.toInt + i, previousMinute)).size >= 2)
          case 1 =>
            (0 to 4).map(i => newMap.get((positionReport.segment.toInt - i, previousMinute)).isDefined
              && newMap((positionReport.segment.toInt - i, previousMinute)).size >= 2)
        }
        accidentInRange.contains(true)
      }

    (XWaySegDirMinute(key.xWay, key.segment, key.direction, minute), (positionReport.vid, isAccident))
  }*/

  /**
    * Updates the number of vehicles per minute on an expressway, segment and direction
    *
    * @param key XWaySegDirMinute
    * @param value Vehicles count in the current batch
    * @param state Vehicles count state
    * @return
    */
  def updateNOV(key: XWaySegDirMinute, value:Option[Int], state:State[Int]):(XWaySegDirMinute, Int) = {

    val s = state.getOption().getOrElse(0)
    val newState = s+value.get
    state.update(newState)
    (key, newState)

  }



  /**
    * Counts the amount of position reports of a vehicle from the same position. If the count is greater or equal than 4,
    * then it indicates that the vehicle has stopped.
    *
    * @param key Vehicle ID
    * @param value Position report of the vehicle
    * @param state Tuple consisting of the last position report and a counter of equal position reports
    * @return VehicleStop Tuple indicating a vehicle being stopped at a particular position.
    */
  def stopDetection(key: Int, value:Option[PositionReport], state:State[(Int, Int, Int, Int, Int)]):(PositionReport, Boolean) = {

    val currRep = value.get
    if (state.exists()) {
      val xWay = state.get()._1        // get last position report
      val dir = state.get()._2
      val lane = state.get()._3
      val pos = state.get()._4
      val counter = state.get()._5        // get count of equal position reports

      if (xWay == currRep.xWay && dir == currRep.direction && lane == currRep.lane && pos == currRep.position)
        state.update((currRep.xWay, currRep.direction, currRep.lane, currRep.position, counter + 1))
    } else {
      state.update((currRep.xWay, currRep.direction, currRep.lane, currRep.position, 1))
    }

    val counter = state.get()._2
    if (counter >= 4)
      (currRep, true) // vehicle stopped
    else
      (currRep, false) // vehicle is moving

  }


  /**
    * Converts a raw string into an Event object.
    *
    * @param record ConsumerRecord from Kafka
    * @return @Event
    */
  def deserializeEvent(record: ConsumerRecord[String, String]): Event = {
    val array = record.value().trim().split(",")
      Event(
        array(0).toShort,
        array(1).toShort,
        array(2).toInt,
        array(3).toInt,
        array(4).toByte,
        array(5).toByte,
        array(6).toByte,
        array(7).toByte,
        array(8).toInt,
        array(9),
        array(14).toByte,
        record.timestamp()
      )
  }

  /**
    * Converts an Event object into PositionReport object
    * @param e @see Event
    * @return Tuple (vehicleID, PositionReport)
    */
  def toPositionReport(e:Event):((Int,Int), PositionReport) = {
    ((e.xWay, e.vid), PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime, isStopped = false, isCrossing = false, -1, -1))
  }


  // UTILITY FUNCTIONS =============================
  def isOnTravelLane(lane:Int):Boolean = {
    lane > 0 && lane < 4
  }

  def calculateToll(lav:Double, nov:Int, isAccident:Boolean):Int = {
    if (lav >= 40 || nov <= 50 || isAccident) 0
    else 2 * scala.math.pow(nov - 50, 2).toInt
  }

}