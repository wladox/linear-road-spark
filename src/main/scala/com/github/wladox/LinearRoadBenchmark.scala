package com.github.wladox

import java.util

import com.github.wladox.component.{TrafficAnalytics, VehicleInformation, VehicleStatistics, XWaySegDirMinute}
import com.github.wladox.model.{Event, PositionReport, XwaySegDir}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is the entry point for the benchmark application.
  */
object LinearRoadBenchmark {

  case class Accident(time:Int, xWay:Int, lane:Int, dir:Int, pos:Int) {
    override def toString: String = s"[Accident: $xWay, $lane, $dir, $pos]"
  }
  case class TollNotification(vid:Int, time:Long, emit:Long, spd:Int, toll:Int) {
    override def toString: String = s"0,$vid,$time,$emit,$spd,$toll"
  }
  case class DailyExpenditureReport(time:Long, emit:Long, qid:Int, bal:Int, day:Short) {
    override def toString: String = s"3,$time,$emit,$qid,$bal"
  }
  case class TollHistory(vid:Int, day: Int, xWay: Int, toll:Int)
  case class XwayDir(xWay:Byte, dir:Byte)
  case class VehicleDayXway(vid:Int, day:Short, xWay:Short)

  def functionToCreateContext(host: String, port: Int, checkpointDirectory: String): StreamingContext = {

    val conf = new SparkConf()
      //.setMaster("spark://"+host+":"+port) // use always "local[n]" locally where n is # of cores; host+":"+port otherwise
      .setMaster("local[*]")
      .setAppName("Linear Road Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "4g")
      //.set("spark.default.parallelism", "1")
      //.set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591
      //.set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.memory.fraction", "0.3")

    conf.registerKryoClasses(
      Array(classOf[Event], classOf[PositionReport], classOf[VehicleInformation], classOf[XwayDir], classOf[XWaySegDirMinute], classOf[XwaySegDir])
    )

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      System.err.println("Usage: LinearRoadBenchmark <hostname> <port> <checkpointDir> <topic> <tollHistory> <kafka.broker>")
      System.exit(1)
    }

    val host          = args(0)
    val port          = args(1).toInt
    val checkpointDir = args(2)
    val topic         = args(3)
    val history       = args(4)
    val broker        = args(5)

    val ssc = StreamingContext.getOrCreate(checkpointDir, () => functionToCreateContext(host, port, checkpointDir))

    val groupId = "linear-road-app-"+System.currentTimeMillis().toString

    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streams = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic),
        kafkaParams)
      )

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
      .filter(v => v.value().trim().nonEmpty && !v.value().equals("0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
      .map(deserializeEvent)
      .cache()


    // ##################### DAILY EXPENDITURES ######################
    val daily = events
      .filter(_.typ == 3)
      .map(e => {
        val tolls = historyMap.value((e.vid, e.xWay))
        val emit = math.round((System.currentTimeMillis() - e.internalTime)/1000f)
        s"3,${e.time},$emit,${e.qid},${tolls(e.day-1)}"
      })

    val positionReports = events
      .filter(_.typ == 0)
      .map(toPositionReport)
      .cache()

    val vehicleStats = positionReports
      .mapWithState(StateSpec.function(VehicleStatistics.updateLastReport _))
      .persist(StorageLevel.MEMORY_ONLY)

        /*.foreachRDD(r => {
          println("*** got an RDD, size = " + r.count())

          r.partitioner match {
            case Some(p) => println("+++ partitioner: " + r.partitioner.get.getClass.getName)
            case None => println("+++ no partitioner")
          }

          if (r.count() > 0) {
            println("*** " + r.getNumPartitions + " partitions")
          }
        })*/

    // ##################### ACCIDENT DETECTION LOGIC ######################

    val accidents       = vehicleStats
      .map(t => (XwayDir(t._2.report.xWay, t._2.report.direction), t._2))
      .mapWithState(StateSpec.function(VehicleStatistics.updateStoppedVehicles _))
      .mapWithState(StateSpec.function(VehicleStatistics.updateAccidents _))
      // TODO improve processing time


    val accidentAlerts = accidents
      .filter(v => v._2._1.isCrossing && v._2._1.report.lane != 4 &&  v._2._2 != -1)
      .map(r => {
        val time = r._2._1.report.time
        val emit = math.round((System.currentTimeMillis() - r._2._1.report.internalTime)/1000f)
        s"1,$time,$emit,${r._1.xWay},${r._2._2},${r._1.dir},${r._2._1.report.vid}"
      })


    // ##################### NUMBER OF VEHICLES LOGIC ######################

    val NOV = vehicleStats
      .map(r => (XWaySegDirMinute(r._2.report.xWay, r._2.report.segment, r._2.report.direction, (r._2.report.time/60+1).toShort), if (r._2.isCrossing) 1 else 0))
      .reduceByKey((r1, r2) => r1 + r2)
      .map(r => {
        (XwaySegDir(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute, r._2))
      })
      .mapWithState(StateSpec.function(TrafficAnalytics.updateNOV2 _))

    // ##################### AVERAGE VELOCITY LOGIC ######################

    val LAV = positionReports
      .map(r => (XWaySegDirMinute(r._2.xWay, r._2.segment, r._2.direction, (r._2.time/60+1).toShort), (1, r._2.speed)))
      .reduceByKey((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2))
      .mapWithState(StateSpec.function(TrafficAnalytics.spdSumPerMinute _).timeout(Seconds(120)))
      .filter(_.isDefined)
      .map(_.get)// TODO improve processing time
      .mapWithState(StateSpec.function(TrafficAnalytics.lav _))


    val tolls = accidents.join(LAV).join(NOV)
      .map(r => {

        val info = r._2._1._1._1
        val isAccident = r._2._1._1._2 != -1

        val lav = r._2._1._2
        val nov = r._2._2

        val toll = calculateToll(lav, nov, isAccident)
        val lane = info.report.lane
        val isCrossing = info.isCrossing
        val emit = math.round((System.currentTimeMillis() - info.report.internalTime)/1000f)

        //val action = if (info.report.lane != 4) "update" else "reset"

        ((info.report.vid, info.report.xWay), s"notify,${info.report.time},$emit,$lav,$toll,$lane,$isCrossing")
        //(info.report.vid, r._2._1._1.report.time, -1, lav, toll)
      })

    // ##################### ACCOUNT BALANCE REQUESTS ######################
    val accRequests = events
      .filter(_.typ == 2)
      .map(e => ((e.vid, e.xWay), s"get,${e.time},${e.internalTime},${e.qid}"))

    tolls
      .union(accRequests)
      .mapWithState(StateSpec.function(processToll _))
      .filter(r => r.isDefined)
      .map(_.get)
      .union(accidentAlerts)
      .union(daily)
      .checkpoint(Seconds(4))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val props = producerParams.value
          val producer = new KafkaProducer[String,String](props)
          partition.foreach( record => {
            val data = record
            val message = new ProducerRecord[String, String]("output-2000k", null, data)
            producer.send(message)
          } )
          producer.close()
        })
      })


    ssc.start()
    ssc.awaitTermination()
  }

  def processToll(key: (Int, Byte), value:Option[String], state:State[Array[Int]]):Option[String] = {

    val arr = value.get.split(",")

    val currentBalance = state.getOption().getOrElse(Array[Int](0,0,0)) // previous toll, current balance, time

    val res = if (arr(0) == "notify") {

      val time        = arr(1).toInt
      val procTime    = arr(2)
      val lav         = arr(3)
      val toll        = arr(4).toInt
      val lane        = arr(5).toByte
      val isCrossing  = arr(6).toBoolean

      currentBalance(2) = time

      if (isCrossing && lane != 4) {
        currentBalance(1) += currentBalance(0)
        currentBalance(0) = toll
      }

      state.update(currentBalance)

      if (isCrossing && lane != 4) {
        Some(s"0,${key._1},$time,$procTime,$lav,$toll")
      } else {
        None
      }

    } else {
    //} else if(arr(0).equals("get")) {

      val time = arr(1).toShort
      val intTime = arr(2).toLong
      val qid = arr(3)
      val emit = math.round((System.currentTimeMillis() - intTime)/1000f)

      val resultTime = currentBalance(2)
      val bal = currentBalance(1)

      Some(s"2,$time,$emit,$resultTime,$qid,$bal")

    } /*else {
      // reset
      currentBalance(0) = 0
      state.update(currentBalance)

      None
    }*/

    res
  }

  @deprecated
  def getDailyExpenditure(key:(Int, Short), value:Option[(Int,Byte)], state:State[Array[Byte]]):(Int,Int) = {

    val qid = value.get._1
    val day = value.get._2

    val exp = state.getOption() match {
      case Some(s) => s(day-1)
      case None => -1
    }
    (qid, exp)
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
  def toPositionReport(e:Event):(Int, PositionReport) = {
    (e.vid, PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime))
  }


  // UTILITY FUNCTIONS =============================
  def isOnTravelLane(lane:Int):Boolean = {
    lane > 0 && lane < 4
  }

  def calculateToll(lav:Double, nov:Int, isAccident:Boolean):Int = {
    if (lav >= 40 || nov <= 50 || isAccident) 0
    else 2 * scala.math.pow(nov - 50, 2).toInt
  }

  def keyByVehicle(event: Event):(Int, PositionReport) = {
    (event.vid, PositionReport(event.time, event.vid, event.speed, event.xWay, event.lane, event.direction, event.segment, event.position, event.internalTime))
  }

}