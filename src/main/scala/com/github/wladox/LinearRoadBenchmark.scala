package com.github.wladox

import com.github.wladox.component.TrafficAnalytics.XWaySegDirMinute
import com.github.wladox.component.VehicleStatistics.VehicleInformation
import com.github.wladox.component.{TrafficAnalytics, VehicleStatistics}
import com.github.wladox.model.{Event, PositionReport}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is the entry point for the benchmark application.
  */
object LinearRoadBenchmark {

  case class Accident(time:Int, xWay:Int, lane:Int, dir:Int, pos:Int) {
    override def toString: String = s"[Accident: $xWay, $lane, $dir, $pos]"
  }

  case class XwayDir(xWay:Int, dir:Int)

  def functionToCreateContext(host: String, port: Int, outputPath: String, checkpointDirectory: String): StreamingContext = {

    val conf = new SparkConf()
      //.setMaster("spark://"+host+":"+port) // use always "local[n]" locally where n is # of cores; host+":"+port otherwise
      .setAppName("Linear Road Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591
    //.set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.executor.memory", "2g")
      //.set("spark.memory.fraction", "0.3")


    conf.registerKryoClasses(
      Array(
        classOf[Event],
        classOf[XWaySegDirMinute],
        classOf[PositionReport],
        classOf[TollNotification],
        classOf[DailyExpenditureReport],
        classOf[VehicleInformation]
      )
    )

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(2000))

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: LinearRoadBenchmark <hostname> <port> <output> <checkpointDir>")
      System.exit(1)
    }

    val host          = args(0)
    val port          = args(1).toInt
    val output        = args(2)
    val checkpointDir = args(3)

    val ssc = StreamingContext.getOrCreate(checkpointDir, () => functionToCreateContext(host, port, output, checkpointDir))

    // Spark SQL for Historical Data
    //    val spark   = SparkSession.builder.config(context.sparkContext.getConf).getOrCreate()
    //
    //    val tollHistorySchema = StructType(Array(
    //      StructField("vid", IntegerType, nullable = false),
    //      StructField("day", IntegerType, nullable = false),
    //      StructField("xway", IntegerType, nullable = false),
    //      StructField("toll", IntegerType, nullable = false)))
    //
    //    val tollHistory = spark
    //      .read
    //      .format("csv")
    //      .schema(tollHistorySchema)
    //      .load(historicalData)
    //
    //    val history = tollHistory
    //      .rdd
    //      .map(r => (r.getInt(0),( r.getInt(1), r.getInt(2), r.getInt(3) )))
    //      .cache()

    // ===================================

    /*val history = ssc.sparkContext.textFile("./data/car.dat.tolls.dat").map(r => {
      val arr = r.split(",")
      ((arr(0).toInt, arr(2).toShort), (arr(1).toInt, arr(3).toInt))
    }).cache()*/


    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "linear-road-app",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streams = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array("linear-road"),
        kafkaParams)
      )

    val events = streams
      .filter(_.value().nonEmpty)
      .map(deserializeEvent)


   /* val dailyExpenditureRequests = events
      .filter(_.typ == 3)
      .map(e => ((e.vid, e.xWay), e.day))
      .transform(rdd => {
        rdd.join(history)
      }).print()*/

    val positionReports = events
      .filter(_.typ == 0)
      .map(toPositionReport)

    val vehicles        = VehicleStatistics.update(positionReports)
        .foreachRDD(r => {
          println("*** got an RDD, size = " + r.count())

          r.partitioner match {
            case Some(p) => println("+++ partitioner: " + r.partitioner.get.getClass.getName)
            case None => println("+++ no partitioner")
          }

          if (r.count() > 0) {
            println("*** " + r.getNumPartitions + " partitions")
          }
        })
      //.saveAsTextFiles("position-reports", "txt")

    // ##################### ACCIDENT DETECTION LOGIC ######################

    /*val stoppedVehicles = StateSpec.function(updateStoppedVehicles _)
    val accidentsState  = StateSpec.function(updateAccidents _)

    val accidents       = vehicles
      .map(t => (XwayDir(t._2._1.xWay, t._2._1.direction), (t._2._1, t._2._2, t._2._3)))
      .mapWithState(stoppedVehicles)
      .mapWithState(accidentsState)

    // ##################### NUMBER OF VEHICLES LOGIC ######################

////    //val accidents               = AccidentAnalytics.process(vehicles)

    val positionReportCount = StateSpec.function(updateNumberOfPositionReports _)
    val NOVstate = StateSpec.function(updateNumberOfVehicles _)

    val vehicleStatistics        = vehicles
      .map(r => (XwayDir(r._2._1.xWay, r._2._1.direction), (r._2._1.vid, r._2._1.position/5280, r._2._1.time / 60, r._2._3, r._2._1.speed)))
      .mapWithState(positionReportCount)
      //.mapWithState(NOVstate)

    // ##################### AVERAGE VELOCITY LOGIC ######################

    val speedSum = StateSpec.function(TrafficAnalytics.vehicleAvgSpd _)

    val vehicleSpeeds  = vehicleStatistics.mapWithState(speedSum)

    val joined = vehicleSpeeds.join(accidents)*/

    // ##################### AVERAGE VELOCITY LOGIC ######################


//    val lavState = StateSpec.function(TrafficAnalytics.lav _)
//
//    val minuteVelocities = vehicles
//      .map(r => {
//        val report = r._2._1
//        (XWaySegDirMinute(report.xWay, report.segment, report.direction, report.time/60), (report.speed, 1))
//      })
//      .reduceByKey((t1,t2) => (t1._1 + t2._1, t1._2 + t2._2))
//      .mapWithState(vehicleSpeeds)
//      .map(r => (XwaySegDir(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute, r._2)))
//      .mapWithState(lavState)

//    val tolls = positionReports
//        .map(r => (XWaySegDirMinute(r._2.xWay, r._2.segment, r._2.direction, r._2.time/60), r._1))
//        .join(numberOfVehicles)
//        .join(minuteVelocities)
        //.print()

//    val segmentStatistics       = SegmentAnalytics.update(accidents, numberOfVehicles, avgVelocitiesPerMinute)
//
//    segmentStatistics.checkpoint(Seconds(5))
//
//    // join the stream of segment crossing vehicles in the current batch with segment statistics and produce toll notifications
//    val tollNotifications = vehicles
//      .filter(_._2.segmentCrossing)
//      .map(r => (XwaySegDir(r._2.xWay, (r._2.position/5280).toByte, r._2.direction), (r._1, r._2.internalTime)))
//      .join(segmentStatistics)
//      .map(r => {
//          val vid   = r._2._1._1
//          val count = r._2._2.numberOfVehicles
//          val speed = r._2._2.sumOfSpeeds
//          val lav   = speed / count
//          val toll = if (lav >= 40 || count <= 50 || r._2._2.accidentExists) 0 else 2 * math.pow(count - 50, 2)
//          TollNotification(vid, r._2._1._2, System.currentTimeMillis(), lav.toInt, toll)
//      })
//      .saveAsTextFiles(output+"query-0")


    //ACCOUNT BALANCE REQUESTS
       /* val accountBalanceState = StateSpec.function(AccountBalanceAnalytics.getAccountBalance _).initialState(mappedtolls)
        val accountBalanceRequests = events
          .filter(e => e.typ == 2)
            .map(r => (r.vid, r.xWay+"."+r.d))
            .mapWithState(accountBalanceState)
            .print(10)*/
    //      .transform((rdd, time) => {
    //        rdd.map(e => (e._2.vid, AccountBalanceRequest(2, e._2.time, e._2.vid, e._2.qid, time.milliseconds)))
    //      })
    //      .mapWithState(accBalState)
    //      .foreachRDD(rdd => {
    //        rdd.saveAsTextFile("/output/account_balances")
    //      })
          //.saveAsTextFiles("/output/account_balances")


    // DAILY EXPENDITURE REQUESTS
//    val dailyExpenditures = events
//        .filter(_.typ == 3)
//        .map(e => {
//          (e.vid, (e.d, e.xWay, e.qid, e.internalTime))
//        })
//        .transform(rdd => {
//          rdd.join(history).filter(r => r._2._1._1 == r._2._2._1 && r._2._1._2 == r._2._2._2).map(r => {
//            DailyExpenditureReport(r._2._1._4, System.currentTimeMillis(), r._2._1._3, r._2._2._3, r._2._1._1)
//          })
//        })
//      .saveAsTextFiles(output+"query-2")

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *
    * @param key (Xway, Direction)
    * @param value (VehicleID, Segment, Minute, Count)
    * @param state Map(Minute -> Array[100])
    * @return (VID, numberOfVehicles)
    */
  def updateNumberOfVehicles(key: XwayDir, value:Option[(Int,Int,Int,Boolean,Int)], state:State[Map[Int, Array[Int]]]):(Int, (Int,Int)) = {

    val minute = value.get._3
    val segment = value.get._2
    val count = if (value.get._4) 1 else 0
    val vid = value.get._1
    val sumOfPosReports = value.get._5

    val map = state.getOption().getOrElse(Map(minute -> Array.fill(100)(0)))
    //val arr = map.getOrElse(minute, Array.fill(100)(0))
    val newMap =if (map.contains(minute)) {
      val arr = map(minute)
      arr(segment) += count
      map + (minute -> arr)
    } else {
      val arr = Array.fill(100)(0)
      arr(segment) = 1
      map + (minute -> arr)
    }
    state.update(newMap)
    (vid, (newMap(minute)(segment), sumOfPosReports))

  }

  def updateNumberOfPositionReports(key: XwayDir, value:Option[(Int,Int,Int,Boolean,Double)], state:State[Map[Int, Array[(Int,Int,Int)]]]):(XwayDir, (Int,Int,Int,Int,Int,Int)) = {

    val minute = value.get._3
    val segment = value.get._2
    val vid = value.get._1
    val segmentCrossing = value.get._4
    val speed = value.get._5.toInt

    val map = state.getOption().getOrElse(Map())

    val newMap = if (map.contains(minute)) {
      val currentStat = map(minute)(segment) //(NOV, TotalCount, TotalSpeed)
      val newStat = if (segmentCrossing) (currentStat._1+1, currentStat._2+1, currentStat._3+speed)
      else (currentStat._1, currentStat._2+1, currentStat._3+speed)

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
  }

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
    *
    * @param key
    * @param value
    * @param state (Lane,Pos)->Set(Vid)
    * @return XwayDir -> (Seg, Size, Min)
    */
  def updateStoppedVehicles(key: XwayDir, value:Option[(VehicleInformation, Boolean, Boolean)],
                            state:State[Map[(Int,Int), Set[Int]]]):(XwayDir, (Int, Int, Int, Int)) = {

    val positionReport = value.get._1
    val isStopped = value.get._2
    val minute = positionReport.time/60
    val mapKey = (positionReport.lane, positionReport.position)

    val s = state.getOption().getOrElse(Map())

    val newMap = if (isStopped) {
      val newSet = if (s.contains(mapKey))
        s(mapKey) + positionReport.vid
      else
        Set(positionReport.vid)

      s + (mapKey -> newSet)
    } else {
      val preKey = (positionReport.prevLane, positionReport.prevPos)
      val newSet = if (s.contains(preKey))
        s(preKey) - positionReport.vid
      else
        Set[Int]()
      s + (preKey -> newSet)
    }

    state.update(newMap)

    (key, (positionReport.vid, positionReport.position/5280, newMap.getOrElse(mapKey, Set()).size, minute))
  }

  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def updateAccidents(key: XwayDir, value:Option[(Int, Int, Int, Int)], state:State[Array[Int]]):(Int, Boolean) = {

    val vid = value.get._1
    val segment = value.get._2
    val stoppedVehicles = value.get._3
    val minute = value.get._4

    val accidents:Array[Int] = state.getOption().getOrElse(Array.fill(100)(-1))

    if (stoppedVehicles > 1) {
      if (accidents(segment) == -1)
        accidents(segment) = minute
    } else {
      accidents(segment) == -1
    }

    state.update(accidents)

    val direction = key.dir

    val range = if (direction == 0) {
      val to = segment+5
      accidents.slice(segment, to)
    } else {
      val from = segment-5
      accidents.slice(from, segment+1)
    }

    val accidentExist = range.count(s => (s != -1) && (s < minute)) > 0

    // direction: 0 -> eastbound, 1 <- westbound
    // depending on direction:
    // if eastbound +5 segment from current segment
    // if westbound -5 segments from current segment

    // if values > -1 are found, check if one of them is smaller than the current minute
    // if yes, then there is accident detected else

    (vid, accidentExist)

  }

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
  def stopDetection(key: Int, value:Option[PositionReport], state:State[(Int, Byte, Byte, Int, Int)]):(PositionReport, Boolean) = {

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
        array(1).toInt,
        array(2).toInt,
        array(3).toDouble,
        array(4).toShort,
        array(5).toByte,
        array(6).toByte,
        array(7).toByte,
        array(8).toInt,
        array(9).toInt,
        array(14).toShort,
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
  def isOnTravelLane(lane:Byte):Boolean = {
    lane > 0 && lane < 4
  }

  def calculateToll(lav:Double, nov:Int):Double = {
    if (lav >= 40 || nov <= 50) 0.0
    else scala.math.pow(2*(nov - 50), 2)
  }

  def keyByVehicle(event: Event):(Int, PositionReport) = {
    (event.vid, PositionReport(event.time, event.vid, event.speed, event.xWay, event.lane, event.direction, event.segment, event.position, event.internalTime))
  }
  // ===============================================

  case class TollNotification(vid:Int, time:Long, emit:Long, spd:Double, toll:Double) {
    override def toString: String = s"0,$vid,$time,$emit,$spd,$toll"
  }
  case class DailyExpenditureReport(time:Long, emit:Long, qid:Int, bal:Int, day:Short) {
    override def toString: String = s"3,$time,$emit,$qid,$bal"
  }
  case class TollHistory(vid:Int, day: Int, xWay: Int, toll:Int)

}