import component.AccountBalanceAnalytics.XwayVid
import component.TrafficAnalytics.{XWaySegDirMinute, XwaySegDirVidMin}
import component._
import model._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object LinearRoadBenchmark {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: LinearRoadBenchmark <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1)

    val outputDir     = "output/"
    val checkpointDir = "checkpoint/"

    val conf = new SparkConf()
      .setMaster("local[*]") // use always "local[n]" locally where n is # of cores
      //.setMaster(host+":"+port)
      .setAppName("Linear Road Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "2g")
      .set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591

    conf.registerKryoClasses(Array(classOf[Event], classOf[XWaySegDirMinute], classOf[PositionReport]))

    // initialize spark context
    val sc      = new SparkContext(conf)
    val ssc     = new StreamingContext(sc, Seconds(1))
    val spark   = SparkSession.builder.config(sc.getConf).getOrCreate()

    ssc.checkpoint("checkpoint/")

    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "linearroad2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // define a map of interesting topics
    val topics = Array("position-reports")

    import spark.implicits._


    val tollHistorySchema = StructType(Array(
      StructField("vid", IntegerType, nullable = false),
      StructField("day", IntegerType, nullable = false),
      StructField("xway", IntegerType, nullable = false),
      StructField("toll", IntegerType, nullable = false)))

    val tollHistory = spark
      .read
      .format("csv")
      .schema(tollHistorySchema)
      .load("/home/wladox/workspace/linear/input/history.dat")

    val partitioner = new HashPartitioner(12)

    val history = tollHistory
      .rdd
      .map(r => (r.getInt(0),( r.getInt(1), r.getInt(2), r.getInt(3) )))
      .partitionBy(partitioner)
      .cache()

    /*val mappedtolls = sc.textFile("/home/wladox/workspace/linear/input/car.dat.tolls.dat").map(r => {
      val line = r.split(",")
      val vid = line(0).toInt
      val day = line(1)
      val xway = line(2)
      val toll = line(3).toShort
      (vid, (xway+"."+day, toll))
    }).groupByKey().mapValues(values => values.toMap).cache()*/
//    val historyData = tollHistory.rdd
//      .map(r => (r.getInt(0), (r.getInt(1), r.getInt(2), r.getInt(3))))
//      .partitionBy(new HashPartitioner(4))
//      .groupByKey(4)
//      .cache()

    val eventStreams = (1 to 1).map(i => {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    })
    val fullStream = ssc.union(eventStreams)

    fullStream.checkpoint(Seconds(5))

    val events = fullStream
      .filter(r=>r.value().nonEmpty)
      .map(deserializeEvent)

    val positionReports = events
      .filter(_.typ == 0)
      .map(toPositionReport)

    val keyedPositionReports = positionReports
      .map(report => (report.vid, report))


    // SEGMENT CROSSINGS =====================================
    val vehicleState = StateSpec.function(SegmentStatistics.update _)
    val segmentCrossings = keyedPositionReports
      .mapWithState(vehicleState)


    // ACCIDENT DETECTION
    val vehicleInformation = StateSpec.function(AccidentAnalytics.updateVehicleInformation _)
    val accidentsState    = StateSpec.function(AccidentAnalytics.updateAccidents _) // ADD TIMEOUT

    val accidents = segmentCrossings
      .map(report => (report._1.vid, report._1))
      .mapWithState(vehicleInformation)
      .filter(_.isDefined)
      .map(r => (r.get._1, r.get._2))
      .mapWithState(accidentsState)



    def red:(Int, Int) => Int = {
      _ + _
    }

    // VEHICLE COUNT
    val novState = StateSpec.function(TrafficAnalytics.vehicleCountPerMinute _)
    val NOV = segmentCrossings
      .map(r => (XWaySegDirMinute(r._1.xWay, r._1.segment, r._1.direction, r._1.time / 60 + 1), if (r._2) 1 else 0))
      .reduceByKey(red, 4)
      .mapWithState(novState)

    // VEHICLE SPEED
    val vehicleSpeedState = StateSpec.function(TrafficAnalytics.vehicleAvgSpd _) // ADD TIMEOUT
    val avgMinuteSpeed = segmentCrossings
      .map(r => {
        val report = r._1
        val min = ((report.time / 60) + 1).toShort
        val key = XwaySegDirVidMin(report.xWay, report.segment, report.direction, report.vid, min)
        (key, report.speed)
      })
      .mapWithState(vehicleSpeedState)
      .reduceByKey((t1,t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })
      .mapWithState(StateSpec.function(TrafficAnalytics.spdSumPerMinute _))

    // SEGMENT STATISTICS
    val segmentStatistics = StateSpec.function(SegmentStatistics.updateTolls _)
    val tolls = accidents
      .join(NOV, 12)
      .join(avgMinuteSpeed)
      .map(r => {
          val key = r._1
          val minute = r._1.minute
          val pos = r._2._1._1._1
          val accident = r._2._1._1._2
          val count = r._2._1._2
          val speed = r._2._2
          (XwaySegDir(key.xWay, key.seg, key.dir), (minute.toShort, pos, accident, count, speed))
        })
      .mapWithState(segmentStatistics)

    tolls.checkpoint(Seconds(5))

    val result = segmentCrossings
      .map(r => (XwaySegDir(r._1.xWay, r._1.segment, r._1.direction), r._1.vid))
      .join(tolls)
      .map(r => {
          val vid = r._2._1
          val count = r._2._2._2
          val speed = r._2._2._3
          val acc = r._2._2._1
          val lav = speed / count
          val toll = if (lav >= 40 || count <= 50) 0 else 2 * math.pow(count - 50, 2)
          TollNotification(0, vid, 99999, System.currentTimeMillis(), lav, toll)
      })
      .print(10)


    /*val accidentAlerts = positionReports
      .map(r => ( XwaySegDir(r.xWay, r.segment, r.direction), r))
      .join(accidents)
      .map(record => (record._2._1, record._2._2))
      .print(10)*/




    /*
    val minAvg = vehicleCounts.join(speedSumPerMinute)
    .map(r => {
       (XwaySegDir(r._1.xWay, r._1.seg, r._1.dir), (r._2._1, r._2._2 / r._2._1))
     })

    val notification = segCrossings
      .filter(_._2)
      .map(r => {
       (XwaySegDir(r._1.xWay, r._1.segment, r._1.direction), (r._1.vid, r._1.time, r._1.internalTime))
      })
      .join(minAvg)
        .map(r => {
          val vid = r._2._1._1
          val time = r._2._1._2

          val internalTime:Long= r._2._1._3

          val nov = r._2._2._1
          val lav = r._2._2._2
          val emit = System.currentTimeMillis()

          val accidents = false
          if (lav <= 40 && nov >= 50 && !accidents) {
            val toll = 2 * scala.math.pow(nov - 50, 2)
            (vid, TollNotification(0, vid, time, emit, lav, toll, internalTime, emit - internalTime))
          } else {
            val toll = 0
            (vid, TollNotification(0, vid, time, emit, lav, toll, internalTime, emit - internalTime))
          }

        })
      .mapWithState(StateSpec.function(updateAccountBalance _))
      //.foreachRDD(rdd => rdd.foreach(println))


//      .mapWithState(StateSpec.function(TrafficAnalytics.lav _))


//    val vehiclesTotalSpeedPerMinute = vehiclesPerMinute.join(avgSpeedPerMinute)
//      .map(r => {
//        //(s"${r._1.xWay}.${r._1.seg}.${r._1.dir}", (r._1.minute, r._2._2 / r._2._1))
//        (XwaySegDirKey(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute, r._2._2 / r._2._1))
//      })
//      .mapWithState(StateSpec.function(TrafficAnalytics.lav _))
//      .print(10)

//      .map(e => e._2)
//      .foreachRDD((rdd, time) => {
//        val count = rdd.count()
//        if (count > 0) {
//          rdd.saveAsTextFile(outputDir + "/reports_" + time.milliseconds.toString)
//          numReportsCollected += count
//          if (numReportsCollected > numReportsToCollect) {
//            System.exit(0)
//          }
//        }
//      })
      /*.transform(
      (rdd, time) => {
        rdd.map(event => Event(event.time, event.vid, event.speed, event.xWay, event.lane, event.dir, event.seg, event.pos, event.qid, event.day, time.milliseconds))
      })*/

    // ACCIDENT DETECTION =====================================
//    val stopPredicate = StateSpec.function(stopDetection _)
//    val accPredicate  = StateSpec.function(accidentDetection _)

//    val accidents = positionReports
//      .map(keyByVehicle)
//      .mapWithState(stopPredicate)
//      .map(r => (XWayLaneDir(r._1.xWay, r._1.lane, r._1.dir), (r._1.pos, r._2, r._1.time, r._1.vid)))
//      .mapWithState(accPredicate)


//    val accidentNotifications = accidents
//      .filter(r => r._2.isDefined)
//      .join(segCrossingVehicles)
//      .map(r => r._2._1)
//      .saveAsTextFiles("output/accidents/alerts")


    // TOLL NOTIFICATION ===============================================================
      val carAvgSpec = StateSpec.function(VehicleStatistics.updateVehicleSpeed _)
//    val minAvgSpec = StateSpec.function(updateMinuteAvgs _)
//    val segStatSpec = StateSpec.function(updateSegmentStatistics _)
//    val tollAssessment = StateSpec.function(updateAccountBalance _)

//    val carAvgs = positionReports
//      .map(r => (XwaySegmentDirectionVidMinKey(r.xWay, r.seg, r.dir, r.vid, (r.time / 60) + 1), (r.speed, r.time, r.internalTime)))
//      .mapWithState(carAvgSpec) // key -> XwaySegmentDirectionVidMinKey, value -> (newAvgVel, oldAvgVel)
//      .map(r => {
//        (MinuteXWaySegDirKey(r._1.min, r._1.xWay, r._1.seg, r._1.dir), (r._1.spd, r._2, r._1.vid, r._1.time, r._1.internalTime))
//      })
//      .mapWithState(minAvgSpec)// velocities of all vehicles in a minute
//      .map(r => {
//        (XwaySegDirKey(r._2.xWay, r._2.seg, r._2.dir), (r._2.min, r._2.spd, r._2.count, r._1, r._2.time, r._2.internalTime)) // (min, speed, count, vid, time)
//      })
//      .mapWithState(segStatSpec)
//      .join(segCrossingVehicles)
//      .join(accidents)
//      .mapWithState(tollAssessment)
//      /*.foreachRDD(rdd => {
//        rdd.saveAsTextFile("/output/tollalerts")
//      })*/
//      .saveAsTextFiles("output/toll/alerts")
*/
    //ACCOUNT BALANCE REQUESTS
    val accountBalanceState = StateSpec.function(AccountBalanceAnalytics.getAccountBalance _).initialState(mappedtolls)

    val accountBalanceRequests = events
      .filter(e => e.typ == 2)
        .map(r => (r.vid, r.xWay+"."+r.d))
        .mapWithState(accountBalanceState)
        .print(10)


//      .transform((rdd, time) => {
//        rdd.map(e => (e._2.vid, AccountBalanceRequest(2, e._2.time, e._2.vid, e._2.qid, time.milliseconds)))
//      })
//      .mapWithState(accBalState)
//      .foreachRDD(rdd => {
//        rdd.saveAsTextFile("/output/account_balances")
//      })
      //.saveAsTextFiles("/output/account_balances")


    // DAILY EXPENDITURE REQUESTS =====================================
    val dailyExpenditures = events
        .filter(_.typ == 3)
        .map(e => {
          (e.vid, (e.d, e.xWay, e.qid, e.internalTime))
        })
        .transform(rdd => {
          rdd.join(history).filter(r => r._2._1._1 == r._2._2._1 && r._2._1._2 == r._2._2._2).map(r => {
            DailyExpenditureReport(3, r._2._1._4, System.currentTimeMillis(), r._2._1._3, r._2._2._3, r._2._1._1)
          })
        })


    ssc.start()
    ssc.awaitTermination()
  }

  def updateNOV(key: XWaySegDirMinute, value:Option[Int], state:State[Int]):(XWaySegDirMinute, Int) = {

    val s = state.getOption().getOrElse(0)
    val newState = s+value.get
    state.update(newState)
    (key, newState)

  }

  /**
    *
    * @param key
    * @param value (Min, Speed, Count, VID, time, internal time)
    * @param state
    * @return
    */
  def updateSegmentStatistics(key: XwaySegDir, value:Option[(Int, Double, Int, Int, Int, Long)], state: State[mutable.Map[Int, (Double, Int)]]): (Int, TollNotification) = {

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
  def updateMinuteAvgs(key: XWaySegDirMinute, value: Option[(Double, Double, Int, Int, Long)], state: State[(Double, Int)]): (Int, MinAvgSpeed) = {

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
    *
    * @param key XWay Lane Direction
    * @param value (Position, Stop, Time, VID)
    * @param state (Map[Segment -> Accident])
    * @return (VID, AccidentNotification)
    */
  def accidentDetection(key: XWayLaneDir,
                        value:Option[(Int, Boolean, Int, Int)],
                        state:State[mutable.Map[Int, Accident]]) : (Int, Option[AccidentNotification]) = {

    val pos       = value.get._1
    val vid       = value.get._4

    val isStopped = value.get._2
    val segment   = pos / 5280
    val time      = value.get._3



    if (isStopped) { // stopped vehicle
      if (state.exists()) {
        val accidents = state.getOption().getOrElse(mutable.Map[Int, Accident]())
        val accident = accidents.get(segment)

        if (accident.isDefined && !accident.get.stop2) {
          val acc = Accident(time, key.xWay, pos, key.dir, accident.get.stop1, stop2 = true)
          state.update(accidents + (segment -> acc))
        } else {
          val acc = Accident(time, key.xWay, pos, key.dir, stop1 = true, stop2 = false)
          state.update(accidents + (segment -> acc))
        }
      } else {
        val firstState = mutable.Map[Int, Accident](segment -> Accident(time, key.xWay, pos, key.dir, stop1 = false, stop2 = false))
        state.update(firstState)
      }

    } else { // moving vehicle
      if (state.exists()) {
        val accidents = state.get()
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

    (vid, None)

  }


  def updateAccountBalance(key: Int, value:Option[TollNotification], state:State[Double]):TollNotification = {
    val balance           = state.getOption().getOrElse(0.0)
    val toll  = value.get.toll
    val newBalance = balance + toll
    state.update(newBalance)
    value.get
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
  def getDailyExpenditure(key:VidXwayDay, value:Option[(Int, Int, Long)], state:State[Int]):DailyExpenditureReport = {
    val time = value.get._1
    val qid = value.get._2
    val internalTime = value.get._3
    val emit = System.currentTimeMillis()
    val bal = 0 // change to state.getOption().getOrElse
    DailyExpenditureReport(3, time, emit, qid, bal, 9999)
  }

  /**
    * Converts a raw string into an Event object.
    *
    * @param record ConsumerRecord
    * @return Tuple (Type, Event)
    */
  def deserializeEvent(record: ConsumerRecord[String, String]): Event = {
    val array = record.value().trim().split(",")
      Event(array(0).toShort,
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

  def toTollHistory(e:String):TollHistory = {
    val array = e.split(",")
    TollHistory(array(0).toInt, array(1).toInt, array(2).toInt, array(3).toInt)
  }

  def toPositionReport(e:Event):PositionReport = {
    PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime)
  }

  def getTollHistoryEvent(line:String):TollHistory = {
    val array = line.split(",")
    TollHistory(array(0).toInt, array(1).toInt, array(2).toInt, array(3).toInt)
  }

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

  def deserializeInt(line:String):(Int, (Int,Int)) = {
    val arr = line.split(",")
    val vid = arr(0).toInt
    val xway = arr(2).toInt
    val day = arr(1).toInt
    val toll = arr(3).toInt
    (vid, (day, toll))
  }

  def requestsMapInt(line:String):(Int, (Int, Int)) = {
    val arr = line.split(",")
    val vid = arr(2).toInt
    val xway = arr(4).toInt
    val day = arr(14).toInt
    val time = arr(1).toInt
    (vid, (day, time))
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
  case class Accident(time:Int, xWay: Int, pos:Int, dir:Byte, stop1:Boolean, stop2:Boolean)
  case class MinAvgSpeed(min:Int, xWay:Int, seg:Byte, dir:Byte, spd:Double, count:Int, time:Int, internalTime:Long) {
    override def toString: String = s"AVG speed in $min minute, xWay: $xWay segment $seg direction $dir = $spd of $count vehicles"
  }
  case class TollNotification(typ:Byte, vid:Int, time:Long, emit:Long, spd:Double, toll:Double)
  case class AccountBalanceRequest(typ:Int, time:Int, vid:Int, qid:Int, internalTime:Long)
  case class AccountBalanceReport(typ:Int, time:Int, emit:Long, qid:Int, bal:Double, resultTime:Long, responseTime:Long)
  case class DailyExpenditureRequest(typ:Int, time:Int, vid:Int, qid:Int, xWay:Int, day:Int, internalTime:Long)
  case class DailyExpenditureReport(typ:Int, time:Long, emit:Long, qid:Int, bal:Int, day:Short)

  case class VidXwayDay(vid:Int, xWay:Int) {

    override def canEqual(that: Any): Boolean = that.isInstanceOf[VidXwayDay]

    override def hashCode(): Int = {
      val prime = 31
      var result = 1
      result = prime * vid + xWay;
      return result
    }

    override def equals(that: scala.Any) = {
      that match {
        case that:VidXwayDay => ( that canEqual this ) && (
          this.vid == that.vid && this.xWay == that.xWay
          )
        case _ => false
      }
    }
  }
  case class TollHistory(vid:Int, day: Int, xWay: Int, toll:Int)

}