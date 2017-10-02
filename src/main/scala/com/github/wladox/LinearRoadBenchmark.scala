package com.github.wladox

import com.github.wladox.component.{AccidentAnalytics, SegmentAnalytics, TrafficAnalytics, VehicleStatistics}
import com.github.wladox.component.TrafficAnalytics.XWaySegDirMinute
import com.github.wladox.component.VehicleStatistics.VehicleInformation
import com.github.wladox.model.{Event, PositionReport, XwaySegDir}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * This is the entry point for the benchmark application.
  */
object LinearRoadBenchmark {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: LinearRoadBenchmark <hostname> <port> <output> <history> <checkpoint>")
      System.exit(1)
    }

    val host          = args(0)
    val port          = args(1)
    val output        = args(2)
    val historicalData = args(3)
    val checkpointDir = args(4)

    val readParallelism       = 1
    val processingParallelism = 1
    val batchInterval         = 1
    val checkpointingInterval = 5

    val conf = new SparkConf()
      .setMaster("local[*]") // use always "local[n]" locally where n is # of cores
      //.setMaster(host+":"+port)
      .setAppName("Linear Road Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.executor.memory", "2g")
      .set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591

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

    // initialize spark context
    val sc      = new SparkContext(conf)
    val ssc     = new StreamingContext(sc, Seconds(batchInterval))
    val spark   = SparkSession.builder.config(sc.getConf).getOrCreate()

    ssc.checkpoint(checkpointDir)

    // configure kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "linearroad",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // define a map of interesting topics
    val topics = Array("position-reports")

    val tollHistorySchema = StructType(Array(
      StructField("vid", IntegerType, nullable = false),
      StructField("day", IntegerType, nullable = false),
      StructField("xway", IntegerType, nullable = false),
      StructField("toll", IntegerType, nullable = false)))

    val tollHistory = spark
      .read
      .format("csv")
      .schema(tollHistorySchema)
      .load(historicalData)

    val partitioner = new HashPartitioner(12)

    val history = tollHistory
      .rdd
      .map(r => (r.getInt(0),( r.getInt(1), r.getInt(2), r.getInt(3) )))
      .partitionBy(partitioner)
      .cache()

    val eventStreams = (1 to readParallelism).map(i => {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    })

    val fullStream = ssc.union(eventStreams)

    val events = fullStream
      .filter(r=>r.value().nonEmpty)
      .map(deserializeEvent)

    val keyedPositionReports = events
      .filter(_.typ == 0)
      .map(toPositionReport)

    val vehicles                = VehicleStatistics.process(keyedPositionReports)
    val accidents               = AccidentAnalytics.process(vehicles)
    val numberOfVehicles        = TrafficAnalytics.countVehiclesInBatch(vehicles)
    val avgVelocitiesPerMinute  = TrafficAnalytics.averageVelocities(vehicles)
    val segmentStatistics       = SegmentAnalytics.update(accidents, numberOfVehicles, avgVelocitiesPerMinute)

    segmentStatistics.checkpoint(Seconds(checkpointingInterval))

    // join the stream of segment crossing vehicles in the current batch with segment statistics and produce toll notifications
    val tollNotifications = vehicles
      .filter(_._2.segmentCrossing)
      .map(r => (XwaySegDir(r._2.xWay, (r._2.position/5280).toByte, r._2.direction), (r._1, r._2.internalTime)))
      .join(segmentStatistics)
      .map(r => {
          val vid   = r._2._1._1
          val count = r._2._2.numberOfVehicles
          val speed = r._2._2.sumOfSpeeds
          val lav   = speed / count
          val toll = if (lav >= 40 || count <= 50 || r._2._2.accidentExists) 0 else 2 * math.pow(count - 50, 2)
          TollNotification(vid, r._2._1._2, System.currentTimeMillis(), lav.toInt, toll)
      })
      .saveAsTextFiles(output+"query-0")


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
    val dailyExpenditures = events
        .filter(_.typ == 3)
        .map(e => {
          (e.vid, (e.d, e.xWay, e.qid, e.internalTime))
        })
        .transform(rdd => {
          rdd.join(history).filter(r => r._2._1._1 == r._2._2._1 && r._2._1._2 == r._2._2._2).map(r => {
            DailyExpenditureReport(r._2._1._4, System.currentTimeMillis(), r._2._1._3, r._2._2._3, r._2._1._1)
          })
        })
      .saveAsTextFiles(output+"query-2")


    ssc.start()
    ssc.awaitTermination()
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