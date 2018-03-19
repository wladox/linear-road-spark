package com.github.wladox

import java.util

import com.github.wladox.component.{TrafficAnalytics, VehicleStatistics, XWaySegDirMinute}
import com.github.wladox.model.{Event, PositionReport, XwaySegDir}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
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
  case class TollNotification(vid:Int, time:Long, emit:Long, spd:Int, toll:Int) {
    override def toString: String = s"0,$vid,$time,$emit,$spd,$toll"
  }
  case class DailyExpenditureReport(time:Long, emit:Long, qid:Int, bal:Int, day:Short) {
    override def toString: String = s"3,$time,$emit,$qid,$bal"
  }
  case class TollHistory(vid:Int, day: Int, xWay: Int, toll:Int)
  case class XwayDir(xWay:Byte, dir:Byte)
  case class VehicleDayXway(vid:Int, day:Short, xWay:Short)

  def functionToCreateContext(host: String, port: Int, outputPath: String, checkpointDirectory: String): StreamingContext = {

    val conf = new SparkConf()
      //.setMaster("spark://"+host+":"+port) // use always "local[n]" locally where n is # of cores; host+":"+port otherwise
      .setMaster("local[*]")
      .setAppName("Linear Road Benchmark")
      .set("spark.driver.memory", "4g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591
      //.set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.memory.fraction", "0.3")


//    conf.registerKryoClasses(
//      Array(
//        classOf[Event],
//        classOf[XWaySegDirMinute],
//        classOf[PositionReport],
//        classOf[TollNotification],
//        classOf[DailyExpenditureReport],
//        classOf[VehicleInformation],
//        classOf[VehicleDayXway]
//      )
//    )

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

    val historicalTolls = ssc.sparkContext.textFile("/home/wladox/workspace/LRSparkApplication/data/car.dat.tolls.dat")
      .map(r => {
        val arr = r.split(",")
        ((arr(0).toInt, arr(2).toByte), arr(3).toByte)
      })
      .groupByKey
      .mapValues(_.toArray)
      .cache()
      //.persist(StorageLevel.MEMORY_ONLY_SER)

    //val broadcast = ssc.sparkContext.broadcast(history)
    //println(broadcast.value.size)


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
      .filter(_.value().trim().nonEmpty)
      .map(deserializeEvent)
      .cache()


    // ##################### DAILY EXPENDITURES ######################
    val daily = events
      .filter(_.typ == 3)
      .map(e => {
        ((e.vid,e.xWay), (e.qid, e.day, e.time, e.internalTime))
      })
      .transform(
        requests => {
          requests.join(historicalTolls).map(r => {
            val qid   = r._2._1._1
            val time = r._2._1._3
            val emit = math.round((System.currentTimeMillis() - r._2._1._4)/1000f) + time
            val state = r._2._2
            s"3,$time,$emit,$qid,${state(r._2._1._2-1)}"
          })
        })
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          // Print statements in this section are shown in the executor's stdout logs
          val kafkaOpTopic = "type-3-output"
          val props = new util.HashMap[String, Object]()

          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

          val producer = new KafkaProducer[String,String](props)
          partition.foreach( record => {
            val data = record.toString
            // As as debugging technique, users can write to DBFS to verify that records are being written out
            // dbutils.fs.put("/tmp/test_kafka_output",data,true)
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
            producer.send(message)
          } )
          producer.close()
        })
      })
//      .saveAsTextFiles("output/type-3")

    // ##################### ACCOUNT BALANCE REQUESTS ######################
    val accRequests = events
      .filter(_.typ == 2)
      .map(e => ((e.vid, e.xWay), "get,"+e.time+","+e.internalTime+","+e.qid))


    val positionReports = events
      .filter(_.typ == 0)
      .map(toPositionReport)

    val positionReportState = StateSpec.function(VehicleStatistics.updateLastReport _)
    val vehicleStats = positionReports.mapWithState(positionReportState)
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
      //.saveAsTextFiles("position-reports", "txt")

    // ##################### ACCIDENT DETECTION LOGIC ######################

    val accidents       = vehicleStats
      .map(t => (XwayDir(t._2.report.xWay, t._2.report.direction), t._2))
      .mapWithState(StateSpec.function(VehicleStatistics.updateStoppedVehicles _))
      .mapWithState(StateSpec.function(VehicleStatistics.updateAccidents _))

    accidents
      .filter(_._2._2 != -1)
      .map(r => {
        val time = r._2._1.report.time
        val emit = math.round((System.currentTimeMillis() - r._2._1.report.internalTime)/1000f) + time
        s"1,$time,$emit,${r._2._2}"
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          // Print statements in this section are shown in the executor's stdout logs
          val kafkaOpTopic = "type-1-output"
          val props = new util.HashMap[String, Object]()

          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

          val producer = new KafkaProducer[String,String](props)
          partition.foreach( record => {
            val data = record.toString
            // As as debugging technique, users can write to DBFS to verify that records are being written out
            // dbutils.fs.put("/tmp/test_kafka_output",data,true)
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
            producer.send(message)
          } )
          producer.close()
        })
      })

//    accidents.saveAsTextFiles("output/type-1")

    // ##################### NUMBER OF VEHICLES LOGIC ######################

    //val positionReportCount = StateSpec.function(TrafficAnalytics.updateNOV _)

    val nov = vehicleStats
      .map(r => (XWaySegDirMinute(r._2.report.xWay, r._2.report.segment, r._2.report.direction, (r._2.report.time/60+1).toShort), if (r._2.isCrossing) 1 else 0))
      .reduceByKey((r1, r2) => r1 + r2)
      .map(r => (XwaySegDir(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute, r._2)))
      .mapWithState(StateSpec.function(TrafficAnalytics.updateNOV2 _))

    // ##################### AVERAGE VELOCITY LOGIC ######################

    val reports = positionReports
      .map(r => (XWaySegDirMinute(r._2.xWay, r._2.segment, r._2.direction, (r._2.time/60+1).toShort), 1))
      .reduceByKey((r1, r2) => r1 + r2)
      .mapWithState(StateSpec.function(TrafficAnalytics.vehicleCount _))

    val velocities = positionReports
      .map(r => (XWaySegDirMinute(r._2.xWay, r._2.segment, r._2.direction, (r._2.time/60+1).toShort), r._2.speed))
      .reduceByKey((r1, r2) => r1 + r2)
      .mapWithState(StateSpec.function(TrafficAnalytics.spdSumPerMinute _))

    val avgVelocities = reports
      .join(velocities)
      .map(r => {
        (XwaySegDir(r._1.xWay, r._1.seg, r._1.dir), (r._1.minute , r._2._2/r._2._1))
      })
      .mapWithState(StateSpec.function(TrafficAnalytics.lav _))

    val segmentStats = avgVelocities
      .join(nov)

    val tolls = accidents
      .join(segmentStats)
      .map(r => {
        val info = r._2._1._1
        val isAccident = r._2._1._2 != -1
        val lav = r._2._2._1
        val nov = r._2._2._2
        val toll = calculateToll(lav, nov, isAccident)

        val emit = math.round((System.currentTimeMillis() - info.report.internalTime)/1000f) + info.report.time

        ((info.report.vid, info.report.xWay), "update,"+info.report.time+","+emit+","+lav+","+toll)
        //(info.report.vid, r._2._1._1.report.time, -1, lav, toll)
      })

    val tollAssessment = tolls
      .union(accRequests)
      .mapWithState(StateSpec.function(accountBalance _))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          // Print statements in this section are shown in the executor's stdout logs
          val kafkaOpTopic = "type-02-output"
          val props = new util.HashMap[String, Object]()

          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

          val producer = new KafkaProducer[String,String](props)
          partition.foreach( record => {
            val data = record.toString
            // As as debugging technique, users can write to DBFS to verify that records are being written out
            // dbutils.fs.put("/tmp/test_kafka_output",data,true)
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
            producer.send(message)
          } )
          producer.close()
        })
      })

//      .saveAsTextFiles("output/type-02")


    // ##################### AVERAGE VELOCITY LOGIC ######################

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


    ssc.start()
    ssc.awaitTermination()
  }

  def accountBalance(key: (Int, Byte), value:Option[String], state:State[Array[Int]]):String = {

    val arr = value.get.split(",")

    val currentBalance = state.getOption().getOrElse(Array[Int](0,0))

    val res = if (arr(0).equals("update")) {

      val time = arr(1).toInt
      val emit = arr(2)
      val lav = arr(3)
      val toll = arr(4).toInt

      currentBalance(0) = time
      currentBalance(1) += toll

      state.update(currentBalance)

      s"0,${key._1},$time,$emit,$lav,$toll"

    } else {

      val time = arr(1).toShort
      val intTime = arr(2).toLong
      val qid = arr(3)
      val emit = math.round((System.currentTimeMillis() - intTime)/1000f)+time

      s"2,$time,$emit,$qid,$currentBalance"
    }

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
        array(3).toFloat,
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