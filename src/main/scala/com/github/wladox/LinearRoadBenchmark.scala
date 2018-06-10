package com.github.wladox

import java.util

import com.github.wladox.component.{TrafficAnalytics, VehicleStatistics, VidTimeXway, XWaySegDirMinute}
import com.github.wladox.model.{Event, PositionReport, XwaySegDir}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StateSpec._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


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

  def functionToCreateContext(checkpointDirectory: String): StreamingContext = {

    val conf = new SparkConf()
      //.setMaster("spark://"+host+":"+port) // use always "local[n]" locally where n is # of cores; host+":"+port otherwise
      //.setMaster("local[*]")
      .setAppName("LinearRoadBenchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.executor.memory", "3g")
      //.set("spark.default.parallelism", "1")
      //.set("spark.kryo.registrationRequired", "false") // https://issues.apache.org/jira/browse/SPARK-12591
      //.set("spark.streaming.blockInterval", "1000ms")
      //.set("spark.memory.fraction", "0.3")

    conf.registerKryoClasses(
      Array(classOf[Event], classOf[PositionReport], classOf[XwayDir], classOf[XWaySegDirMinute], classOf[XwaySegDir])
    )

    val sc  = new SparkContext(conf)
    //sc.setLogLevel("debug")
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: LinearRoadBenchmark <checkpointDir> <inputTopic> <outputTopic> <tollHistory> <kafkaBroker>")
      System.exit(1)
    }

    val checkpointDir = args(0)
    val inputTopic    = args(1)
    val outputTopic   = args(2)
    val history       = args(3)
    val broker        = args(4)
    val groupId       = "linear-road-app"

    // create streaming context
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => functionToCreateContext(checkpointDir))

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

    val partitioner = new HashPartitioner(2)

    val events = streams
      .filter(v => !v.value().isEmpty)
      .transform(rdd => rdd.map(deserializeEvent).partitionBy(partitioner).setName("source"))
      .cache()


    // ##################### DAILY EXPENDITURES RESPONSES ######################
    events
      .filter(_._2.typ == DAILY_EXPENDITURE_REQUEST)
      .map(e => {
        val tolls = historyMap.value((e._2.vid, e._2.xWay))
        val emit = System.currentTimeMillis() - e._2.internalTime
        s"3,${e._2.time},$emit,${e._2.qid},${tolls(e._2.day-1)}"
      })
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

    val type0type2 = events
      .filter(e => e._2.typ == POSITION_REPORT || e._2.typ == ACCOUNT_BALANCE_REQUEST)
      .cache()

    // ##################### ACCIDENT DETECTION ######################

    val myPartitioner = new Partitioner() {

      override def numPartitions: Int = 2

      def getPartition(key: Any): Int = key match {
        case null => 0
        case k:XwayDir => k.hashCode % numPartitions
        case _ => {
          val k = key.asInstanceOf[(Byte,Byte,Int)]
          XwayDir(k._1, k._2).hashCode % numPartitions
        }
      }
    }

    val updatedPositionReports = type0type2
      .filter(e => e._2.typ == POSITION_REPORT)
      .map(r => keyByVid(r._2))
      .mapWithState(function(VehicleStatistics.updateLastReport _).partitioner(myPartitioner))

      //.mapWithState(function(VehicleStatistics.updateStoppedVehicles _))

    val accidents = updatedPositionReports
      .mapWithState(function(VehicleStatistics.updateAccidents _))
      //.saveAsTextFiles("output/vehicle-")

      //.foreachRDD(rdd => println("RDD size " + rdd.count()))

    // ##################### ACCIDENT NOTIFICATION ######################
    accidents
      .filter(v => v._2._1.isCrossing && v._2._1.lane != 4 &&  v._2._2 != -1)
      .map(r => {
        val time = r._2._1.time
        val emit = System.currentTimeMillis() - r._2._1.internalTime
        val report = r._2._1
        s"1,$time,$emit,${report.xWay},${r._2._2},${report.direction},${report.vid}"
      })
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

    val novPartitioner = new Partitioner() {

      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key match {
        case null => 0
        case XwaySegDir(xway, segment, direction) => {
          nonNegativeMod(XwayDir(xway, direction).hashCode, numPartitions)
        }
      }
    }

    val keyedReports = type0type2
      .filter(e => e._2.typ == POSITION_REPORT)
      .map(r => (XwaySegDir(r._2.xWay, r._2.segment, r._2.direction), (r._2.vid, r._2.speed, r._2.time)))
      .cache()
    //.transform(rdd => rdd.map(r => (XwaySegDir(r._2.xWay, r._2.segment, r._2.direction), (r._2.vid, r._2.speed, r._2.time))).partitionBy(novPartitioner))

    val vidPartitioner = new Partitioner() {

      override def numPartitions: Int = 4

      override def getPartition(key: Any): Int = key match {
        case null => 0
        case s:String => {
          val splitted = s.split(".")
          nonNegativeMod(s"${s(0)}.${s(2)}".hashCode, numPartitions)
        }
      }

    }

    // ##################### NUMBER OF VEHICLES ######################
    val NOV = keyedReports.mapWithState(function(TrafficAnalytics.updateNOV _).partitioner(novPartitioner))
      // TODO add partitioner

    // ##################### AVERAGE VELOCITY ######################
    val LAV = keyedReports.mapWithState(function(TrafficAnalytics.updateLAV _).partitioner(novPartitioner))

    // CORRECT ORDER DO NOT TOUCH

    // ##################### TOLL CALCULATION / NOTIFICATION ######################
    val segmentStats = LAV
      .join(NOV, vidPartitioner)    // JOIN CORRECTLY
      .join(accidents, vidPartitioner) //.saveAsTextFiles("output/acc-")
      .mapValues(v => (v._1._1, v._1._2, v._2._2, v._2._1.isCrossing))

    type0type2
      .map(e => (s"${e._2.vid}.${e._2.time}.${e._2.xWay}", e))
      .join(segmentStats, vidPartitioner)
      .filter(r => (r._2._2._4 && r._2._1._2.lane != 4) || r._2._1._2.typ == ACCOUNT_BALANCE_REQUEST)
      .map(r => {

        val event = r._2._1._2
        if (event.typ == POSITION_REPORT) {

          val lav         = r._2._2._1
          val nov         = r._2._2._2
          val isAccident  = r._2._2._3 != -1
          val toll        = calculateToll(lav, nov, isAccident)

          (s"${event.vid},${event.xWay}", s"notify,${event.time},${event.internalTime},$lav,$toll")
        } else {

          val qid = event.qid

          (s"${event.vid},${event.xWay}", s"get,${event.time},${event.internalTime},$qid")
        }
      })
      .mapWithState(function(updateAccountBalance _))
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

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def updateAccountBalance(key: String, value:Option[String], state:State[List[Int]]):String = {

    val arr         = value.get.split(",")
    val tolls       = state.getOption().getOrElse(List[Int]()) // previous toll, current balance, time
    val time        = arr(1).toInt
    val intTime     = arr(2).toLong
    val vid         = key.split(",")(0)
    val emit        = System.currentTimeMillis() - intTime

    val response = if (arr(0) == "notify") {

      val lav         = arr(3)
      val toll        = arr(4).toInt
      val newTolls    = tolls ++ List(toll)
      state.update(newTolls)
      s"0,$vid,$time,$emit,$lav,$toll"

    } else {

      val qid         = arr(3)
      val bal         = tolls.slice(0, tolls.size-1).sum

      s"2,$time,$emit,$time,$qid,$bal"
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
  def deserializeEvent(record: ConsumerRecord[String, String]): (XwayDir, Event) = {
    val array = record.value().trim().split(",")
    val e = Event(
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
    (XwayDir(e.xWay, e.direction), e)
  }

  /**
    * Converts an Event object into PositionReport object
    * @param e @see Event
    * @return Tuple (vehicleID, PositionReport)
    */
  def keyByVid(e:Event):((Byte,Byte,Int), PositionReport) = {
    ((e.xWay, e.direction, e.vid), PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime, isStopped = false, isCrossing = false, -1, -1))
  }

  def keyByXwayDir(e:Event):(XwayDir, PositionReport) = {
    (XwayDir(e.xWay, e.direction), PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime, isStopped = false, isCrossing = false, -1, -1))
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