package structured

import com.github.wladox.model.{Event, PositionReport}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}

object StreamingWordCount {

  case class TollHistory(vid:Int, day:Int, xway:Int, toll:Int)
  case class VehicleTrip(trip:Seq[PositionReport])
  case class VidSeg(vid:Int, segment:Int, count:Long)

  implicit val userEventEncoder: Encoder[PositionReport] = Encoders.kryo[PositionReport]
  implicit val userSessionEncoder: Encoder[VehicleTrip] = Encoders.kryo[VehicleTrip]

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder().appName("LinearRoadBenchmark")
                .master("local[*]")
                .getOrCreate()

//    val lines = spark
//                .readStream
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "localhost:9092")
//                .option("subscribe", "xway*")
//                .option("startingOffsets", "earliest*")
//                .load()

    import spark.implicits._

    // HISTORICAL DATA
    val customSchema = StructType(Array(
          StructField("vid", IntegerType, nullable = false),
          StructField("day", IntegerType, nullable = false),
          StructField("xway", IntegerType, nullable = false),
          StructField("toll", IntegerType, nullable = false)))

    val eventSchema = StructType(Array(
      StructField("typ", IntegerType, nullable = false),
      StructField("time", IntegerType, nullable = false),
      StructField("vid", IntegerType, nullable = false),
      StructField("speed", IntegerType, nullable = false),
      StructField("xway", IntegerType, nullable = false),
      StructField("lane", IntegerType, nullable = false),
      StructField("dir", IntegerType, nullable = false),
      StructField("segment", IntegerType, nullable = false),
      StructField("position", IntegerType, nullable = false),
      StructField("qid", IntegerType, nullable = false),
      StructField("init", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false),
      StructField("dow", IntegerType, nullable = false),
      StructField("tod", IntegerType, nullable = false),
      StructField("day", IntegerType, nullable = false)
    ))

    val historicalTolls = spark
      .read.format("csv")
      .schema(customSchema)
      .load("/home/wladox/workspace/linear/input/car.dat.tolls.dat")
      .as[TollHistory]
    // =================

    // INCOMING EVENTS
    val inputDF = spark
      .readStream
      .format("socket")
      //.schema(eventSchema)
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val events = inputDF
      .as[String]
      .map(_.split(","))
      .map(deserialize)
      .withColumn("dt", from_unixtime($"time", "HH:mm:ss").cast("timestamp"))

    val positionReports :Dataset[Row] = events
      .filter($"typ" === 0)

    // calculate avg speeds of a vehicle per minute
    /*val vehicleAvgs = positionReports.as[PositionReport]
      .map(r => {
        (r.vid, r.time / 60 +1 , r.speed, r.dt)
      })
      .groupByKey(r => r._1)
      .mapGroupsWithState(updateMinuteAverages)
      .grou

      //.avg("speed")

    val lav = vehicleAvgs
      .groupBy($"time")
      .avg("avg(speed)").as("minAvg")

    implicit val vidSegEncoder: Encoder[VidSeg] = Encoders.kryo[VidSeg]

    val segCrossings = events
      .select($"vid", $"segment")
      .groupBy($"vid", $"segment")
      .count().as("count")
      .filter($"count" === 1)

    val historicalBalances = events
      .filter($"typ" === 3)
      .join(historicalTolls, Seq("vid", "day"))

    val query = lav.writeStream
      .outputMode("complete")
      .format("console")
      .start()*/

//    val historicalQuery = historicalBalances
//      .writeStream
//      .outputMode("append")
//      .format("")
//      .start()

    //query.awaitTermination()

  }

//  def updateMinuteAverages(vid: Int,
//                           positionReports: Iterator[(Int, Int, Double, Timestamp)],
//                           state: GroupState[Double]): Option[Double] = {
//    if (state.hasTimedOut) {
//      // We've timed out, lets extract the state and send it down the stream
//      state.remove()
//      state.getOption
//    } else {
//      /*
//        New data has come in for the given user id. We'll look up the current state
//        to see if we already have something stored. If not, we'll just take the current user events
//        and update the state, otherwise will concatenate the user events we already have with the
//        new incoming events.
//      */
//      val currentState = state.getOption
//      val updatedUserSession = currentState.fold(VehicleTrip(positionReports.toSeq)) (currentUserSession => VehicleTrip(currentUserSession.trip ++ positionReports.toSeq))
//
//      if (true) {
//        /*
//        If we've received a flag indicating this should be the last event batch, let's close
//        the state and send the user session downstream.
//        */
//        //state.remove()
//        Some(updatedUserSession)
//      } else {
//        state.update(updatedUserSession)
//        state.setTimeoutDuration("1 minute")
//        None
//      }
//    }
//  }



  def toPositionReport(e:Event):PositionReport = {
    PositionReport(e.time, e.vid, e.speed, e.xWay, e.lane, e.direction, e.segment, e.position, e.internalTime)
  }

  def toTollHistory(array:Array[String]):TollHistory = {
    TollHistory(array(0).toInt, array(1).toInt, array(2).toInt, array(3).toInt)
  }

  def deserialize(array:Array[String]): Event = {
    Event(array(0).toShort,
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
      -1
    )
  }
}