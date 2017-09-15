import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 21.01.17.
  */
object StatefullStream {

  /*def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]") // use always "local[n]" locally
      .setAppName("LinearRoadBenchmark")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

    // checkpointing is mandatory
    ssc.checkpoint("_checkpoints")

    val rdd = sc.parallelize(0 to 9).map(n => (n, n % 2 toString))
    import org.apache.spark.streaming.dstream.ConstantInputDStream
    val sessions = new ConstantInputDStream(ssc, rdd)

    import org.apache.spark.streaming.{State, StateSpec, Time}
    val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Int]) => {
      println(s">>> batchTime = $batchTime")
      println(s">>> key       = $key")
      println(s">>> value     = $value")
      println(s">>> state     = $state")
      val sum = value.getOrElse("").size + state.getOption.getOrElse(0)
      state.update(sum)
      Some((key, value, sum)) // mapped value
    }
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = sessions.mapWithState(spec)

    mappedStatefulStream.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
*/
}
