package component

import com.github.wladox.LinearRoadBenchmark.TollNotification
import org.apache.spark.streaming.State

/**
  * Created by wladox on 04.01.17.
  */
object AccountBalanceAnalytics {

  /**
    * Adds the toll for the exited segment to the current account balance of a vehicle and output the toll for
    * the new entered segment.
    *
    * @param vid - vehicleID
    * @param value - toll for the previous segment
    * @param state - account balance
    * @return Toll notification for the new segment
    */
//  def update(vid:Int, value:Option[(Double, Double)], state:State[Double]):TollNotification = {
//    val toll = state.getOption().getOrElse(0.0)
//    val prevToll = value.get._1
//    val newToll = value.get._1
//    state.update(toll + prevToll)
//
//    TollNotification(vid, -1, System.currentTimeMillis(), -1, newToll)
//  }


}
