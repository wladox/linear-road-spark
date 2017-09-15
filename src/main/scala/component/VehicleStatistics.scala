package component

import component.TrafficAnalytics.XwaySegDirVidMin
import org.apache.spark.streaming.State

object VehicleStatistics {

  case class CarAvgSpeed(vid:Int, min:Int, xWay:Int, seg:Byte, dir:Byte, spd:Double, time:Int, internalTime:Long)

  /**
    *
    * @param key XwaySegmentDirectionVidMinKey
    * @param value vehicle average velocity per minute, internal time
    * @param state Speed
    * @return Key-> MinuteXWaySegDirKey, Value ->(new avg velocity, velocity to subtract, counter)
    */
  def updateVehicleSpeed(key: XwaySegDirVidMin, value: Option[(Double, Int, Long)], state: State[Double]): (CarAvgSpeed, Double) = {

    val currentSpd    = value.get._1
    val time          = value.get._2
    val internalTime  = value.get._3

    if (state.exists()) {
      val spd = state.get()
      state.update((spd + currentSpd) / 2.0)
      (CarAvgSpeed(key.vid, key.minute, key.xWay, key.seg, key.dir, state.get(), time, internalTime), spd)
    } else {
      state.update(currentSpd)
      (CarAvgSpeed(key.vid, key.minute, key.xWay, key.seg, key.dir, state.get(), time, internalTime), -1.0)
    }

  }
}