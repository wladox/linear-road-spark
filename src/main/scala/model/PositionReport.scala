package model

/**
  * Created by root on 15.01.17.
  */
case class PositionReport(
                          time:Int,
                          vid:Int,
                          speed:Double,
                          xWay:Short,
                          lane:Short,
                          dir:Byte,
                          seg:Byte,
                          pos:Int
                          ) {

  override def toString =
    "POS: (time: " + time + ", vehicle: " + vid + ", speed: " + speed + ", pos: " + pos + ", xWay: " + xWay + ", lane: " + lane + ", direction: " + dir + ", segment: " + seg + ")"
}
