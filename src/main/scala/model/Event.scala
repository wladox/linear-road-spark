package model

/**
  * Created by wladox on 04.12.16.
  */

case class Event(
                  typ:Int,
                  time:Int,
                  vid:Int,
                  speed:Double,
                  xWay:Short,
                  lane:Byte,
                  direction:Byte,
                  segment:Byte,
                  position:Int,
                  qid:Int,
                  d:Short,
                  internalTime:Long
) extends Serializable{

  override def toString: String =
    "(time: " + time + ", vehicle: " + vid + ", speed: " + speed + ", pos: " + position + ", xWay: " + xWay + ", lane: " + lane + ", direction: " + direction + ", segment: " + segment + ")"

}

