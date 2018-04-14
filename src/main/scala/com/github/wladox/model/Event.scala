package com.github.wladox.model

/**
  * Created by wladox on 04.12.16.
  */

case class Event(
                  typ:Int,
                  time:Short,
                  vid:Int,
                  speed:Int,
                  xWay:Byte,
                  lane:Byte,
                  direction:Byte,
                  segment:Byte,
                  position:Int,
                  qid:String,
                  day:Byte,
                  internalTime:Long
) extends Serializable{

  override def toString: String =
    "(time: " + time + ", vehicle: " + vid + ", speed: " + speed + ", pos: " + position + ", xWay: " + xWay + ", lane: " + lane + ", direction: " + direction + ", segment: " + segment + ")"

}

