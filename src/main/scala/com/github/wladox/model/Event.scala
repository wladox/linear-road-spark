package com.github.wladox.model

/**
  * Created by wladox on 04.12.16.
  */

case class Event(
                  typ:Int,
                  time:Int,
                  vid:Int,
                  speed:Float,
                  xWay:Int,
                  lane:Int,
                  direction:Int,
                  segment:Int,
                  position:Int,
                  qid:Int,
                  day:Int,
                  internalTime:Long
) extends Serializable{

  override def toString: String =
    "(time: " + time + ", vehicle: " + vid + ", speed: " + speed + ", pos: " + position + ", xWay: " + xWay + ", lane: " + lane + ", direction: " + direction + ", segment: " + segment + ")"

}

