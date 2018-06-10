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
    s"$time.$vid.$speed.$position.$xWay.$lane.$direction.$segment.$qid"

}

