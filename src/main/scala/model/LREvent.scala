package model

/**
  * Created by wladox on 04.12.16.
  */

@SerialVersionUID(123L)
case class LREvent (
                     eventType:Int,
                     time:Int,
                     vid:Int,
                     speed:Double,
                     xWay:Short,
                     lane:Byte,
                     dir:Byte,
                     seg:Byte,
                     pos:Int,
                     qid:Int,
                     day:Int,
                     internalTime:Long
) extends Serializable{

  override def toString =
    "(time: " + time + ", vehicle: " + vid + ", speed: " + speed + ", pos: " + pos + ", xWay: " + xWay + ", lane: " + lane + ", direction: " + dir + ", segment: " + seg + ")"

}

