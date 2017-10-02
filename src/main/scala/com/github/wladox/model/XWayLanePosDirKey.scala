package com.github.wladox.model

/**
  * Created by root on 16.01.17.
  */
case class XWayLanePosDirKey(xWay:Int, lane:Short, pos:Int, dir:Byte) extends Serializable{

  override def toString =
    "(xWay: " + xWay + ", lane: " + lane + ", pos: " + pos + ", direction: " + dir + ")"
}
