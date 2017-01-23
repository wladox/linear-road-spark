package model

/**
  * Created by root on 15.01.17.
  */
case class MinuteXWaySegDirKey(minute: Int, xWay:Int, seg:Int, dir:Byte) {

  override def toString: String = "minute: " + minute + ", xWay: " + xWay + ", seg: " + seg + ", dir: " + dir
}
