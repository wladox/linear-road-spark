package model

/**
  * Created by root on 15.01.17.
  */
case class MinuteXwaySegDirVIDKey(vid:Int, minute: Int, xWay:Int, seg:Byte, dir:Byte) {

  override def toString: String = "VID: " + vid + ", minute: " + minute + ", xWay: " + xWay + ", seg: " + seg + ", dir: " + dir
}
