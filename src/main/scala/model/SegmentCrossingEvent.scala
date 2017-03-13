package model

/**
  * Created by root on 15.01.17.
  */
case class SegmentCrossingEvent(vid:Int, minute: Int, xWay:Int, seg:Byte, dir:Byte, notification:Boolean)
