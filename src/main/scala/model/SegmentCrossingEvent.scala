package model

/**
  * Created by root on 15.01.17.
  */
case class SegmentCrossingEvent(vid:Int, minute: Int, xWay:Int, seg:Int, dir:Byte, notification:Boolean)
