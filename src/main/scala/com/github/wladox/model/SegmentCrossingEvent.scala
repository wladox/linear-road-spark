package com.github.wladox.model

/**
  * Created by root on 15.01.17.
  */
case class SegmentCrossingEvent(vid:Int, minute: Short, xWay:Int, seg:Byte, dir:Byte, notification:Boolean)
