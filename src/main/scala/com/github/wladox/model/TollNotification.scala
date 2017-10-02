package com.github.wladox.model

/**
  * Created by root on 23.01.17.
  */
case class TollNotification(vid:Int, time:Long, emit:Long, spd:Double, toll:Double) {

  override def toString: String = s"TOLL: (0, $vid, $time, $emit, $spd, $toll)"
}
