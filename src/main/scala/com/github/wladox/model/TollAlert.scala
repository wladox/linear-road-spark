package com.github.wladox.model

/**
  * Created by wladox on 28.02.18.
  */
case class TollAlert(typ:Int, carid:Int, time:Long, emittedTime:Long, lav:Float, toll:Int) {

  override def toString: String = s"$typ,$carid,$time,$emittedTime,$lav,$toll"

}
