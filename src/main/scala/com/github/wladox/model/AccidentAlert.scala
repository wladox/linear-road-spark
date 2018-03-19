package com.github.wladox.model

/**
  * Created by wladox on 28.02.18.
  */
case class AccidentAlert(typ:Int, time:Long, emittedTime:Long, carid:Int, segment:Int) {

  override def toString: String = s"$typ,$time,$emittedTime,$carid,$segment"
}
