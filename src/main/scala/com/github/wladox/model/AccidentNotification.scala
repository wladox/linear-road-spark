package com.github.wladox.model
/**
  * Created by root on 25.01.17.
  */
case class AccidentNotification(time:Long, emit:Long, seg:Byte) {

  override def toString: String = s"ACC_NOT: $time, $emit, $seg"

}