package com.github.wladox.model

/**
  * Created by wladox on 15.01.17.
  */
case class PositionReport(
                           time:Int,
                           vid:Int,
                           speed:Double,
                           xWay:Int,
                           lane:Byte,
                           direction:Byte,
                           segment:Byte,
                           position:Int,
                           internalTime:Long
                          ) {

  override def toString =
    s"$time.$vid.$speed.$position.$xWay.$lane.$direction.$segment"
}
