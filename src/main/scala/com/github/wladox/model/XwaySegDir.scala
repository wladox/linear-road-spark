package com.github.wladox.model

/**
  * Created by root on 21.01.17.
  */
case class XwaySegDir(xWay:Byte, segment:Byte, direction:Byte) {
  override def toString: String = s"$xWay.$segment.$direction"
}