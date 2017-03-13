package model

/**
  * Created by root on 21.01.17.
  */
case class XwaySegDirKey(xWay: Int, segment:Byte, direction:Int) {

  override def toString: String = s"($xWay , $segment , $direction)"

}