package com.github.wladox.model

/**
  * Created by Wladimir Postnikov on 21.01.17.
  */
case class XwaySegDir(xWay:Byte, segment:Byte, direction:Byte) {

  override def toString: String = s"$xWay.$segment.$direction"

  def canEqual(a: Any): Boolean = a.isInstanceOf[XwaySegDir]

  override def equals(that: Any): Boolean =
    that match {
      case that: XwaySegDir => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + xWay.toInt
    result = prime * result + segment.toInt
    result = prime * result + direction.toInt
    result
  }
}