package com.github.wladox.model

/**
  * Created by wladox on 15.01.17.
  */
case class PositionReport(time:Int,
                           vid:Int,
                           speed:Float,
                           xWay:Int,
                           lane:Int,
                           direction:Int,
                           segment:Int,
                           position:Int,
                           internalTime:Long
                          ) {

  override def toString = s"$time.$vid.$speed.$position.$xWay.$lane.$direction.$segment"

  /*override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + age;
    result = prime * result + (if (name == null) 0 else name.hashCode)
    return result
  }


  override def equals(obj: scala.Any): Boolean =
    obj match {
      case obj:PositionReport => obj.isInstanceOf[PositionReport] && this.hashCode() == obj.hashCode()
      case _ => false
    }*/
}
