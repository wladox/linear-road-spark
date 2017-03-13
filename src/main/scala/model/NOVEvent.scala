package model

/**
  * Created by root on 23.01.17.
  */
case class NOVEvent(key: MinuteXWaySegDirKey, nov:Int) {

  override def toString: String = s"NOV: $key -> $nov"
}
