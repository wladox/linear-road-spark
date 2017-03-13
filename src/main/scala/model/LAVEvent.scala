package model

/**
  * Created by root on 23.01.17.
  */
case class LAVEvent(key: MinuteXWaySegDirKey, state:Double) {

  override def toString: String = s"AVG VELOCITY on $key is $state"
}
