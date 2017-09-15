package component

import org.apache.spark.streaming.State

/**
  * Created by root on 04.01.17.
  */
object AccountBalanceAnalytics {

  case class AccountBalanceEvent(typ:XwayVid, time: Int, emit:Long, qid:Int, bal:Double, resultTime:Long)
  case class XwayVid(xWay:Short, vid:Int)

  def getAccountBalance(vid:Int, xwayday:Option[String], state:State[Map[String, Short]]):Option[Short] = {
    val toll = state.getOption().getOrElse(Map[String,Short]())
    toll.get(xwayday.get)
  }

}
