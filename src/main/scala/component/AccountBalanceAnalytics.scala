package component

/**
  * Created by root on 04.01.17.
  */
object AccountBalanceAnalytics {

  case class AccountBalanceEvent(typ:Int, time: Int, emit:Long, qid:Int, bal:Double, resultTime:Long)

  /*def accountBalance(events:DStream[AccountBalanceRequest]):DStream[AccountBalanceEvent] = {

  }*/

}
