package com.github.wladox.model

import scala.collection.mutable

/**
  * Created by root on 23.01.17.
  */
case class LAVState(values: mutable.MutableList[Double], count:Int, sum:Double, start:Int, end:Int) {
  override def toString: String = s"[$values] , $count , $sum , $start , $end "
}
