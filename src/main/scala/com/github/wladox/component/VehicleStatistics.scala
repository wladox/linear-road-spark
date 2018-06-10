package com.github.wladox.component

import com.github.wladox.XwayDir
import com.github.wladox.model.PositionReport
import org.apache.spark.streaming.State

object VehicleStatistics {

  case class Accident(firstCar:Int, secondCar:Int, created:Int, cleared:Int) {
    override def toString: String = s"$firstCar,$secondCar,$created.$cleared"
  }

  /**
    * Updates latest vehicle state to calculate current  speed, whether the vechile is stopped.
    *
    * @param key    (Xway, Direction, Vehicle ID)
    * @param value  Position report
    * @param state  previous position report, stop counter
    * @return stream with updated vehicle information (VID, (Positionreport, isStopped, isSegmentCrossing, subtractSpeed, addSpeed, vehicleCount))
    */
  def updateLastReport(key:(Byte,Byte,Int), value:Option[PositionReport], state:State[(PositionReport, Int)]):(XwayDir, PositionReport) = {

    val currentReport = value.get

    state.getOption() match {
      case Some(lastReport) =>

        val equal           = positionChanged(lastReport._1, currentReport)
        val isCrossing      = currentReport.segment != lastReport._1.segment || lastReport._1.lane == 4
        val newCounter      = if (equal) lastReport._2 + 1 else 1
        val isStopped       = newCounter >= 4

        //val info = VehicleInformation(currentReport, isStopped, segmentCrossed, lastReport._1.lane, lastReport._1.position)

        state.update((currentReport, newCounter))

        (XwayDir(key._1, key._2), PositionReport(
          currentReport.time,
          currentReport.vid,
          currentReport.speed,
          currentReport.xWay,
          currentReport.lane,
          currentReport.direction,
          currentReport.segment,
          currentReport.position,
          currentReport.internalTime,
          isStopped,
          isCrossing,
          lastReport._1.lane,
          lastReport._1.position))

      case None =>

        state.update(currentReport, 1)

        (XwayDir(key._1, key._2), PositionReport(
          currentReport.time,
          currentReport.vid,
          currentReport.speed,
          currentReport.xWay,
          currentReport.lane,
          currentReport.direction,
          currentReport.segment,
          currentReport.position,
          currentReport.internalTime,
          false,
          true,
          currentReport.lane,
          currentReport.position))
    }

  }

  def positionChanged(r1:PositionReport, r2:PositionReport):Boolean = {
    r1.position == r2.position && r1.lane == r2.lane
  }

  /**
    *
    * @param key
    * @param value
    * @param state (Lane,Pos)->Set(Vid)
    * @return XwayDir -> (Seg, Size, Min)
    */
  def updateStoppedVehicles(key: XwayDir, value:Option[PositionReport], state:State[Map[(Byte,Int), Set[Int]]]):(XwayDir, (PositionReport, Int)) = {

    val report = value.get
    val isStopped   = report.isStopped
    val minute      = report.time/60

    val mapKey      = (report.lane, report.position)

    val s = state.getOption().getOrElse(Map())

    // if the vehicle is stopped, check if its VID is stored in the set of stopped vehicles on the given lane and position
    val newMap = if (isStopped) {
      // check if the map contains a set for the given lane and position
      // if there is a set, then add the VID to the set of stopped vehicles
      // otherwise create a new set with the given VID
      val newSet = if (s.contains(mapKey)) //TODO avoid unnecessary addition to set
        s(mapKey) + report.vid
      else
        Set(report.vid)
      s + (mapKey -> newSet)
    } else {
      // if the vehicle is not stopped check if there is a set stored for the previous lane and position of the vehicle
      // and remove the VID from this set
      // otherwise do nothing
      val prevKey = (report.lastLane, report.lastPos)
      if (s.contains(prevKey)) {
        val newSet = s(prevKey) - report.vid
        s + (prevKey -> newSet)
      } else {
        s
      }

    }

    state.update(newMap)
    val stoppedCars = newMap.getOrElse(mapKey, Set()).size
    (key, (report, stoppedCars))
  }

  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def updateAccidents(key: XwayDir, value:Option[PositionReport], state:State[Map[Int, Accident]]):(String, (PositionReport, Int)) = {

    val report          = value.get
    val vid             = report.vid
    val segment         = report.segment
    val minute          = report.time/60+1

    //val accidents:Array[Int] = state.getOption().getOrElse(Array.fill(100)(-1))
    val accidents:Map[Int,Accident] = state.getOption().getOrElse(Map[Int, Accident]())

    // UPDATE/CREATE ACCIDENTS
    // if the current vehicle is stopped and the # of stopped vehicles on the lane and position of the current vehicle
    // is greater than 1, then we need to create or update an accident
    // if there is no accident stored for the given segment create new accident


    if (report.isStopped) {
      // there is an accident
      if (accidents.contains(report.segment)) {
        val accident = accidents(report.segment)
        if (accident.secondCar == -1) {
          val newAccident = Accident(accident.firstCar, report.vid, minute, -1)
          //System.out.println("ACCIDENT OCCURED: min=" + minute + " seg=" + report.segment)
          val newState = accidents ++ Map(report.segment.toInt -> newAccident)
          state.update(newState)
        }
      } else {
        val newState = accidents ++ Map(report.segment.toInt -> Accident(report.vid, -1, 999, -1))
        state.update(newState)
      }
      /*if (stoppedVehicles > 1) {
        if (accidents.contains(segment)) {
          // there is already an accident on the given segment, so just add the VID to it
          val acc = accidents(segment)
          val accCars = acc.accidentCars
          // update crashed cars
          if (!accCars.contains(vid)) {
            val newState = accidents ++ Map(segment -> Accident(acc.time, acc.clearTime, accCars ++ Seq(vid)))
            state.update(newState)
          }
        } else {
          // there is no accident stored for the given segment, so create new
          val accident = Accident(minute, -1, Set(vid))
          val newState = accidents ++ Map(segment -> accident)
          state.update(newState)
        }

      } else {
      // only 1 car is stopped, nothing to do
      }*/
    } else {

      val prevSegment = report.lastPos / 5280
      if (accidents.contains(prevSegment) && accidents(prevSegment).cleared == -1 &&
        (accidents(prevSegment).firstCar == report.vid || accidents(prevSegment).secondCar == report.vid)) {
        val unclearedAccident = accidents(prevSegment)
        val clearedAccident = Accident(unclearedAccident.firstCar, unclearedAccident.secondCar, unclearedAccident.created, minute)
        val newState = accidents ++ Map(prevSegment -> clearedAccident)
        state.update(newState)
      } else {
        state.update(accidents)
      }

      // clear old accident if the current car was involved
      /*val lastSegment = (report.lastPos/5280).toByte
      if (accidents.contains(lastSegment)) {
        val cleared = clearAccident(report, accidents(lastSegment))
        val newState = cleared match {
          case Some(a) => {
            accidents ++ Map(lastSegment -> a)
          }
          case None => accidents
        }

        state.update(newState)
      } else if (accidents.contains(segment)) {
        val cleared = clearAccident(report, accidents(segment))
        val newState = cleared match {
          case Some(a) => {
            accidents ++ Map(segment -> a)
          }
          case None => accidents
        }
        state.update(newState)
      }*/
    }



    // RETRIEVE ACCIDENTS IN SEGMENT RANGE
    /*val range = if (key.dir == 0) {
      val to = segment+4
      accidents.filter(e => e._1 >= segment && e._1 <= to)
    } else {
      val from = segment-4
      accidents.filter(e => e._1 >= from && e._1 <= segment)
    }*/

    // direction: 0 -> eastbound, 1 <- westbound
    // depending on direction:
    // if eastbound +5 segment from current segment
    // if westbound -5 segments from current segment
    // if values > -1 are found, check if one of them is smaller than the current minute
    // if yes, then there is accident detected else

    /*val segm:Byte = findAccident(minute.toShort, range) match {
      case Some(res) => res._1
      case None => -1
    }*/

    val from  = if (report.direction == 0) report.segment else math.max(report.segment - 4, 0)
    val to    = if (report.direction == 0) math.min(report.segment + 4, 99) else report.segment

    val isAccident = state.get().find(acc => {
      acc._1 >= from && acc._1 <= to && ((minute >= acc._2.created+1 && acc._2.cleared == -1) || (minute >= acc._2.created+1 && minute <= acc._2.cleared))
    })

    val segm = if (isAccident.isDefined) isAccident.get._1 else -1

    /*if ((report.vid == 5768 || report.vid == 0) && report.time == 621 && report.segment == 40)
      System.out.print()

    if (report.vid == 16265 && report.time == 1150)
      System.out.print()*/

    (s"${report.vid}.${report.time}.${report.xWay}",(report, segm))
  }

  def findAcc(report:PositionReport, accidents:Array[Option[Accident]]): Int = {

    val from  = if (report.direction == 0) report.segment else math.max(report.segment - 4, 0)
    val to    = if (report.direction == 0) math.min(report.segment + 4, 99) else report.segment
    val minute = report.time/60+1
    val range = accidents.zipWithIndex.slice(from, to+1)

    val acc = range.find(t => {
      t._1.isDefined && ((minute >= t._1.get.created+1 && t._1.get.cleared == -1) || (minute >= t._1.get.created+1 && minute <= t._1.get.cleared))
    })

    if (acc.isDefined) acc.get._2 else -1
  }

//  def findAccident(minute:Short, accidents:Map[Byte, Accident]):Option[(Byte, Accident)] = {
//    accidents.find(t => {
//      (t._2.time+1 <= minute && t._2.clearTime == -1) || (t._2.time+1 <= minute && minute <= t._2.clearTime)
//    })
//  }

  /*def clearAccident(p:PositionReport, acc:Accident):Option[Accident] = {
    if (acc.accidentCars.contains(p.vid) && acc.clearTime == -1)
      return Some(Accident(acc.time, p.time/60+1, acc.accidentCars))
    None
  }*/
}