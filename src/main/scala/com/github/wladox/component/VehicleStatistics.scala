package com.github.wladox.component

import com.github.wladox.LinearRoadBenchmark.XwayDir
import com.github.wladox.model.PositionReport
import org.apache.spark.streaming.State

case class VehicleInformation(report:PositionReport, isStopped:Boolean, isCrossing:Boolean, lastLane:Byte, lastPosition:Int)
case class Accident(time:Int, clearTime:Int, accidentCars:Set[Int]) {
  override def toString: String = s"$time,$clearTime,$accidentCars"
}

object VehicleStatistics {

  /**
    * Updates latest vehicle state to calculate current  speed, whether the vechile is stopped.
    *
    * @param vid    Vehicle ID
    * @param value  Position report
    * @param state  previous position report, stop counter
    * @return stream with updated vehicle information (VID, (Positionreport, isStopped, isSegmentCrossing, subtractSpeed, addSpeed, vehicleCount))
    */
  def updateLastReport(vid:Int, value:Option[PositionReport], state:State[(PositionReport, Int)]):(Int, VehicleInformation) = {

    val currentReport = value.get

    state.getOption() match {
      case Some(lastReport) =>

        val equal = positionChanged(lastReport._1, currentReport)

        val segmentCrossed = currentReport.segment != lastReport._1.segment

        val newCounter    = if (equal) lastReport._2 + 1 else 1

        val isStopped = newCounter >= 4

        val info = VehicleInformation(currentReport, isStopped, segmentCrossed, lastReport._1.lane, lastReport._1.position)

        state.update((currentReport, newCounter))

        (vid, info)
      case None =>

        state.update(currentReport, 1)

        val vehInfo = VehicleInformation(currentReport, isStopped = false, isCrossing = true, currentReport.lane, currentReport.position)

        (vid, vehInfo)
    }
//    val previousPosition:VehicleInformation = state.getOption().getOrElse(
//      VehicleInformation(-1, -1, -1, -1, -1, stopped = false, segmentCrossing = false, -1, -1)
//    )

//    val segmentCrossed = currentReport.position/5280 != pPos/5280
//    val stopped     = newCounter >= 4
//    val newVehiclePosition = VehicleInformation(currentReport.time/60+1,currentReport.xWay.toShort, currentReport.position,
//      currentReport.direction, newCounter.toByte, stopped, segmentCrossed, currentReport.speed, currentReport.internalTime)
//
//    state.update(newVehiclePosition)

//    (vid, newVehiclePosition)

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
  def updateStoppedVehicles(key: XwayDir, value:Option[VehicleInformation], state:State[Map[(Byte,Int), Set[Int]]]):(XwayDir, (VehicleInformation, Int)) = {

    val vehicleInfo = value.get
    val isStopped   = vehicleInfo.isStopped
    val minute      = vehicleInfo.report.time/60

    val mapKey      = (vehicleInfo.report.lane, vehicleInfo.report.position)

    val s = state.getOption().getOrElse(Map())

    // if the vehicle is stopped, check if its VID is stored in the set of stopped vehicles on the given lane and position
    val newMap = if (isStopped) {
      // check if the map contains a set for the given lane and position
      // if there is a set, then add the VID to the set of stopped vehicles
      // otherwise create a new set with the given VID
      val newSet = if (s.contains(mapKey))
        s(mapKey) + vehicleInfo.report.vid
      else
        Set(vehicleInfo.report.vid)
      s + (mapKey -> newSet)
    } else {
      // if the vehicle is not stopped check if there is a set stored for the previous lane and position of the vehicle
      // and remove the VID from this set
      // otherwise do nothing
      val prevKey = (vehicleInfo.lastLane, vehicleInfo.lastPosition)
      if (s.contains(prevKey)) {
        val newSet = s(prevKey) - vehicleInfo.report.vid
        s + (prevKey -> newSet)
      } else {
        s
      }

    }

    state.update(newMap)
    val stoppedCars = newMap.getOrElse(mapKey, Set()).size
    (key, (vehicleInfo, stoppedCars))
  }

  /**
    *
    * @param key
    * @param value
    * @param state
    * @return
    */
  def updateAccidents(key: XwayDir, value:Option[(VehicleInformation, Int)], state:State[Map[Byte, Accident]]):(XWaySegDirMinute, (VehicleInformation, Byte)) = {

    val car             = value.get._1
    val vid             = car.report.vid
    val segment         = car.report.segment
    val stoppedVehicles = value.get._2
    val minute          = car.report.time/60+1

    //val accidents:Array[Int] = state.getOption().getOrElse(Array.fill(100)(-1))
    val accidents = state.getOption().getOrElse(Map())

    // UPDATE/CREATE ACCIDENTS
    // if the current vehicle is stopped and the # of stopped vehicles on the lane and position of the current vehicle
    // is greater than 1, then we need to create or update an accident
    // if there is no accident stored for the given segment create new accident

    if (car.isStopped) {
      // there is an accident
      if (stoppedVehicles > 1) {
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
          print("ACCIDENT: " + accident)
          val newState = accidents ++ Map(segment -> accident)
          state.update(newState)
        }

      } else {
      // only 1 car is stopped, maybe there is an accident

      }
    } else {
      // clear old accident if the current car was involved
      val lastSegment = (car.lastPosition/5280).toByte
      if (accidents.contains(lastSegment)) {
        val cleared = clearAccident(car.report, accidents(lastSegment))
        val newState = cleared match {
          case Some(a) => accidents ++ Map(lastSegment -> a)
          case None => accidents
        }

        state.update(newState)
      } else if (accidents.contains(segment)) {
        val cleared = clearAccident(car.report, accidents(segment))
        val newState = cleared match {
          case Some(a) => accidents ++ Map(segment -> a)
          case None => accidents
        }
        state.update(newState)
      }
    }

    // RETRIEVE ACCIDENTS IN SEGMENT RANGE
    val range = if (key.dir == 0) {
      val to = segment+4
      accidents.filter(e => e._1 >= segment && e._1 <= to)
    } else {
      val from = segment-4
      accidents.filter(e => e._1 >= from && e._1 <= segment)
    }

    // direction: 0 -> eastbound, 1 <- westbound
    // depending on direction:
    // if eastbound +5 segment from current segment
    // if westbound -5 segments from current segment
    // if values > -1 are found, check if one of them is smaller than the current minute
    // if yes, then there is accident detected else

    val segm:Byte = findAccident(minute.toShort, range) match {
      case Some(res) => res._1
      case None => -1
    }

    if (segm != -1)
      print("Accident proximity " + segm)

    (XWaySegDirMinute(key.xWay, car.report.segment, key.dir, minute.toShort),(car, segm))
  }

  def findAccident(minute:Short, accidents:Map[Byte, Accident]):Option[(Byte, Accident)] = {
    accidents.find(t => {
      (t._2.time+1 <= minute && t._2.clearTime == -1) || (t._2.time+1 <= minute && minute <= t._2.clearTime)
    })
  }

  def clearAccident(p:PositionReport, acc:Accident):Option[Accident] = {
    if (acc.accidentCars.contains(p.vid) && acc.clearTime == -1)
      Some(Accident(acc.time, p.time/60+1, acc.accidentCars))
    None
  }
}