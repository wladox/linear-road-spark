import com.github.wladox.component.{TrafficAnalytics, VehicleStatistics, XWaySegDirMinute}
import com.github.wladox.model.{Event, PositionReport}
import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}
import org.scalatest.FunSuite

/**
  * Created by root on 03.03.18.
  */
class SampleStreamingTest extends FunSuite with StreamingSuiteBase {

  ignore("detect stopped vehicle") {
    val data = List(
      List((61, PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1, false, false, -1, -1))),
      List((61, PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1, false, false, -1, -1))),
      List((61, PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1, false, false, -1, -1))),
      List((61, PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1, false, false, -1, -1)))
    )
    val expected = List(
      List(61, PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1, isStopped = true, isCrossing = false, 2, 232425))
    )

    //testOperation[(Int,PositionReport), (Int, VehicleInformation)](data, myFunc, expected, false)
  }

  /*test("detect accident") {

    val vh1 = (XwayDir(0, 1), (VehicleInformation(PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1), isStopped = true, isCrossing = false, 1, 232425), 2))
    val vh2 = (XwayDir(0, 1), (VehicleInformation(PositionReport(66, 261, 66, 0, 2, 1, 49, 232425, -1), isStopped = true, isCrossing = false, 2, 232425), 2))
    val vh3 = (XwayDir(0, 1), (VehicleInformation(PositionReport(126, 262, 34, 0, 2, 1, 52, 274560, -1), isStopped = false, isCrossing = false, 0, 232425), 2))

    val data = List[List[(XwayDir, (VehicleInformation, Int))]](
      List[(XwayDir, (VehicleInformation, Int))](vh1),
      List[(XwayDir, (VehicleInformation, Int))](vh2),
      List[(XwayDir, (VehicleInformation, Int))](vh3))

    val expected = List[List[(Int, Boolean)]](
      List[(Int, Boolean)]((260, false)),
      List[(Int, Boolean)]((261, true)),
      List[(Int, Boolean)]((262, true)))

    //testOperation(data, accidents _, expected, ordered = false)

  }*/

  /*test("count vehicles") {
    val vh1 = (XwayDir(0, 1), VehicleInformation(PositionReport(61, 260, 60, 0, 2, 1, 49, 232425, -1), isStopped = true, isCrossing = true, 1, 232425))
    val vh2 = (XwayDir(0, 1), VehicleInformation(PositionReport(66, 261, 66, 0, 2, 1, 49, 232425, -1), isStopped = true, isCrossing = true, 2, 232425))
    val vh3 = (XwayDir(0, 1), VehicleInformation(PositionReport(126, 262, 34, 0, 2, 1, 52, 274560, -1), isStopped = false, isCrossing = true, 0, 232425))
    val vh4 = (XwayDir(0, 1), VehicleInformation(PositionReport(126, 262, 34, 0, 2, 1, 52, 274560, -1), isStopped = false, isCrossing = false, 0, 232425))

    val data = List[List[(XwayDir, VehicleInformation)]](
      List[(XwayDir, VehicleInformation)](vh1),
      List[(XwayDir, VehicleInformation)](vh2),
      List[(XwayDir, VehicleInformation)](vh3),
      List[(XwayDir, VehicleInformation)](vh4))

    val expected = List[List[(XwayDir, Int)]](
      List[(XwayDir, Int)]((XwayDir(0, 1), 1)),
      List[(XwayDir, Int)]((XwayDir(0, 1), 2)),
      List[(XwayDir, Int)]((XwayDir(0, 1), 1)),
      List[(XwayDir, Int)]((XwayDir(0, 1), 1)))

    testOperation(data, nov _, expected, ordered = false)

  }*/

  /*test("count position reports") {

    val p1 = (77, PositionReport(30, 77, 25, 0, 2, 1, 1, 10558, -1))
    val p2 = (78, PositionReport(38, 78, 25, 0, 2, 1, 1, 10454, -1))
    val p3 = (79, PositionReport(14, 79, 25, 0, 2, 1, 1, 10435, -1))
    val p4 = (80, PositionReport(1, 80, 25, 0, 2, 1, 1, 10258, -1))

    val v1 = (XWaySegDirMinute(0, 1, 1, 2), 3)
    val v2 = (XWaySegDirMinute(0, 1, 1, 2), 6)
    val v3 = (XWaySegDirMinute(0, 1, 1, 2), 0)
    val v4 = (XWaySegDirMinute(0, 1, 1, 2), 12)

    val data = List[List[(Int, PositionReport)]](
      List[(Int, PositionReport)](p1),
      List[(Int, PositionReport)](p2),
      List[(Int, PositionReport)](p3),
      List[(Int, PositionReport)](p4))

    val expected = List[List[(XWaySegDirMinute, Int)]](
      List[(XWaySegDirMinute, Int)]((XWaySegDirMinute(0, 1, 1, 1), 4))
    )

    testOperation(data, positionReportsCount _, expected, ordered = false)
  }*/

  def positionReportsCount(s:DStream[(Int, PositionReport)]):DStream[(XWaySegDirMinute, Int)] = {
    s.map(r => {
      (XWaySegDirMinute(r._2.xWay, r._2.segment, r._2.direction, (r._2.time/60+1).toShort), 1)
    })
    .reduceByKey((r1, r2) => r1 + r2)
    .mapWithState(StateSpec.function(TrafficAnalytics.vehicleCount _))

  }

  /*def stoppedVehicles(s:DStream[(Int, PositionReport)]):DStream[(Int, VehicleInformation)] = {
    s.mapWithState(StateSpec.function(VehicleStatistics.updateLastReport _))
  }*/

  //def accidents(s:DStream[(XwayDir, (VehicleInformation, Int))]):DStream[(Int, Boolean)] = {
    //s.mapWithState(StateSpec.function(VehicleStatistics.updateAccidents _ ))
  //}

  /*def nov(s:DStream[(XwayDir, VehicleInformation)]):DStream[(XwayDir, Int)] = {
    s.mapWithState(StateSpec.function(TrafficAnalytics.updateNOV _))
  }*/

}

object SampleStreamingTest {
  def updateLastReport(word:String, value:Option[Int], state:State[Int]):(String, Int) = {

    val count = value.get

    state.getOption() match {
      case Some(lastCount) =>
        val newCount = lastCount + count
        state.update(newCount)
        (word, newCount)
      case None =>
        state.update(count)
        (word, count)
    }
  }

}
