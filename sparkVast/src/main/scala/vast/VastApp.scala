package main.scala.vast

import java.time._
import java.time.format.DateTimeFormatter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by atuladhar on 6/4/17.
  */
object VastApp {
  val DATA_FILE = "data/lekagulSensorData.csv"
  val DATA_FILE_WITH_WEEK_OF_DAY = "data/sensorDataWithDayOfWeek.csv"

  case class Tracker(unixTimestamp: Long, dateTime: String, date: String, gate: String, carType: String = "")

  case class TripRecord(carId: String, carType: String, date: String, timedPath: List[(String, String)])

  case class TripWithElapsedRecord(carId: String, carType: String, date: String, totalTime: Long, timedPath: List[(String, String, Long)])

  case class PathRecord(carId: String, carType: String, date: String, path: List[String])

  case class PathStringRecord(carId: String, carType: String, date: String, path: String)

  case class PathStringRevRecord(carId: String, carType: String, date: String, path: String, pathRev: String)

  case class PathStringHashRecord(carId: String, carType: String, date: String, path: String, pathRev: String, pathHash: Int, dayOfWeek: String)

  case class MultiDayPathRecord(carId: String, carType: String, daysSpan: Int, path: String)

  case class PathCount(path: String, count: Int)

  case class SensorData(timestamp: String, carId: String, carType: String, gateName: String, dayOfWeek: String)

  //June 21
  case class DistSpeed(distance: Float, speed: Float)
  case class Path(timestamp: String, gate: String, timeElapsed: Long, distance: Float, speed: Float)
  case class MultiPath(timestamp: String, gate: String, timeElapsed: Long, distSpeed: List[DistSpeed])

  case class CompleteRecord(carId: String, carType: String, date: String, totalTime: Long, timedPath: List[Path], path: String, pathRev: String, pathHash: Int, dayOfWeek: String)

  case class SuperCompleteRecord(carId: String, carType: String, date: String, totalTime: Long, timedPath: List[MultiPath], path: String, pathRev: String, pathHash: Int, dayOfWeek: String)

  def asLocalDateTime(d: String): LocalDateTime = {
    LocalDateTime.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def asLocalDate(d: String): LocalDate = {
    LocalDate.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  def asUnixTimestamp(d: LocalDateTime): Long = {
    d.toEpochSecond(ZoneOffset.UTC)
  }

  def asDateTime(d: LocalDateTime): String = {
    d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def asDate(d: LocalDateTime): String = {
    d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  /**
    * Writes a json file with the trip information(Timestamp and gate name) for each car in each day.
    * Using spark functions like groupByKey ... requires us to provide an Encoder. It can be done in
    * 2 ways:
    *   1. explicitly like in writeTripRecord method.
    *   2. import spark.implicits._ like in writePathRecord method.
    *
    * @param sensorData
    */
  private def writeTripRecord(sensorData: DataFrame) = {
    val groupedByCarId: KeyValueGroupedDataset[String, Row] = sensorData.groupByKey(row =>
      row.getAs[String]("car-id")
    )(Encoders.STRING)

    val tripRecords = groupedByCarId.flatMapGroups {
      (carId: String, rowList: Iterator[Row]) =>
        rowList.foldLeft(List[Tracker]()) {
          (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime),
              row.getAs[String]("gate-name")) :: resList
        }.groupBy(_.date)
            .mapValues(l => l.sortBy(_.unixTimestamp))
            .foldLeft(List[TripRecord]()) {
              (tripList, v: (String, List[Tracker])) =>
                val date: String = v._1
                val carType: String = v._2.head.carType
                val timeGatePairs: List[(String, String)] = v._2.map(t => (t.dateTime, t.gate))
                TripRecord(carId, carType, date, timeGatePairs) :: tripList
            }
    }(Encoders.product)

    // Debug
    /*
    println(sensorData.count)

    tripRecords.show(20, false)

    println(tripRecords.getClass)

    println(tripRecords.count)
    println(tripRecords.distinct().count())
    */

    tripRecords
  }

  private def writeTripWithElapsedTimeRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val groupedByCarId: KeyValueGroupedDataset[String, Row] = sensorData.groupByKey(row =>
      row.getAs[String]("car-id")
    )(Encoders.STRING)

    val tripRecords = groupedByCarId.flatMapGroups {
      (carId: String, rowList: Iterator[Row]) =>
        rowList.foldLeft(List[Tracker]()) {
          (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime),
              row.getAs[String]("gate-name")) :: resList
        }.groupBy(_.date)
            .mapValues(l => l.sortBy(_.unixTimestamp))
            .foldLeft(List[TripWithElapsedRecord]()) {
              (tripList, v: (String, List[Tracker])) =>
                val date: String = v._1
                val carType: String = v._2.head.carType
                val startTime: Long = v._2.head.unixTimestamp
                //                val timeGatePairs: List[(String, String)] = v._2.map(t => (t.dateTime, t.gate))

                val (endTime: Long, timeGateElapsedTriple: List[(String, String, Long)]) = v._2
                    .foldLeft((Long.MinValue, List[(String, String, Long)]())) {
                      case ((prevTime: Long, tripleList), t) =>
                        val elapsedTime = if (prevTime > 0) t.unixTimestamp - prevTime else 0

                        (t.unixTimestamp, (t.dateTime, t.gate, elapsedTime)::tripleList)
                    }

                TripWithElapsedRecord(carId, carType, date, endTime - startTime, timeGateElapsedTriple) :: tripList
            }
    }

    tripRecords
  }

  /**
    * Writes a json file with just the trip gates. Uses the import spark.implicits._ to implicitly supply
    * the Encoders.
    *
    * @param spark
    * @param sensorData
    */
  private def writePathRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp))
              .foldLeft(List[PathRecord]()) {
                (tripList, v: (String, List[Tracker])) =>
                  val date: String = v._1
                  val carType: String = v._2.head.carType
                  val pathList: List[String] = v._2.map(t => t.gate)
                  PathRecord(carId, carType, date, pathList) :: tripList
              }
        }
    pathRecords
  }

  /**
    * Write path as a concatenated string for each car per day
    * @param spark
    * @param sensorData
    */
  private def writePathStringRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp))
              .foldLeft(List[PathStringRecord]()) {
                (tripList, v: (String, List[Tracker])) =>
                  val date = v._1
                  val carType = v._2.head.carType
                  val path = v._2.map(t => t.gate).mkString(":")
                  PathStringRecord(carId, carType, date, path) :: tripList
              }
        }
    pathStringRecords
  }

  /**
    * Write path as a concatenated string for each car per day, and also the reverse path.
    * @param spark
    * @param sensorData
    */
  private def writePathRevStringCount(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringCountRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp))
              .foldLeft(List[PathStringRevRecord]()) {
                (tripList, v: (String, List[Tracker])) =>
                  val date = v._1
                  val carType = v._2.head.carType
                  val pathList = v._2.map(t => t.gate)
                  val path = pathList.mkString(":")
                  val pathRev = pathList.reverse.mkString(":")
                  PathStringRevRecord(carId, carType, date, path, pathRev) :: tripList
              }
        }
        .groupByKey {
          row =>
            if(row.path > row.pathRev) {
              row.path.hashCode() + row.pathRev.hashCode() * 31
            } else {
              row.pathRev.hashCode() + row.path.hashCode() * 31
            }
        }
        .mapGroups{(_, rowList) =>
          val (count, path) = rowList.foldLeft((0, "")) { (p, row) =>
            (p._1 + 1, row.path)
          }
          PathCount(path, count)
        }

    /*.foldLeft(Map[String, Int]()) {
  (pathCountMap: Map[String, Int], p: PathStringRevRecord) =>
    if(pathCountMap.contains(p.path)){
      val newCount: Int = pathCountMap.getOrElse(p.path, 0) + 1
      pathCountMap + (p.path -> newCount)
    } else if (pathCountMap.contains(p.pathRev)) {
      val newCount: Int = pathCountMap.getOrElse(p.pathRev, 0) + 1
      pathCountMap + (p.pathRev -> newCount)
    } else {
      pathCountMap + (p.path -> 1)
    }
  //                    pathCountMap
}.foldLeft (List[PathCount]()) {
  (l, m) => PathCount(m._1, m._2) :: l
}*/
    pathStringCountRecords

  }

  /** June 18
    * Write path as a concatenated string for each car per day, and also the reverse path.
    * @param spark
    * @param sensorData
    */
  private def writePathRevStringWithHashCount(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringHashRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carId: String, rowList: Iterator[Row]) =>
          var carType = ""
          rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            carType = row.getAs[String]("car-type")
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp))
              .foldLeft(List[PathStringHashRecord]()) {
                (tripList, v: (String, List[Tracker])) =>
                  val date = v._1
                  val pathList = v._2.map(t => t.gate)
                  val path = pathList.mkString(":")
                  val pathRev = pathList.reverse.mkString(":")
                  var hash = 1
                  if(path > pathRev) {
                    hash = path.hashCode() + pathRev.hashCode() * 31
                  } else {
                    hash = pathRev.hashCode() + path.hashCode() * 31
                  }
                  PathStringHashRecord(carId, carType, date, path, pathRev, hash, findDayOfWeek(asLocalDate(date))) :: tripList
              }
        }
    pathStringHashRecords
  }

  /**
    * Write path as a concatenated string for each car for multiple days. Also counts the number of days.
    * @param spark
    * @param sensorData
    */
  private def writeMultipleDayPathStringRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._
    val multiDayPathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          var carType = ""
          val (count, pathList) = rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            carType = row.getAs[String]("car-type")
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp)/*.map(_.gate)*/)
              .foldRight((0, List[String]())) {
                (v: (String, List[Tracker]), tripList) =>
                  val path: String = v._2.map(t => t.gate).mkString(":")
                  (tripList._1 + 1, path :: tripList._2)
              }

          MultiDayPathRecord(carId, carType, count, pathList.mkString("||"))
        }
    multiDayPathStringRecords
  }

  /** June 21
    * Write complete record of path, reverse path, path with time, hash, day of week
    * @param spark
    * @param sensorData
    * @return
    */
  private def writeCompleteRecord(spark: SparkSession, sensorData: DataFrame, distanceData: Map[String, Float]) = {
    import spark.implicits._

    val groupedByCarId: KeyValueGroupedDataset[String, Row] = sensorData.groupByKey(row =>
      row.getAs[String]("car-id")
    )(Encoders.STRING)

    val tripRecords = groupedByCarId.flatMapGroups {
      (carId: String, rowList: Iterator[Row]) =>
        var carType = ""
        rowList.foldLeft(List[Tracker]()) {
          (resList, row: Row) =>
            carType = row.getAs[String]("car-type")
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime),
              row.getAs[String]("gate-name")) :: resList
        }.groupBy(_.date)
            .mapValues(l => l.sortBy(_.unixTimestamp))
            .foldLeft(List[CompleteRecord]()) {
              (tripList, v: (String, List[Tracker])) =>
                val date: String = v._1
                val startTime: Long = v._2.head.unixTimestamp
//                val (endTime: Long, timeGateElapsedTriple: List[Path]) = v._2
                val (lastTrackerOpt: Option[Tracker], timeGateElapsedTriple: List[Path]) = v._2
//                    .foldLeft((Long.MinValue, List[Path]())) {
                    .foldLeft((None: Option[Tracker], List[Path]())) {
                      case ((prevTracker: Option[Tracker], tripleList), t) =>
//                      case ((prevTracker, tripleList), t) =>
//                        val elapsedTime = if (prevTracker != null) t.unixTimestamp - prevTracker.unixTimestamp else 0
                        val (elapsedTime: Long, distance: Float) = prevTracker match {
                          case Some(p) =>
                            val ds: Float = distanceData.getOrElse(p.gate + ":" + t.gate, 0: Float)

                            (t.unixTimestamp - p.unixTimestamp, ds)

                          case None => (0: Long, 0: Float)
                        }

                        (Some(t), Path(t.dateTime, t.gate, elapsedTime, distance, distance * 60 * 60 /elapsedTime)::tripleList)
                    }

                val endTime = lastTrackerOpt match {
                  case Some(t) => t.unixTimestamp
                  case None => 0
                }
                val sortedTimeGateElapsedTriple = timeGateElapsedTriple.reverse
                val pathList = sortedTimeGateElapsedTriple.map(_.gate)
                val path = pathList.mkString(":")
                val pathRev = pathList.reverse.mkString(":")
                var hash = 1
                if(path > pathRev) {
                  hash = path.hashCode() + pathRev.hashCode() * 31
                } else {
                  hash = pathRev.hashCode() + path.hashCode() * 31
                }
                CompleteRecord(carId, carType, date, endTime - startTime, sortedTimeGateElapsedTriple, path, pathRev, hash, findDayOfWeek(asLocalDate(date))) :: tripList
            }
    }

    tripRecords
  }

  private def writeSuperCompleteRecord(spark: SparkSession, sensorData: DataFrame, distanceData: Map[String, List[Float]]) = {
    import spark.implicits._

    val groupedByCarId: KeyValueGroupedDataset[String, Row] = sensorData.groupByKey(row =>
      row.getAs[String]("car-id")
    )(Encoders.STRING)

    val tripRecords = groupedByCarId.flatMapGroups {
      (carId: String, rowList: Iterator[Row]) =>
        var carType = ""
        rowList.foldLeft(List[Tracker]()) {
          (resList, row: Row) =>
            carType = row.getAs[String]("car-type")
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime),
              row.getAs[String]("gate-name")) :: resList
        }.groupBy(_.date)
            .mapValues(l => l.sortBy(_.unixTimestamp))
            .foldLeft(List[SuperCompleteRecord]()) {
              (tripList, v: (String, List[Tracker])) =>
                val date: String = v._1
                val startTime: Long = v._2.head.unixTimestamp
                //                val (endTime: Long, timeGateElapsedTriple: List[Path]) = v._2
                val (lastTrackerOpt: Option[Tracker], timeGateElapsedTriple: List[MultiPath]) = v._2
                    //                    .foldLeft((Long.MinValue, List[Path]())) {
                    .foldLeft((None: Option[Tracker], List[MultiPath]())) {
                  case ((prevTracker: Option[Tracker], tripleList), t) =>
                    //                      case ((prevTracker, tripleList), t) =>
                    //                        val elapsedTime = if (prevTracker != null) t.unixTimestamp - prevTracker.unixTimestamp else 0
                    val (elapsedTime: Long, distances: List[Float]) = prevTracker match {
                      case Some(p) =>
                        val distList: List[Float] = distanceData.getOrElse(p.gate + ":" + t.gate, List.empty[Float])

                        (t.unixTimestamp - p.unixTimestamp, distList)

                      case None => (0: Long, List.empty[Float])
                    }
                    val distSpeeds = distances.map(d=> DistSpeed(d, d * 60 * 60 /elapsedTime))
                    (Some(t), MultiPath(t.dateTime, t.gate, elapsedTime, distSpeeds)::tripleList)
                }

                val endTime = lastTrackerOpt match {
                  case Some(t) => t.unixTimestamp
                  case None => 0
                }
                val sortedTimeGateElapsedTriple = timeGateElapsedTriple.reverse
                val pathList = sortedTimeGateElapsedTriple.map(_.gate)
                val path = pathList.mkString(":")
                val pathRev = pathList.reverse.mkString(":")
                var hash = 1
                if(path > pathRev) {
                  hash = path.hashCode() + pathRev.hashCode() * 31
                } else {
                  hash = pathRev.hashCode() + path.hashCode() * 31
                }
                SuperCompleteRecord(carId, carType, date, endTime - startTime, sortedTimeGateElapsedTriple, path, pathRev, hash, findDayOfWeek(asLocalDate(date))) :: tripList
            }
    }

    tripRecords
  }

  /** June 21
    * Estimates the path with the calculated speed closest to the speed limit.
    * @param spark
    * @param sensorData
    * @param distanceData
    * @return
    */
  private def writeEstimateCompleteRecord(spark: SparkSession, sensorData: DataFrame, distanceData: Map[String, List[Float]]) = {
    import spark.implicits._

    val groupedByCarId: KeyValueGroupedDataset[String, Row] = sensorData.groupByKey(row =>
      row.getAs[String]("car-id")
    )(Encoders.STRING)

    val tripRecords = groupedByCarId.flatMapGroups {
      (carId: String, rowList: Iterator[Row]) =>
        var carType = ""
        rowList.foldLeft(List[Tracker]()) {
          (resList, row: Row) =>
            carType = row.getAs[String]("car-type")
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime),
              asDateTime(localDateTime),
              asDate(localDateTime),
              row.getAs[String]("gate-name")) :: resList
        }.groupBy(_.date)
            .mapValues(l => l.sortBy(_.unixTimestamp))
            .foldLeft(List[CompleteRecord]()) {
              (tripList, v: (String, List[Tracker])) =>
                val date: String = v._1
                val startTime: Long = v._2.head.unixTimestamp
                val (lastTrackerOpt: Option[Tracker], timeGateElapsedTriple: List[MultiPath]) = v._2
                    .foldLeft((None: Option[Tracker], List[MultiPath]())) {
                  case ((prevTracker: Option[Tracker], tripleList), t) =>
                    val (elapsedTime: Long, distances: List[Float]) = prevTracker match {
                      case Some(p) =>
                        val distList: List[Float] = distanceData.getOrElse(p.gate + ":" + t.gate, List.empty[Float])

                        (t.unixTimestamp - p.unixTimestamp, distList)

                      case None => (0: Long, List.empty[Float])
                    }
                    val distSpeeds = distances.map(d=> DistSpeed(d, d * 60 * 60 /elapsedTime))
                    (Some(t), MultiPath(t.dateTime, t.gate, elapsedTime, distSpeeds)::tripleList)
                }

                val endTime = lastTrackerOpt match {
                  case Some(t) => t.unixTimestamp
                  case None => 0
                }
                val speedLimit: Float = 25

                val timedPath = timeGateElapsedTriple.map {
                  case ((ds: MultiPath)) =>
                    val (speed, dist) = ds.distSpeed.foldLeft((Float.MaxValue, 0: Float)) {
                      case ((s, d), ds: DistSpeed) =>
                        if ((ds.speed - speedLimit) < (s - speedLimit)) (ds.speed, ds.distance) else (s, d)
                    }
                    Path(ds.timestamp, ds.gate, ds.timeElapsed, dist, if (speed != Float.MaxValue) speed else 0)
                }

                val sortedTimedPath = timedPath.reverse
                val pathList = sortedTimedPath.map(_.gate)
                val path = pathList.mkString(":")
                val pathRev = pathList.reverse.mkString(":")
                var hash = 1
                if(path > pathRev) {
                  hash = path.hashCode() + pathRev.hashCode() * 31
                } else {
                  hash = pathRev.hashCode() + path.hashCode() * 31
                }
                CompleteRecord(carId, carType, date, endTime - startTime, sortedTimedPath, path, pathRev, hash, findDayOfWeek(asLocalDate(date))) :: tripList
            }
    }

    tripRecords
  }

  def findDayOfWeek(ts: String) = {
    asLocalDateTime(ts).getDayOfWeek match {
      case DayOfWeek.SUNDAY => "sunday"
      case DayOfWeek.MONDAY => "monday"
      case DayOfWeek.TUESDAY => "tuesday"
      case DayOfWeek.WEDNESDAY => "wednesday"
      case DayOfWeek.THURSDAY => "thursday"
      case DayOfWeek.FRIDAY => "friday"
      case DayOfWeek.SATURDAY => "saturday"
    }
  }

  def findDayOfWeek(ts: LocalDate) = {
    ts.getDayOfWeek match {
      case DayOfWeek.SUNDAY => "sunday"
      case DayOfWeek.MONDAY => "monday"
      case DayOfWeek.TUESDAY => "tuesday"
      case DayOfWeek.WEDNESDAY => "wednesday"
      case DayOfWeek.THURSDAY => "thursday"
      case DayOfWeek.FRIDAY => "friday"
      case DayOfWeek.SATURDAY => "saturday"
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Vast Challenge 2017")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val sensorData: DataFrame = spark.read.option("header", true).csv(DATA_FILE)

   /* val sensorDataWithDayOfWeek = sensorData.map{
      row =>
        val ts = row.getAs[String]("Timestamp")
        SensorData(ts, row.getAs[String]("car-id"), row.getAs[String]("car-type"), row.getAs[String]("gate-name"), findDayOfWeek(ts))
    } (Encoders.product)

    sensorDataWithDayOfWeek.coalesce(1)
        .write.option("header", true)
        .csv("outputJun15/sensorDataWithDayOfWeek")*/

//    val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
    /*  writeTripRecord(sensorData).coalesce(1)
          .write.json("output/tripRecords")*/

    /*writePathRecord(spark, sensorData).coalesce(1)
        .write.json("output/pathRecords")*/

    /*
    val pathRecords = writePathRecord(spark, sensorData)

    pathRecords.coalesce(1).write.json("output/pathRecords")

    val singleDayPathRecords = writePathStringRecord(spark, sensorData)
    singleDayPathRecords.coalesce(1)
        .write.option("header", true)
        .csv("output/singleDayPathStringRecords")

    singleDayPathRecords.groupBy(col("path")).count().coalesce(1)
        .write.option("header", true)
        .csv("output/dailyPatternCount")

    val multipleDaysPathRecords = writeMultipleDayPathStringRecord(spark, sensorData)
    multipleDaysPathRecords.coalesce(1)
        .write.option("header", true)
        .csv("output/multipleDayPathRecord")

    multipleDaysPathRecords.filter(col("daysSpan") > 1).coalesce(1)
        .write.option("header", true)
        .csv("output/multipleDayPathRecordFiltered")

    multipleDaysPathRecords.groupBy(col("path")).count().coalesce(1)
        .write.option("header", true)
        .csv("output/multipleDayPatternCountFiltered")

    multipleDaysPathRecords.filter(col("daysSpan") > 1).groupBy(col("path")).count().coalesce(1)
        .write.option("header", true)
        .csv("output/multipleDayPatternCountFiltered")
  */

    /*val pathRevStringCount = writePathRevStringCount(spark, sensorData)

    pathRevStringCount.show(20, false)
        pathRevStringCount.coalesce(1)
        .write.option("header", true)
        .csv("outputJun15/pathRevStringCount")
*/
    /*writeTripWithElapsedTimeRecord(spark, sensorData).coalesce(1)
        .write.json("outputJun15/tripWithElapsedTimeSortedRecord")*/

    //June 18
    /*writePathRevStringWithHashCount(spark, sensorData)
//        .show(20, false)
        .coalesce(1)
        .write.option("header", true)
        .csv("outputJun18/singleDayPathStringWithHashAndDayOfWeek")*/

    // June 21
    /*val distanceMap: Map[String, List[Float]] = spark.read.option("header", true).csv("data/graphEdgeInfoRenamedCSV.csv")
        .collect().foldLeft(Map[String, List[Float]]()) {
      case (dm, r) =>
        val key = r.getAs[String]("source") + ":" + r.getAs[String]("target")
        val newVal = r.getAs[String]("distance").toFloat :: dm.getOrElse(key, List[Float]())
        dm updated(key, newVal)
    }*/

//    distanceMap.foreach{ case ((k, v: List[Float])) => if (v.length > 1) println(k)}
  /*  writeSuperCompleteRecord(spark, sensorData, distanceMap)
//        .show(20, false)
//        .limit(30)
        .coalesce(1)
        .write.json("outputJun21/superCompleteRecord")*/


    /*writeEstimateCompleteRecord(spark, sensorData, distanceMap)
//                .show(20, false)
        //        .limit(30)
        .coalesce(1)
        .write.json("outputJun21/estimateCompleteRecord")
    */
    spark.stop()
  }


}
