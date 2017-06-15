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

  case class TripRecord(carId: String, carTypes: String, date: String, timedPath: List[(String, String)])

  case class TripWithElapsedRecord(carId: String, carTypes: String, date: String, totalTime: Long, timedPath: List[(String, String, Long)])

  case class PathRecord(carId: String, carType: String, date: String, path: List[String])

  case class PathStringRecord(carId: String, carType: String, date: String, path: String)

  case class PathStringRevRecord(carId: String, carType: String, date: String, path: String, pathRev: String)

  case class MultiDayPathRecord(carId: String, carType: String, daysSpan: Int, path: String)

  case class PathCount(path: String, count: Int)

  case class SensorData(timestamp: String, carId: String, carType: String, gateName: String, dayOfWeek: String)

  def asLocalDateTime(d: String): LocalDateTime = {
    LocalDateTime.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Vast Challenge 2017")
        .master("local[*]")
        .getOrCreate()

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


    spark.stop()
  }
}
