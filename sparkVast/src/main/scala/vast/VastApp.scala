package main.scala.vast

import java.time._
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by atuladhar on 6/4/17.
  */
object VastApp {
  val DATA_FILE = "data/lekagulSensorData.csv"
  val PATH_STRING_FILE = "data/pathString.json"
  val CAR_FULL_PATH_FILE = "data/carWiseFullPath.csv"

  case class Tracker(unixTimestamp: Long, dateTime: String, date: String, gate: String, carType: String = "")

  case class TripRecord(carId: String, date: String, timedPath: List[(String, String)])

  case class PathRecord(carId: String, date: String, path: List[String])

  case class PathStringRecord(carId: String, date: String, path: String)

  case class MultiDayPathRecord(carId: String, daysSpan: Int, path: String)

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
            .mapValues(l => l.sortBy(_.unixTimestamp)
                .map(e => (e.dateTime, e.gate)))
            .foldLeft(List[TripRecord]()) {
              (tripList, v) =>
                TripRecord(carId, v._1, v._2) :: tripList
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

    tripRecords.coalesce(1).write.json("output/tripRecords")
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
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp)
                  .map(e => e.gate))
              .foldLeft(List[PathRecord]()) {
                (tripList, v) =>
                  PathRecord(carId, v._1, v._2) :: tripList
              }
        }
    pathRecords.coalesce(1).write.json("output/pathRecords")
  }

  private def writePathStringRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp)
                  .map(e => e.gate))
              .foldLeft(List[PathStringRecord]()) {
                (tripList, v) =>
                  PathStringRecord(carId, v._1, v._2.mkString(":")) :: tripList
              }
        }
    pathStringRecords.coalesce(1).write.json("output/pathStringRecords")
  }

  /*private def writeMultipleDayPathStringRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          val groupedByDate = rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
          if (groupedByDate.size > 1) {
            PathStringRecord(carId, "", groupedByDate.mapValues(l => l.sortBy(_.unixTimestamp).map(_.gate)).mkString(":"))
          } else {
            PathStringRecord("", "", "")
          }
        }
    pathStringRecords.filter(col("carId").notEqual("")).select(col("carId"), col("path")).coalesce(1).write.csv("output/multipleDayPathStringRecordsFiltered")
  }*/

  private def writeMultipleDayPathStringRecord(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._

    val pathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          PathStringRecord(carId, "", rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.sortBy(_.unixTimestamp)
              .map(e => e.gate).mkString(":"))
        }
    pathStringRecords.select(col("carId"), col("path")).coalesce(1).write.csv("output/multipleDayPathStringRecords")
  }

  /* private def writeMultipleDayPathStringRecord2(spark: SparkSession, sensorData: DataFrame) = {
     import spark.implicits._
     val multiDayPathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
         .mapGroups { (carId: String, rowList: Iterator[Row]) =>
           (rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
             val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
             Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
           }.groupBy(_.date)
               .mapValues(l => l.sortBy(_.unixTimestamp))
               .foldLeft((0, List[String]())) {
                 (tripList, v) =>
                   (tripList._1 + 1, tripList._1 + v._2.mkString(":") :: tripList._2)
               }._2.mkString("#")
           )
         }

     multiDayPathStringRecords.coalesce(1).write.json("output/multiPathNew")
   }*/
  private def writeMultipleDayPathStringRecord2(spark: SparkSession, sensorData: DataFrame) = {
    import spark.implicits._
    val multiDayPathStringRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          val (count, pathList) = rowList.foldLeft(List[Tracker]()) { (resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp).map(_.gate))
              .foldRight((0, List[String]())) {
                (v, tripList) =>
                  (tripList._1 + 1, tripList._1 + v._2.mkString(":") :: tripList._2)
              }
          MultiDayPathRecord(carId, count, pathList.mkString("#"))
        }

//    multiDayPathStringRecords.coalesce(1).write.json("output/multiPathNew")
    multiDayPathStringRecords.filter(col("daysSpan") > 1).coalesce(1).write.csv("output/multiPathNewFilteredRight")
    multiDayPathStringRecords.coalesce(1).write.csv("output/multiPathNewRight")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Vast Challenge 2017")
        .master("local[*]")
        .getOrCreate()

    val sensorData: DataFrame = spark.read.option("header", true).csv(DATA_FILE)

    //    writeTripRecord(sensorData)
    //    writePathRecord(spark, sensorData)
    //    writePathStringRecord(spark, sensorData)
    //    writeMultipleDayPathStringRecord(spark, sensorData)

    //    val dailyPatternData: DataFrame = spark.read.json(PATH_STRING_FILE)
    //    dailyPatternData.groupBy(col("path")).count().coalesce(1).write.csv("output/dailyPattern")

    //    val multipleDayPatternData: DataFrame = spark.read.option("header", true).csv(CAR_FULL_PATH_FILE)
    //    multipleDayPatternData.groupBy(col("path")).count().coalesce(1).write.csv("output/multidayPatternFiltered")
//        writeMultipleDayPathStringRecord2(spark, sensorData)

//    val dailyPatternData: DataFrame = spark.read.json(PATH_STRING_FILE)
//    dailyPatternData.groupBy(col("carId")).agg(countDistinct("date")).coalesce(1).write.csv("output/groupedDateCount")

    //multiple day pattern using multipathNewFilteredCSV
    val multipleDayPattern = spark.read.option("header", true).csv("data/multipathNewFilteredRight.csv")
    multipleDayPattern.groupBy(col("path")).count().coalesce(1).write.csv("output/multipleDayPatternNewRight")

    spark.stop()
  }
}
