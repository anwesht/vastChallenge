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

//  case class Record(timestamp: LocalDateTime, carId: String, carType: String, gateName: String)
  case class Record(timestamp: String, carId: String, carType: String, gateName: String)
//  case class Records (carId: String, trips: Array[Record])
  case class Records (carId: String, trips: Map[String, List[Record]])
  case class TestRecord(test: Map[String, List[(String, String)]])
  case class TestRecord1(carId: String, stops: List[(String, String)])

  case class Tracker(unixTimestamp: Long, dateTime: String, date: String, gate: String)
  case class TrackerRecord(track: Map[String, List[Tracker]])
  case class TrackerRecord1(track: Map[String, List[String]])

  def asLocalDateTime (d: String): LocalDateTime = {
    LocalDateTime.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def asUnixTimestamp (d: LocalDateTime): Long = {
    d.toEpochSecond(ZoneOffset.UTC)
  }

  def asDateTime (d: LocalDateTime): String = {
    d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def asDate (d: LocalDateTime): String = {
    d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Vast Challenge 2017")
        .master("local[2]")
        .getOrCreate()
    import spark.implicits._

    val sensorData: DataFrame = spark.read.option("header", true).csv(DATA_FILE)
        .withColumn("path", col("gate-name"))
        .withColumn("isFirst", lit(false))

    sensorData.printSchema()
    sensorData.filter(col("gate-name").startsWith("entrance") and col("car-type").equalTo("6")).groupBy("gate-name").count.show

    //    grouping test
    /*val grouped = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups { (carType, rowList) =>
          var p = ""
          var isIn = false
          Iterator(rowList.map{ r =>
            if (isIn && r.getAs[String]("gate-name").startsWith("entrance")) {
              isIn = !isIn
              p += r.getAs[String]("gate-name")
              Record(r.getAs[String]("Timestamp"), r.getAs[String]("car-id"), r.getAs[String]("car-type"), r.getAs[String]("gate-name"), p, false )
            } else {
              Record(r.getAs[String]("Timestamp"), r.getAs[String]("car-id"), r.getAs[String]("car-type"), r.getAs[String]("gate-name"), p, false )
            }
          }.toArray)
        }*/

   /* val groupedByCarId = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId, rowList) =>
          TestRecord(rowList.foldLeft(List[(String, String)]()) {(resList, row: Row) =>
            (asDate(row.getAs[String]("Timestamp")), row.getAs[String]("gate-name")) :: resList
          }.groupBy(_._1))
        }*/

      /*val groupedByCarId = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId, rowList) =>
          TestRecord1(carId, rowList.foldLeft(List[(String, String)]()) {(resList, row: Row) =>
            (asDate(row.getAs[String]("Timestamp")), row.getAs[String]("gate-name")) :: resList
          })
        }*/

//      case class Tracker(unixTimestamp: Long, dateTime: String, date: String, gate: String)

    /*val groupedByCarId = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) {(resList, row: Row) =>
              val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name") ) :: resList
          }.groupBy(_.date).mapValues(l => l.sortBy(_.unixTimestamp))
        }*/
      val groupedByCarId = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carId: String, rowList: Iterator[Row]) =>
          TrackerRecord1(rowList.foldLeft(List[Tracker]()) {(resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name") ) :: resList
          }.groupBy(_.date).mapValues(l => l.sortBy(_.unixTimestamp).map(_.gate))
          )
        }

//    val dailyTripData = groupedByCarId.collect()

    groupedByCarId.show(20, false)

    spark.stop()
    /*val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))  */
  }

  //  def findTrips(I)
}
