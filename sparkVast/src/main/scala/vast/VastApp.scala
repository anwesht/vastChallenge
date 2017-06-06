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

  case class Tracker(unixTimestamp: Long, dateTime: String, date: String, gate: String)
  case class TripRecord(carId: String, date: String, path: List[(String, String)])

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
        .master("local[*]")
        .getOrCreate()
    import spark.implicits._

    val sensorData: DataFrame = spark.read.option("header", true).csv(DATA_FILE)

//    sensorData.printSchema()
//    sensorData.filter(col("gate-name").startsWith("entrance") and col("car-type").equalTo("6")).groupBy("gate-name").count.show

    //    grouping test
    val tripRecords = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .flatMapGroups{ (carId: String, rowList: Iterator[Row]) =>
          rowList.foldLeft(List[Tracker]()) {(resList, row: Row) =>
            val localDateTime = asLocalDateTime(row.getAs[String]("Timestamp"))
            Tracker(asUnixTimestamp(localDateTime), asDateTime(localDateTime), asDate(localDateTime), row.getAs[String]("gate-name") ) :: resList
          }.groupBy(_.date)
              .mapValues(l => l.sortBy(_.unixTimestamp)
                  .map(e => (e.dateTime, e.gate)))
              .foldLeft(List[TripRecord]()) {
                (tripList, v) =>
                  TripRecord(carId, v._1, v._2)::tripList
              }
        }

    println(sensorData.count)

    tripRecords.show(20, false)

    println(tripRecords.getClass)

    println(tripRecords.count)
    println(tripRecords.distinct().count())

    tripRecords.write.json("tripRecordsFlatMapJson")


    spark.stop()
    /*val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))  */
  }
}
