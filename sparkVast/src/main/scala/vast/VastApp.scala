package main.scala.vast

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

  case class Record(timestamp: String, carId: String, carType: String, gateName: String, path: String, isFirst: Boolean)
  case class Records (carId: String, trips: Array[Record])

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
   /* val grouped = sensorData.groupByKey(row => row.getAs[String]("car-id"))
        .mapGroups { (carType, rowList) =>
            var p = ""
            var isIn = false
            rowList.foreach{ r =>
              if (isIn && r.getAs[String]("gate-name").startsWith("entrance")) {
                isIn = !isIn
                p += r.getAs[String]("gate-name")
                RowFactory.create(to_utc_timestamp(col("Timestamp"), "MM/dd/yyyy HH:mm:ss"), r.getAs[String]("car-id"), r.getAs[String]("car-type"), p )
              }
            }
        }*/
    /*val grouped = sensorData.groupByKey(row => row.getAs[String]("car-id"))
       .flatMapGroups { (carType, rowList) =>
         var p = ""
         var isIn = false
         Iterator(rowList.map{ r =>
           if (isIn && r.getAs[String]("gate-name").startsWith("entrance")) {
             isIn = !isIn
             p += r.getAs[String]("gate-name")
             RowFactory.create(to_utc_timestamp(col("Timestamp"), "MM/dd/yyyy HH:mm:ss"), r.getAs[String]("car-id"), r.getAs[String]("car-type"), p )
           } else {
             RowFactory.create(to_utc_timestamp(col("Timestamp"), "MM/dd/yyyy HH:mm:ss"), r.getAs[String]("car-id"), r.getAs[String]("car-type"), p )
           }
         }.toArray)
       } */

    // 1 = timestamp, 2 = car-id, 3 = car-type, 4 = gate-name, 5 = path

//    val sensorRecord = sensorData.as[Record]
//    val grouped = sensorRecord.groupByKey(row => row.carId)
    /*val grouped = sensorRecord.groupByKey(row => row.carId)
        .mapGroups { (carType, rowList) =>
          var p = ""
          var isIn = false
          Records(carType, rowList.map{ r =>
            if (isIn && r.gateName.startsWith("entrance")) {
              isIn = !isIn
              p += r.gateName
//              RowFactory.create(to_utc_timestamp(col("Timestamp"), "MM/dd/yyyy HH:mm:ss"), r._2, r._3, p )
              Record(r.timestamp, r.carId, r.carType, r.gateName, p, false )
            } else {
              Record(r.timestamp, r.carId, r.carType, r.gateName, p, false )
            }
          }.toArray)
        }*/

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
        val grouped = sensorData.groupByKey(row => row.getAs[String]("car-id"))
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
          }

    //    grouped.show()
    println(grouped.getClass)

    spark.stop()
    /*val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))  */
  }

//  def findTrips(I)
}
