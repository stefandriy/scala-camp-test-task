package example

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.spark_project.guava.base.Strings.isNullOrEmpty

import scala.collection.mutable

object Hello extends App {

  val conf = new SparkConf().setAppName("scala_test").setMaster("local[4]")
  val spark = SparkSession.builder.config(conf).getOrCreate()

  val path = "/path/to/crimes"
  val files = new File(path).list.map(file => path + "/" + file)

  spark.read.option("header", "true").csv(files: _*)
    .filter(row => !isNullOrEmpty(row.getAs[String]("Crime ID")))
    .filter(row => !isNullOrEmpty(row.getAs[String]("Longitude")))
    .filter(row => !isNullOrEmpty(row.getAs[String]("Latitude")))
    .groupBy("Longitude", "Latitude")
    .agg(collect_list(
      struct("Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude", "Location", "LSOA code", "LSOA name", "Crime type", "Last outcome category", "Context")),
      count("*").as("count"))
    .sort(desc("count"))
    .limit(5)
    .foreach(row => {
      println("(" + row.getAs[String]("Longitude") + "," + row.getAs[String]("Latitude") + ")" + ": " + row.getAs[String]("count"))
      row.getAs[mutable.WrappedArray[Row]](2)
        .filter(row2 => row2.getAs[String]("Crime type").toLowerCase().contains("theft"))
        .foreach(row2 => println(row2))
    })
}
