package scalaspark

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Set log level to ERROR
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("pruebaScalaSpark1")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.log.level", "ERROR") // Set Spark log level to ERROR
      .getOrCreate()

    // Load the text files
    val mydata = spark.sparkContext.textFile("data/purplecow.txt")
    val mydata2 = spark.sparkContext.textFile("data/union.txt")

    // Apply FlatMap transformation to split lines into words
    val wordsRDD = mydata.flatMap(_.split(" "))
    println(wordsRDD.collect().mkString(", "))

    // Retrieve the first element of the RDD
    val firstElement = mydata.first()
    println(firstElement)

    // Retrieve the top element of the RDD
    val topElements = mydata.top(1)
    println(topElements.mkString(", "))

    // Combine two RDDs using Union
    val combinedRDD = mydata.union(mydata2)
    println(combinedRDD.collect().mkString(", "))

    // Retrieve the top 2 elements from the combined RDD
    println(combinedRDD.top(2).mkString(", "))
  }
}
