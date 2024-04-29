package scalaspark

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Set log level to ERROR
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("pruebaScalaSpark1") // Set application name
      .master("local[*]") // Set local execution mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Set Spark driver configuration
      .config("spark.log.level", "ERROR") // Set Spark log level to ERROR
      .getOrCreate()

    // Load user data from text file
    val users = spark.sparkContext.textFile("data/users.txt")

    // Load postal codes data from text file
    val postalcodes = spark.sparkContext.textFile("data/postalcodes.txt")

    val wordsCount = spark.sparkContext.textFile("data/words.txt")

    // Map user data to extract name and last name
    val mappedUsers = users.map(line => {
      val fields = line.split("\\s+")
      val name = fields.head
      val lastName = fields.tail.mkString(" ") // Concatenate remaining fields as last name
      (name, lastName)
    })

    // Print mapped users
    println("Mapped users:")
    mappedUsers.collect().foreach(println)

    // Split each line of postal codes data into postal code, latitude, and longitude
    val separatedData = postalcodes.map(line => {
      val fields = line.split("\\s+")
      val postalCode = fields(0)
      val latitude = fields(1)
      val longitude = fields(2)
      (postalCode, (latitude, longitude))
    })

    // Print postal codes with latitude and longitude
    println("Postal codes separated with Latitude and Longitude:")
    separatedData.collect().foreach(println)

    // Count words in the words data
    val wordCount = wordsCount.flatMap(_.split("\\s+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    // Print word count
    println("Word count:")
    wordCount.collect().foreach(println)
  }
}
