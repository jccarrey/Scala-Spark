package scalaspark

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("pruebaScalaSpark1") // Set the application name
      .master("local[*]") // Set local execution mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Set Spark driver configuration
      .getOrCreate()

    // Load the text file
    val mydata = spark.sparkContext.textFile("data/purplecow.txt")

    // Count the number of lines in the file
    val count = mydata.count()

    // Print the result
    println("The number of lines in the file is: " + count)

    // Take the first 2 elements of the RDD and convert it to an array
    val take = mydata.take(2)

    // Convert the array to a comma-separated string and then print it
    println(take.mkString(", "))

    // Collect all elements of the RDD and convert it to an array
    val collect = mydata.collect()

    // Convert the array to a comma-separated string and then print it
    println(collect.mkString(", "))

    // Convert all words to uppercase
    val uppercaseData = mydata.map(_.toUpperCase())

    // Collect all elements of the RDD and convert them to uppercase
    val collectUpper = uppercaseData.collect()

    // Convert the array to a comma-separated string and then print it
    println(collectUpper.mkString(", "))

    // Filtrar las frases que comienzan con "I"
    val filteredData = uppercaseData.filter(_.startsWith("I"))

    val collectFiltered = filteredData.collect()
    println(collectFiltered.mkString(", "))
  }
}
