package de.philippbrunenberg.sparkvideocourse

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, concat, current_timestamp, expr, lit, max, row_number, year}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("spark1") // Application name
      .master("local[*]") // Local execution mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Spark driver configuration
      .getOrCreate()

    // Read the CSV file and load it into a DataFrame
    val df: DataFrame = spark.read
      .option("header", value = true) // Specify that the first row is the header
      .option("inferSchema", value = true) // Infer the schema of the data
      .csv("data/AAPL.csv") // Path of the CSV file

    // Show the first few rows of the DataFrame and its schema
    df.show()
    df.printSchema()

    // Define a column representing the "Open" column
    val column = df("Open")

    // Create a new column that adds 2.0 to the "Open" column
    val newColumn = (column + 2.0).as("OpenIncreasedBy2")

    // Convert the "Open" column to StringType and rename it to "OpenAsString"
    val columnString = column.cast(StringType).as("OpenAsString")

    // Create a column with a constant value of 2.0
    val litColumn = lit(2.0)

    // Create a new column that concatenates the "OpenAsString" column with the string "Hello World"
    val newColumnString = concat(columnString, lit("Hello World"))

    // Select the relevant columns of the DataFrame and display the result
    df.select(column, newColumn, columnString, litColumn, newColumnString)
      //      .filter(newColumn > 2.0)
      //      .filter(newColumn > column)
      //      .filter(newColumn === column)
      .show(truncate = false) // Show the DataFrame without truncating the columns

    // Expression to get the current timestamp and convert it to String type
    val timestampFromExpressions = expr("cast(current_timestamp() as string) as timestampExpression")

    // Get the current timestamp using Spark SQL functions and cast it to String type, with alias "timestampFunctions"
    val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFunctions")

    // Select and show the columns "timestampFromExpressions" and "timestampFromFunctions" of the DataFrame
    df.select(timestampFromExpressions, timestampFromFunctions).show()

    // Select and show the date as string, the value of Open incremented by 1.0, and the current timestamp
    df.selectExpr("cast(Date as string)", "Open+ 1.0", "current_timestamp()").show()

    // Create a temporary view of the DataFrame
    df.createTempView("df")

    // Execute a SQL query on the temporary view and show the result
    spark.sql("select * from df").show()

    // List of renamed columns
    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    // Select and show the renamed columns of the DataFrame
    df.select(renameColumns: _*).show()

    // Select and show all columns with lowercase names
    df.select(df.columns.map(c => col(c).as(c.toLowerCase())): _*).show()

    // Select renamed columns, calculate the difference between 'close' and 'open',
    // filter for those records where 'close' is more than 10% greater than 'open', and print the result

    val stockData = df.select(renameColumns: _*)
//      .withColumn("diff", col("close") - col("open"))
//      .filter(col("close") > col("open") * 1.1)
//    stockData.show()

    import spark.implicits._
    stockData
      .groupBy(year($"date").as("year"))
      .agg(max($"close").as("maxClose"), avg($"close").as("avgClose"))
      .sort($"maxClose".desc)
      .show()

    stockData
      .groupBy(year($"date").as("year"))
      .max("close", "high")
      .show()

    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    stockData
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
      .explain(extended = true)
  }
}
