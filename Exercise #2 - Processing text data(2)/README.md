 📊 Exercise #2 - Spark Scala: Text Data Processing(2)

This exercise showcases the usage of Apache Spark in Scala for processing text data. It covers various tasks such as loading text files, performing transformations on text data, executing basic data analysis, and combining multiple datasets. Additionally, it demonstrates how to create a Spark session and use SparkContext to work with RDDs (Resilient Distributed Datasets).

Summary

 - Loading Text Files: Two text files, "purplecow.txt" and "union.txt", are loaded into RDDs (Resilient Distributed Datasets) using SparkContext's textFile() method.
 - Text Transformation: The flatMap() transformation is applied to split lines of text into individual words, creating an RDD of words.
 - Data Analysis: Basic data analysis tasks are performed, including retrieving the first element and the top element of the RDD.
 - Combining RDDs: Two RDDs are combined using the union() operation to create a single RDD containing data from both files.