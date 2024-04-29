 📊 Exercise #3 - Pair RDDs, MapReduce

This exercise utilizes Apache Spark to process text data. It loads three text files, performs transformations, and conducts basic data analysis tasks. The code extracts user data, separates postal codes, and counts words, demonstrating Spark's capabilities for text processing and analysis.

Summary

 - Loading Data: The code loads three text files, "users.txt", "postalcodes.txt", and "words.txt", into Spark RDDs (Resilient Distributed Datasets) using SparkContext's textFile() method.
 - Text Transformation: It applies transformations like map and flatMap to perform data processing tasks. For example, it maps user data to extract names and last names, and it splits postal codes data into postal code, latitude, and longitude.
 - Data Analysis: Basic data analysis tasks are conducted, such as printing mapped user data, separating postal codes with latitude and longitude, and counting words in the words data.
 - Spark Session Creation: A Spark session is created with specific configurations like setting the application name and the log level to ERROR. This session is used for Spark job execution.