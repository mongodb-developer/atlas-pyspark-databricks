from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Use environment variable for uri.
load_dotenv()
connection_string: str = os.environ.get("CONNECTION_STRING")

# Create a SparkSession. Ensure you have the mongo-spark-connector included.
my_spark = SparkSession \
    .builder \
    .appName("tutorial") \
    .config("spark.mongodb.read.connection.uri", connection_string) \
    .config("spark.mongodb.write.connection.uri", connection_string) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3") \
    .getOrCreate()

# Create a data frame so you can write to your bookshelf in your Atlas cluster.
add_books = my_spark \
    .createDataFrame([("War and Peace", "Leo Tolstoy", 1867)], ["title", "author", "year"])

add_books.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', connection_string) \
    .option('database', 'bookshelf') \
    .option('collection', 'books') \
    .mode("append") \
    .save() 

# Create a data frame so you can read in your books from your bookshelf.
return_books = my_spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', connection_string) \
    .option('database', 'bookshelf') \
    .option('collection', 'books') \
    .load()

# Show the books in your PySpark shell.
return_books.show()