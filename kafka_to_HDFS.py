from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.types import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

kafka_topic = "CUSTOMER_DATA"
kafka_server = "localhost:9092"
csv_file = "D:\\study_materIAL\\cpe\\salesdataset\\AdventureWorksProducts-210509-235702.csv"
output_path = "hdfs://localhost:9000/input_data"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read the CSV file into a DataFrame
    df = spark.read.format("csv").option("header", "true").load(csv_file)
    df.show()

    # Convert DataFrame to JSON and sending dataframe to kafka topic
    df.select(to_json(struct("*")).alias("value"))\
        .selectExpr("CAST(value AS STRING)")\
        .write\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_server)\
        .option('topic', kafka_topic)\
        .save()




    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()


    #Reading the kafka topic data in key string format
    kafka_df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')

    #Defining schema
    product_schema = StructType([
        StructField('ProductKey', IntegerType(), nullable=True),
        StructField('ProductSubcategoryKey', IntegerType(), nullable=True),
        StructField('ProductSKU', StringType(), nullable=True),
        StructField('ProductName', StringType(), nullable=True),
        StructField('ModelName', StringType(), nullable=True),
        StructField('ProductDescription', StringType(), nullable=True),
        StructField('ProductColor', StringType(), nullable=True),
        StructField('ProductSize', StringType(), nullable=True),
        StructField('ProductStyle', StringType(), nullable=True),
        StructField('ProductCost', DoubleType(), nullable=True),
        StructField('ProductPrice', DoubleType(), nullable=True)
    ])



    # Process the streaming DataFrame as needed(converting key value data into dataframe structure using schema)
    processed_df = kafka_df.select(from_json(col("value").cast("string"), product_schema).alias("data")).select("data.*")

    # Write the processed DataFrame to HDFS as JSON format
    query = processed_df \
        .writeStream \
        .outputMode("Append") \
        .format("json") \
        .option("format","append")\
        .option("path", output_path) \
        .option("checkpointLocation", "hdfs://localhost:9000/path/to/checkpoint") \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()
    # stop sparkSession
    spark.stop()
