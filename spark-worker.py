from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
from psycopg2 import Error
from pyspark.sql.types import StructType, StructField, StringType

# Create the Spark Session
spark = SparkSession \
    .builder \
    .appName("weather streaming") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.2") \
    .master("local[*]") \
    .getOrCreate()

# Kafka bootstrap servers
kafka_bootstrap_servers = "broker:29092"

# Define the Kafka schema for weather data
schema_vehicle = StructType([
    StructField("Formatted Date", StringType(), True),
    StructField("Summary", StringType(), True),
    StructField("Precip Type", StringType(), True),
    StructField("Temperature (C)", StringType(), True),
    StructField("Apparent Temperature (C)", StringType(), True),
    StructField("Humidity", StringType(), True),
    StructField("Wind Speed (km/h)", StringType(), True),
    StructField("Wind Bearing (degrees)", StringType(), True),
    StructField("Visibility (km)", StringType(), True),
    StructField("Loud Cover", StringType(), True),
    StructField("Pressure (millibars)", StringType(), True),
    StructField("Daily Summary", StringType(), True)
])



postgres_url = "jdbc:postgresql://172.23.0.3:5432/admindb"
postgres_properties = {
    "user": "admin",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

host = "172.23.0.3"
database = "admindb"
user = "admin"
password = "root"

# Function to check if table exists in PostgreSQL
def table_exists(cursor, table_name):
    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
    cursor.execute(query)
    exists = cursor.fetchone()[0]
    return exists

# Function to create table in PostgreSQL
def create_postgres_table(schema):
    # Connect to PostgreSQL
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            port=5432
        )

        cursor = connection.cursor()

        # Check if table exists
        table_name = "weather_data"
        if table_exists(cursor, table_name):
            print(f"Table '{table_name}' already exists in PostgreSQL")
        else:
            # Define SQL query to create table
            create_table_query = '''
            CREATE TABLE weather_data (
                "Formatted Date" VARCHAR(255),
                "Summary" VARCHAR(255),
                "Precip Type" VARCHAR(255),
                "Temperature (C)" VARCHAR(255),
                "Apparent Temperature (C)" VARCHAR(255),
                "Humidity" VARCHAR(255),
                "Wind Speed (km/h)" VARCHAR(255),
                "Wind Bearing (degrees)" VARCHAR(255),
                "Visibility (km)" VARCHAR(255),
                "Loud Cover" VARCHAR(255),
                "Pressure (millibars)" VARCHAR(255),
                "Daily Summary" VARCHAR(255)
            );
            '''

            # Execute SQL query to create table
            cursor.execute(create_table_query)
            connection.commit()
            print("Table 'weather_data' created successfully in PostgreSQL")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    # finally:
        # Close database connection
        # if connection:
            # cursor.close()
            # connection.close()
            # print("PostgreSQL connection is closed")

# Create PostgreSQL table using schema_vehicle
create_postgres_table(schema_vehicle)

def read_kafka_data(topic, schema):
    # Read data from Kafka topic
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON data from 'value' column based on the provided schema
    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                  .withColumn("data", from_json(col("value"), schema)) \
                  .select("data.*")

    return parsed_df

# Read weather data from Kafka using defined schema
weather_data = read_kafka_data("weatherdata", schema_vehicle)


def write_to_postgres(df, table_name):
    df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_postgres(batch_df, batch_id, table_name)) \
        .start() \
        .awaitTermination()

#Function to write each batch to PostgreSQL
def write_batch_to_postgres(batch_df, batch_id, table_name):
    try:
        print(f"Writing batch {batch_id} to table {table_name}")
        batch_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", table_name) \
            .option("user", postgres_properties["user"]) \
            .option("password", postgres_properties["password"]) \
            .option("driver", postgres_properties["driver"]) \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written to table {table_name} successfully.")
    except Exception as e:
        print(f"Error writing batch {batch_id} to table {table_name}: {e}")

# Write the joined DataFrame to PostgreSQL
write_to_postgres(weather_data, "weather_data")

# Write the parsed data to the console for testing
# query = weather_data.writeStream \
#                     .format("console") \
#                     .outputMode("append") \
#                     .start()

# # Wait for the stream to terminate
# query.awaitTermination()
