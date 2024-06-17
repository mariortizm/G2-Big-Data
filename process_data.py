import socket
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def receive_events_from_server(host='localhost', port=50020):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("HDFS Event Receiver") \
        .getOrCreate()

    # Define the schema of the data
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("date", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("quantity_products", IntegerType(), True),
        StructField("order_id", StringType(), True)
    ])

    # List to store the events
    events = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        print(f"Connected to server at {host}:{port}")

        buffer = ""
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                buffer += data.decode('utf-8')

                while True:
                    try:
                        event, index = json.JSONDecoder().raw_decode(buffer)
                        buffer = buffer[index:].strip()
                        events.append(event)
                        print("Received event:", event)
                        
                        # If the list has more than 10 events, save them to HDFS
                        if len(events) >= 10:
                            df = spark.createDataFrame(events, schema)
                            hdfs_output_dir = "hdfs://172.17.0.2:9000/raw_data"
                            df.write.mode("append").parquet(hdfs_output_dir)
                            events.clear()
                            print("Saved 10 events to HDFS.")
                    
                    except json.JSONDecodeError:
                        # Not enough data to decode a complete JSON object
                        break
                    
        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            # Save any remaining events to HDFS
            if events:
                df = spark.createDataFrame(events, schema)
                hdfs_output_dir = "hdfs://172.17.0.2:9000/raw_data"
                df.write.mode("append").parquet(hdfs_output_dir)
                print(f"Saved remaining {len(events)} events to HDFS.")

    return spark

def merge_parquet_files(spark, hdfs_dir, output_file):
    df = spark.read.parquet(hdfs_dir)
    df.coalesce(1).write.mode('overwrite').parquet(output_file)

if __name__ == '__main__':
    # Receive events and obtain Spark session
    spark = receive_events_from_server()

    # Define directories for merging
    hdfs_dir = "hdfs://172.17.0.2:9000/raw_data"
    output_file = "hdfs://172.17.0.2:9000/merged_data"

    # Merge Parquet files
    merge_parquet_files(spark, hdfs_dir, output_file)

    # Stop Spark session
    spark.stop()