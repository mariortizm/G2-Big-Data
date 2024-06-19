import socket
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def receive_events_from_server(host='localhost', port=50014, event_interval=30):
    # Crear una sesión de Spark
    spark = SparkSession.builder \
        .appName("HDFS Event Receiver") \
        .getOrCreate()
    
    # Definir el esquema de los datos
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("date", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("quantity_products", IntegerType(), True),
        StructField("order_id", StringType(), True)
    ])

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
                        print("Received event:", event)
                        
                        # Convertir el evento en un DataFrame y guardarlo en HDFS
                        df = spark.createDataFrame([event], schema)
                        hdfs_output_dir = "hdfs://172.17.0.2:9000/bronze_data"
                        df.write.mode("append").parquet(hdfs_output_dir, compression="gzip")
                        print("Saved event to HDFS.")
                    
                    except json.JSONDecodeError:
                        # No hay suficientes datos para decodificar un objeto JSON completo
                        break
                
                # Espera el intervalo de tiempo antes de procesar el siguiente evento
                time.sleep(event_interval)
                    
        except Exception as e:
            print(f"An error occurred: {e}")

    spark.stop()

if __name__ == '__main__':
    # Define el intervalo de eventos (en segundos)
    event_interval = 30

    # Recibir eventos y obtener la sesión de Spark
    receive_events_from_server(event_interval=event_interval)
