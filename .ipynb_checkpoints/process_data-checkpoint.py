from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
                        .appName("Process Data") \
                        .getOrCreate()

    df_stream = spark.readStream \
                      .format("socket") \
                      .option("host", "localhost") \
                      .option("port", 50000) \
                      .load()

    df_transformed = df_stream.selectExpr("CAST(value AS STRING) as raw_data") \
                               .select(
                                   # Agregar procesamiento necesario aquí
                               )

    query = df_transformed.writeStream \
                           .outputMode("append") \
                           .format("console") \
                           .start()

    query.awaitTermination()

if __name__ == '__main__':
    main()
