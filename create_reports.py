from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
                        .appName("Create Reports") \
                        .enableHiveSupport() \
                        .getOrCreate()

    # Suponiendo que los datos han sido almacenados en Hive
    df = spark.sql("SELECT * FROM silver_table")

    # Aquí podrías hacer más transformaciones o agregaciones
    df.write.format("parquet").saveAsTable("gold_table")

if __name__ == '__main__':
    main()
