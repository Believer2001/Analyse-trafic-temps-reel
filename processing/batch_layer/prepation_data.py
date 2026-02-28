from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, minute, floor

spark = SparkSession.builder.appName("BatchLayerProcessing").getOrCreate()

# 1. Chargement de l'historique depuis HDFS
master_df = spark.read.parquet("hdfs://namenode:9000/traffic_project/master_dataset")

# 2. Feature Engineering : transformer le temps en données cycliques
# dayofweek: 1 (Dimanche) à 7 (Samedi)
enriched_df = master_df.withColumn("hour", hour(col("ingestion_time"))) \
                       .withColumn("day_of_week", dayofweek(col("ingestion_time"))) \
                       .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))

# Sauvegarde de la vue analytique (Gold Table)
enriched_df.write.mode("overwrite").parquet("hdfs://namenode:9000/traffic_project/gold_analytics")