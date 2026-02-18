from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Session Spark simplifiée
spark = SparkSession.builder \
    .appName("TrafficAnalyzer") \
    .getOrCreate()

# 2. Schéma
schema = StructType([
    StructField("avenue", StringType(), True),
    StructField("coords", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("current_speed", DoubleType(), True),
    StructField("free_flow_speed", DoubleType(), True),
    StructField("confidence", DoubleType(), True)
])

# 3. Lecture Kafka (Utilise broker1:9092 comme vu dans tes logs)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transformations
traffic_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

analyzer_df = traffic_df.withColumn(
    "congestion_index",
    ((col("free_flow_speed") - col("current_speed")) / col("free_flow_speed")) * 100
).withColumn(
    "status",
    when(col("congestion_index") < 20, "Fluide")
    .when(col("congestion_index") < 50, "Modere")
    .otherwise("Embouteillage")
)

# 5. SORTIE CONSOLE UNIQUEMENT (On commente HDFS pour l'instant)
# On utilise un dossier local au conteneur pour le checkpoint
query = analyzer_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_console") \
    .start()

# TRÈS IMPORTANT : Cette ligne empêche le script de s'arrêter
query.awaitTermination()