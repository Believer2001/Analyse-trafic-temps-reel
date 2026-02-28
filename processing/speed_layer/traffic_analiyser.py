from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.functions import to_json, struct, year, month, dayofmonth, hour,from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


from dotenv import load_dotenv
import os 


# chargement des variable d environnement 

load_dotenv("/opt/spark/work-dir/.env")

hdfs_path = os.getenv("HDFS_OUTPUT_PATH") 
kafka_serving_topic = os.getenv("TOPIC_OUTPUT")
kafka_bootstrap_servers =os.getenv("KAFKA_SERVER_ADDR_INT")
kafka_input_topic=os.getenv("TOPIC_NAME")



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

print("\n"*3)
print("lecture depuis kafaka")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_input_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.metadata.max.age.ms", "1000") \
    .option("failOnDataLoss", "false") \
    .load()


print("\n"*3)
print(" fin lecture depuis kafaka\n")

# mapping  the colonne 

# On récupère la valeur brute de Kafka et on applique le schéma

df_parsed = raw_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

print("\n"*3)
print(" fin parsing kafaka\n")

result_df = df_parsed.withColumn(
    "congestion_index",
    when(col("free_flow_speed") > 0,
         ((col("free_flow_speed") - col("current_speed")) / col("free_flow_speed")) * 100
    ).otherwise(0)
).withColumn(
    "status",
    when(col("congestion_index") < 20, "Fluide")
    .when(col("congestion_index") < 50, "Modere")
    .otherwise("Embouteillage")
)

 
print("\n"*3)
print(" fin transformation kafaka\n")
# 5. SORTIE CONSOLE UNIQUEMENT (On commente HDFS pour l'instant)

# On utilise un dossier local au conteneur pour le checkpoint



# ==============================
# 6️⃣ Ajout colonnes de partition
# ==============================

result_df = result_df \
    .withColumn("event_time", from_unixtime(col("timestamp"))) \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day", dayofmonth(col("event_time")))


# --- 3. DÉMARRAGE DE LA BATCH LAYER (HDFS) ---

print("\n"*3)
print(" debut ecriture ver kafka  lecture depuis hdfs\n")
query_hdfs = result_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_path) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_hdfs") \
    .partitionBy("year", "month", "day") \
    .trigger(processingTime='3 minute') \
    .start()

# --- 4. DÉMARRAGE DE LA SERVING LAYER (KAFKA) ---
# Kafka attend une colonne "value" de type String (généralement du JSON)

serving_df = result_df.select(
    to_json(struct("*")).alias("value")
)
print("\n"*3)
print(" debut ecriture ver kafka  lecture depuis kafaka\n")
query_kafka_serving = serving_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_serving_topic) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_kafka_serving") \
    .start()
# TRÈS IMPORTANT : Cette ligne empêche le script de s'arrêter
spark.streams.awaitAnyTermination()