from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel

# 1. Session Spark avec support Kafka
spark = SparkSession.builder \
    .appName("RealTimeTrafficPrediction") \
    .getOrCreate()

# 2. CHARGEMENT DU MODÈLE (Phase B)
# On charge le modèle entraîné précédemment sur Hadoop
model = RandomForestRegressionModel.load("hdfs://localhost:9000/models/traffic_prediction_model")

# 3. Lecture du flux Kafka (Inbound data)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .load()

# 4. Schéma et parsing
schema = StructType([
    StructField("avenue", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("current_speed", DoubleType(), True),
    StructField("free_flow_speed", DoubleType(), True)
])

# Conversion JSON et extraction des caractéristiques (Features)
# Le modèle a besoin des mêmes colonnes que lors de l'entraînement
traffic_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("hour", hour(col("timestamp").cast("timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("timestamp").cast("timestamp")))

# 5. Préparation des données pour le modèle (VectorAssembler)
assembler = VectorAssembler(
    inputCols=["hour", "day_of_week", "current_speed", "free_flow_speed"],
    outputCol="features"
)
# Note : transform() est utilisé ici sur un flux streaming
stream_features = assembler.transform(traffic_stream)

# 6. PRÉDICTION
# Spark applique le modèle Random Forest sur chaque ligne du micro-batch
predictions = model.transform(stream_features)

# 7. Affichage des résultats avec la prédiction
# On affiche la vitesse actuelle VS la vitesse prédite dans 30 min
final_output = predictions.select(
    "avenue", 
    "current_speed", 
    col("prediction").alias("predicted_speed_30_min")
)

# Envoi vers la console (pour démo) ou vers un autre topic Kafka d'alerte
query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()