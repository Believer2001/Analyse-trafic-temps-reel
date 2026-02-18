from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Initialisation de la session Spark
spark = SparkSession.builder.appName("TrafficModelTraining").getOrCreate()

# 2. Chargement des données historiques depuis Hadoop
# On suppose que Spark Streaming a enregistré les données en format Parquet
history_df = spark.read.parquet("hdfs://localhost:9000/traffic_history/")

# 3. Préparation des données (Feature Engineering)
# Le modèle a besoin de colonnes numériques pour comprendre
# Imaginons que nous extrayons l'heure et le jour du timestamp
from pyspark.sql.functions import hour, dayofweek
data_cleaned = history_df.select(
    hour("timestamp").alias("hour"),
    dayofweek("timestamp").alias("day_of_week"),
    "current_speed",
    "free_flow_speed",
    "label_speed_30_min" # C'est la vitesse qu'on a observée 30min plus tard
)

# 4. Assemblage des caractéristiques (Features)
# Spark MLlib exige que toutes les entrées soient dans une seule colonne "features"
assembler = VectorAssembler(
    inputCols=["hour", "day_of_week", "current_speed", "free_flow_speed"],
    outputCol="features"
)
output = assembler.transform(data_cleaned)

# 5. Création et entraînement du modèle Random Forest
rf = RandomForestRegressor(featuresCol="features", labelCol="label_speed_30_min")
model = rf.fit(output)

# 6. Sauvegarde du modèle sur HDFS pour que le script Streaming puisse l'utiliser
model.write().overwrite().save("hdfs://localhost:9000/models/traffic_prediction_model")

print("Modèle entraîné et sauvegardé avec succès sur HDFS !")