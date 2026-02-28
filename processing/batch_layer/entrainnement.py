from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Initialisation de la Session Spark
spark = SparkSession.builder \
    .appName("Traffic_Batch_ML_Training") \
    .getOrCreate()

# 2. Chargement du Master Dataset depuis HDFS
# Assure-toi que le chemin correspond à celui de ton streaming
master_df = spark.read.parquet("hdfs://namenode:9000/traffic_project/master_dataset")

# 3. Feature Engineering : Création de la "Mémoire" (Time Series Mapping)
# On définit une fenêtre pour regarder le passé de chaque avenue
window_spec = Window.partitionBy("avenue").orderBy("ingestion_time")

df_with_lags = master_df \
    .withColumn("hour", F.hour("ingestion_time")) \
    .withColumn("day_of_week", F.dayofweek("ingestion_time")) \
    .withColumn("vitesse_T_minus_5", F.lag("vitesse_actuelle", 1).over(window_spec)) \
    .withColumn("vitesse_T_minus_10", F.lag("vitesse_actuelle", 2).over(window_spec)) \
    .withColumn("tendance", F.col("vitesse_T_minus_5") - F.col("vitesse_T_minus_10"))

# On retire les lignes NULL créées par le décalage (les débuts de séries)
dataset = df_with_lags.dropna()

# 4. Pipeline de Machine Learning
# A. Indexation de l'avenue (transformation du nom en nombre)
indexer = StringIndexer(inputCol="avenue", outputCol="avenue_index")

# B. Assemblage des caractéristiques (Features)
# On utilise le présent (hour, day) et le passé (lags)
feature_cols = ["avenue_index", "hour", "day_of_week", "vitesse_T_minus_5", "vitesse_T_minus_10", "tendance"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# C. L'algorithme : Gradient Boosted Trees
# Plus puissant que Random Forest pour les séries temporelles tabulaires
gbt = GBTRegressor(featuresCol="features", labelCol="vitesse_actuelle", maxIter=20)

# D. Construction du Pipeline
pipeline = Pipeline(stages=[indexer, assembler, gbt])

# 5. Entraînement
print("Début de l'entraînement du modèle GBT...")
(train_data, test_data) = dataset.randomSplit([0.8, 0.2])
model = pipeline.fit(train_data)

# 6. Évaluation du modèle
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="vitesse_actuelle", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) sur les données de test : {rmse}")

# 7. Sauvegarde du modèle sur HDFS
# Ce modèle sera chargé par ton script de Streaming pour les prédictions live
model_path = "hdfs://namenode:9000/traffic_project/models/gbt_traffic_model"
model.write().overwrite().save(model_path)

print(f"Modèle sauvegardé avec succès sur HDFS à : {model_path}")

spark.stop()