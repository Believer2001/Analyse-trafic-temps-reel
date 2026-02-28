from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.regression import LabeledPoint, StreamingLinearRegressionWithSGD
from pyspark.mllib.linalg import Vectors
import json
import math
from datetime import datetime

# ==============================
# 1️⃣ Spark Session + Streaming Context
# ==============================

spark = SparkSession.builder \
    .appName("TrafficOnlineLearning") \
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 60)  # micro-batch 60 sec

# ==============================
# 2️⃣ Graphe (voisinage Casa)
# ==============================

import json

# ==============================
# 2️⃣ Charger le graphe broadcast
# ==============================

with open("/tmp/graph_neighbors.json", "r") as f:
    graph_dict = json.load(f)

graph_broadcast = sc.broadcast(graph_dict)

# ==============================
# 3️⃣ Lecture Kafka
# ==============================

kafkaStream = KafkaUtils.createDirectStream(
    ssc,
    ["traffic_topic"],
    {"metadata.broker.list": "localhost:9092"}
)

# ==============================
# 4️⃣ Parsing JSON
# ==============================

def parse_json(record):
    data = json.loads(record[1])
    return (
        data["avenue"],
        datetime.fromtimestamp(data ["timestamp"]), # convertir la date
        float(data["congestion_index"]),
        float(data["current_speed"])
    )

parsed_stream = kafkaStream.map(parse_json)

# ==============================
# 5️⃣ Construction features
# ==============================

# mémoire simple pour lag (demo simplifiée)
state_memory = {}

def propagate_neighbor_congestion(avenue, hops=2):
    visited = set()
    queue = [(avenue, 0)]
    total = 0.0
    count = 0
    while queue:
        node, level = queue.pop(0)
        if node in visited or level > hops:
            continue
        visited.add(node)
        if node in state_memory:
            total += state_memory[node][0]
            count += 1
        for n in graph_broadcast.value.get(node, []):
            queue.append((n, level+1))
    return total / count if count > 0 else 0.0

def build_features(record):
    avenue, heure, congestion, speed = record
    
    # --- Lags ---
    if avenue not in state_memory:
        state_memory[avenue] = [0.0, 0.0, 0.0]  # lag1, lag2, lag3
    lag1, lag2, lag3 = state_memory[avenue]

    # --- Encodage horaire cyclique ---
    hour = heure.hour + heure.minute / 60.0
    hour_sin = math.sin(2 * math.pi * hour / 24)
    hour_cos = math.cos(2 * math.pi * hour / 24)
    
    # --- Voisinage multi-hop ---
    neighbor_congestion = propagate_neighbor_congestion(avenue, hops=2)
    
    # --- Features ---
    features = Vectors.dense([lag1, lag2, lag3, neighbor_congestion, hour_sin, hour_cos])
    
    # --- Label ---
    labeled_point = LabeledPoint(congestion, features)
    
    # --- Mise à jour mémoire ---
    state_memory[avenue] = [congestion, lag1, lag2]
    
    return labeled_point


training_stream = parsed_stream.map(build_features)

# ==============================
# 6️⃣ Modèle SGD
# ==============================

model = StreamingLinearRegressionWithSGD(
    stepSize=0.01,
    numIterations=1
)

model.setInitialWeights(Vectors.zeros(3))

# train online
model.trainOn(training_stream)

# prédiction shadow
predictions = model.predictOn(training_stream.map(lambda lp: lp.features))

predictions.pprint()

# ==============================
# 7️⃣ Lancement
# ==============================

ssc.start()
ssc.awaitTermination()