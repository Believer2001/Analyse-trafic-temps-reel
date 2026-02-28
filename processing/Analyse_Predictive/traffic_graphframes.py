import osmnx as ox
from graphframes import GraphFrame
from pyspark.sql import SparkSession

# Spark
spark = SparkSession.builder.appName("GraphCasaOSM").getOrCreate()

# Télécharger le graphe routier complet
G = ox.graph_from_place("Casablanca, Morocco", network_type="drive")

# Convertir en GeoDataFrames
nodes, edges = ox.graph_to_gdfs(G)

# Exemple : filtrer seulement les routes qui t'intéressent
axes_travailles = [
    "Boulevard Zerktouni",
    "Avenue FAR",
    "Boulevard Mohammed V",
    "Boulevard d'Anfa",
    "Avenue 2 Mars",
    "Corniche"
]

# edges['name'] peut être une liste si plusieurs noms sur le segment
edges_filtered = edges[edges['name'].apply(
    lambda x: any(ax in (x if isinstance(x,list) else [x]) for ax in axes_travailles)
)]

# Vertices correspondants aux edges filtrées
nodes_filtered = nodes.loc[
    nodes.index.isin(edges_filtered['u'].tolist() + edges_filtered['v'].tolist())
]

# Convertir en Spark DataFrames
vertices = spark.createDataFrame(nodes_filtered.reset_index()[['osmid']].rename(columns={'osmid':'id'}))
edges_spark = edges_filtered.reset_index()[['u','v']].rename(columns={'u':'src','v':'dst'})
edges_df = spark.createDataFrame(edges_spark)

# Créer GraphFrame
graph_filtered = GraphFrame(vertices, edges_df)

# Après avoir créé graph_filtered (GraphFrame)
# On va juste extraire les voisins sous forme dict pour broadcast
edges_list = graph_filtered.edges.select("src", "dst").collect()

graph_dict = {}
for row in edges_list:
    if row.src not in graph_dict:
        graph_dict[row.src] = []
    graph_dict[row.src].append(row.dst)

# Sauvegarder dans un fichier JSON pour le récupérer ensuite
import json
with open("/tmp/graph_neighbors.json", "w") as f:
    json.dump(graph_dict, f)

# Vérification
graph_filtered.vertices.show()
graph_filtered.edges.show()