<h1 >AANALYSE DE TRAFFIC EN TEMPS-REEL </h1>

<h2>Description du project</h2>
<p>
L'objectif de ce projet est de concevoir un système capable de surveiller et d'analyser les modèles de trafic en temps réel. Dans ce projet, nous utiliserons des flux de trafic en direct ainsi que l'historique des données, ce qui pourra aider à prédire les embouteillages et à recommander des itinéraires alternatifs. L'utilisation efficace de ces données pourra même aider la ville à planifier la modernisation de ses infrastructures.
</p>


<h2>Architecture</h2>
Nous  allons implémenter une architecture classique  Lanbda avec deux couches essentielles au niveau du **Data processing** que sont la couche  : Speed(traitement temps-réel) et la  couche  long-terme ( pour le bacth processing)

1. couche d'ingestion : 
Une API  pour nous fournir les données (l'API TomTom pour sa gratuité)
2. couche de traitement :
- Spark streming : pour la couche speed:
- spark MLlib
3. couche de stockage :  Apache Hadoop pour ke data lake 

<h2>Implémentation</h2>
- La couche Ingestion : 
 Pour  implémenter cette couche, nous allons utiliser   l'API TomTom. Pour cela, nous allons créer un compte   afin de créer une API KEY.

 Pour obtenir les  données , nous allons utiliser la stratégie **Bounding Box**  qui permet d'avoir les   données  de l'ensembles des avenues  se trouvant à l'interieur d'une zone géographique plutôt d'otenir les données     relatives a une seule routes. Pour
 les texte nous allons pas proceder exactement comme  nous l'avons décrit car nous sont  limité par la contrainte de requête jounalière qui est de 2500. A la place , nous allons   fournir la liste des axe princiaux. c'est l'approche multi-point 





 commande :

 ./kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

 # 1. Copie ton script Python dans le conteneur
docker cp .\ton_script_spark.py spark-master:/tmp/

# 2. Entre dans le conteneur
docker exec -it spark-master bash

# 3. Lance le job depuis l'intérieur
./spark-submit /tmp/processing/traffic_analiyser.py


