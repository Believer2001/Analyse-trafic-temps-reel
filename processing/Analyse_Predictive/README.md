=================================================
Kafka → Lagging (T-1, T-2) 
      → Graph propagation (GraphX/GraphFrames)
      → Features: [lag1, lag2, neighbor_1hop, neighbor_2hop, centrality]
      → StreamingLinearRegressionWithSGD
      → Dashboard shadow


on doit considerer le temps , et pour ce on doin mettre sont calactr cyclicité  

```python

hour = dt.hour + dt.minute/60
hour_sin = math.sin(2*math.pi*hour/24)
hour_cos = math.cos(2*math.pi*hour/24)

# Ajouter hour_sin et hour_cos comme features dans ton vecteur
features = Vectors.dense([lag1, lag2, lag3, neighbor_congestion, hour_sin, hour_cos])
```


pour la partie modelisation de graph,  j'utilise l'osm pour telechar le graph de la ville de casa 
=================================================





Pour faire de l'Online Learning, on ne peut pas utiliser le GBTRegressor classique (car il ne sait pas apprendre "petit à petit"). On va utiliser un algorithme de Descente de Gradient Stochastique (SGD).

Voici le plan pour ton script Spark :
1. Préparation du "Lagging" (Le passé du trafic)

Le modèle doit savoir si la vitesse sur le Boulevard Zerktouni est en train de baisser ou de monter. On crée donc des colonnes T−1 et T−2.
2. Le Modèle d'Online Learning (SGD)

Dans Spark, on utilise StreamingLinearRegressionWithSGD. Ce modèle a une particularité : il possède une méthode trainOn(data) qui met à jour les poids à chaque micro-batch reçu de Kafka.
3. La Matrice d'Adjacence (Le Graphe de Casa)

Pour que ton IA comprenne que si l'Avenue des FAR est rouge, le Tunnel de la Marina va bientôt l'être, il faut lui donner "la carte" des connexions.

Crée un petit dictionnaire de voisinage pour simuler le graphe :

L'astuce Online : À chaque fois que tu reçois une donnée sur l'Avenue des FAR, tu injectes aussi la valeur de congestion de ses voisins (Tunnel_Marina) comme "Feature" supplémentaire. C'est le début du GNN (Graph Neural Network).
4. Ce que tu vas voir sur ton Dashboard

Une fois ce modèle déployé, tes colonnes Pydeck ne seront plus juste des données passées. Tu pourras afficher :

    Couleur Réelle : La vitesse actuelle lue sur Kafka.

    Ombre (Shadow) : La prédiction du modèle SGD qui s'affine chaque minute.

Ta prochaine étape concrète :

Est-ce que tu veux que je te donne le code complet du script Spark qui fait le "Lagging" et prépare les vecteurs pour le modèle SGD, ou préfères-tu d'abord qu'on définisse manuellement les liens entre tes 10 avenues principales pour construire le graphe ?