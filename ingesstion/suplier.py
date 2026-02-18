import requests
import json
import time
from kafka import KafkaProducer
import os 
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---


API_KEY =os.getenv("API_KEY")
KAFKA_SERVER =os.getenv("KAFKA_SERVER")
TOPIC_NAME = os.getenv("TOPIC_NAME")
print(KAFKA_SERVER)
# Dictionnaire des points stratégiques (Exemple : Casablanca)
CITY_POINTS = {
    "Avenue_FAR": "33.5951,-7.6062",
    "Boulevard_Zerktouni": "33.5867,-7.6258",
    "Route_de_Nouaceur": "33.5352,-7.6201",
    "Avenue_2_Mars": "33.5689,-7.6085",
    "Corniche": "33.5992,-7.6541"
}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_traffic_for_point(name, coords):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={API_KEY}&point={coords}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get('flowSegmentData', {})
            # On construit un message enrichi avec le nom de l'avenue
            return {
                "avenue": name,
                "coords": coords,
                "timestamp": time.time(),
                "current_speed": data.get("currentSpeed"),
                "free_flow_speed": data.get("freeFlowSpeed"),
                "confidence": data.get("confidence")
            }
    except Exception as e:
        print(f"Erreur sur {name}: {e}")
    return None

# --- BOUCLE PRINCIPALE ---
print(f"Surveillance de {len(CITY_POINTS)} points en cours...")
try:
    while True:
        for name, coords in CITY_POINTS.items():
            traffic_info = get_traffic_for_point(name, coords)
            if traffic_info:
                producer.send(TOPIC_NAME, value=traffic_info)
                print(f"[{name}] Envoyé : {traffic_info['current_speed']} km/h")
        
        # On attend 3 minutes (180 secondes) avant la prochaine vague
        # 5 points x 20 fois/heure = 100 requêtes/heure
        # 100 x 24h = 2400 requêtes/jour (Limite gratuite TomTom : 2500)
        print("En attente de la prochaine mise à jour...")
        time.sleep(180) 

except KeyboardInterrupt:
    print("Arrêt.")
finally:
    producer.close()