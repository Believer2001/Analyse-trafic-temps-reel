import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

st.title("🚦 Dashboard Trafic Temps Réel")

# Connexion à Kafka (le nom 'kafka' correspond au nom du service dans Docker Compose)
consumer = KafkaConsumer(
    'traffic-predictions',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Placeholder pour les données
placeholder = st.empty()

for message in consumer:
    data = message.value
    with placeholder.container():
        st.metric(label=f"Avenue: {data['avenue']}", 
                  value=f"{data['current_speed']} km/h", 
                  delta=f"Prédit: {data['predicted_speed_30_min']} km/h")
        
        # Affichage d'une alerte si la prédiction est mauvaise
        if data['predicted_speed_30_min'] < (data['current_speed'] * 0.5):
            st.error(f"⚠️ Alerte Congestion Majeure sur {data['avenue']} dans 30 min !")