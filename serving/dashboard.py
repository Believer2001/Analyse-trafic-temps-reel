import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import plotly.express as px
import pydeck as pdk

# Configuration de la page
st.set_page_config(page_title="Casablanca Smart Traffic", layout="wide")
st.title("🚗 Dashboard Trafic Casablanca (Temps Réel)")

# Initialisation du Consumer Kafka
# Note : 'broker1:9092' est correct si le dashboard est dans le réseau Docker
@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'traffic-serving',
        bootstrap_servers=['broker1:9092'], 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='dashboard-viewer-casablanca'
    )

consumer = get_kafka_consumer()

# --- INITIALISATION DES PLACEHOLDERS (Hors de la boucle) ---
# Cela réserve les espaces une seule fois au chargement
kpi_row = st.empty()
map_row = st.empty()
chart_row = st.empty()

# Stockage des données
if 'data_history' not in st.session_state:
    st.session_state.data_history = []

def get_status_color(status):
    if status == "Embouteillage":
        return [220, 20, 60, 160]
    elif status == "Modere":
        return [255, 165, 0, 160]
    return [34, 139, 34, 160]

# Boucle de lecture Kafka
for message in consumer:
    record = message.value
    st.session_state.data_history.append(record)
    
    if len(st.session_state.data_history) > 50:
        st.session_state.data_history.pop(0)
    
    df = pd.DataFrame(st.session_state.data_history)
    
    # Prétraitement
    df[['lat', 'lon']] = df['coords'].str.split(',', expand=True).astype(float)
    df['color'] = df['status'].apply(get_status_color)

    # 1. Mise à jour des KPIs
    with kpi_row.container():
        col1, col2, col3 = st.columns(3)
        avg_congestion = df['congestion_index'].mean()
        jam_count = len(df[df['status'] == 'Embouteillage'])
        
        col1.metric("Congestion Moyenne", f"{avg_congestion:.1f}%")
        col2.metric("Points Critiques", jam_count)
        col3.metric("Dernière Avenue", record['avenue'])

    # 2. Mise à jour de la Carte Pydeck
    with map_row.container():
        st.subheader("📍 Analyse Spatiale 3D (Congestion)")
        
        layer = pdk.Layer(
            "ColumnLayer",
            df,
            get_position=['lon', 'lat'],
            get_elevation="congestion_index",
            elevation_scale=50,
            radius=150,
            get_fill_color="color",
            pickable=True,
        )

        view_state = pdk.ViewState(
            latitude=33.5731, longitude=-7.5898, zoom=11, pitch=45
        )

        # On utilise un ID de deck fixe
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{avenue}: {congestion_index}%"}
        ))

    # 3. Mise à jour du Graphique Plotly
    with chart_row.container():
        fig = px.bar(
            df.sort_values('current_speed').head(10), 
            x='current_speed', 
            y='avenue', 
            orientation='h',
            title="Top 10 Avenues les plus lentes (km/h)",
            color='status',
            color_discrete_map={'Fluide':'green', 'Modere':'orange', 'Embouteillage':'red'}
        )
        # AJOUT CRUCIAL : Une 'key' fixe pour éviter l'erreur DuplicateElementId
        st.plotly_chart(fig, use_container_width=True)