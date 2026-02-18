# Utiliser une image Python légère
FROM python:3.9-slim

# Répertoire de travail
WORKDIR /app

# Installation des dépendances
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copier le code du dashboard
COPY dashboard.py .

# Exposer le port par défaut de Streamlit
EXPOSE 8501

# Lancer l'application
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]