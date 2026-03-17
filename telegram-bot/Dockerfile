# ─── Stage base ───────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Variabili d'ambiente di sistema
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FIREBASE_SERVICE_ACCOUNT_PATH=/app/serviceAccount.json

WORKDIR /app

# Installa dipendenze prima del codice (sfrutta layer cache di Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice del bot
COPY . .

# Il serviceAccount.json NON è nel repo — viene iniettato come variabile
# d'ambiente FIREBASE_SERVICE_ACCOUNT_JSON al runtime (vedi sotto).
#
# Railway / Render lo gestiscono così:
#   1. Aggiungi la variabile d'ambiente FIREBASE_SERVICE_ACCOUNT_JSON
#      con il contenuto del file JSON come stringa
#   2. Lo script di entrypoint scrive il file prima di avviare il bot
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
