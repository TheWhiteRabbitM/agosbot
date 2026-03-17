#!/bin/sh
# Entrypoint per il container Docker del bot Telegram.
# Scrive il serviceAccount.json dalla variabile d'ambiente prima di avviare il bot.

set -e

# Se FIREBASE_SERVICE_ACCOUNT_JSON è impostata, scrivila su file
if [ -n "$FIREBASE_SERVICE_ACCOUNT_JSON" ]; then
    echo "$FIREBASE_SERVICE_ACCOUNT_JSON" > /app/serviceAccount.json
    echo "✅ serviceAccount.json scritto da variabile d'ambiente"
fi

# Verifica che il file esista (o da variabile o montato come volume)
if [ ! -f "/app/serviceAccount.json" ]; then
    echo "❌ serviceAccount.json non trovato."
    echo "   Imposta FIREBASE_SERVICE_ACCOUNT_JSON oppure monta il file come volume."
    exit 1
fi

# Verifica che il token sia configurato
if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "❌ TELEGRAM_BOT_TOKEN non configurato."
    exit 1
fi

echo "🤖 Avvio bot..."
exec python bot.py
