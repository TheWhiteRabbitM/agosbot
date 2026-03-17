#!/bin/sh
# Entrypoint per il container Docker del bot Telegram.
# Scrive il serviceAccount.json dalla variabile d'ambiente prima di avviare il bot.

set -e

# Metodo 1 (consigliato): base64 — nessun problema di escaping
# Genera il valore con: base64 -w 0 serviceAccount.json  (Linux/Mac)
if [ -n "$FIREBASE_SERVICE_ACCOUNT_B64" ]; then
    printf '%s' "$FIREBASE_SERVICE_ACCOUNT_B64" | base64 -d > /app/serviceAccount.json
    echo "✅ serviceAccount.json scritto da variabile base64"

# Metodo 2 (fallback): JSON grezzo
elif [ -n "$FIREBASE_SERVICE_ACCOUNT_JSON" ]; then
    printf '%s' "$FIREBASE_SERVICE_ACCOUNT_JSON" | python3 -c "
import sys, json
raw = sys.stdin.read()
data = json.loads(raw)
if 'private_key' in data:
    data['private_key'] = data['private_key'].replace('\\\\n', '\n')
with open('/app/serviceAccount.json', 'w') as f:
    json.dump(data, f, indent=2)
"
    echo "✅ serviceAccount.json scritto da variabile JSON"
fi

# Verifica che il file esista
if [ ! -f "/app/serviceAccount.json" ]; then
    echo "❌ serviceAccount.json non trovato."
    echo "   Imposta FIREBASE_SERVICE_ACCOUNT_B64 (consigliato) oppure FIREBASE_SERVICE_ACCOUNT_JSON"
    exit 1
fi

# Verifica che il token sia configurato
if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "❌ TELEGRAM_BOT_TOKEN non configurato."
    exit 1
fi

echo "🤖 Avvio bot..."
exec python bot.py
