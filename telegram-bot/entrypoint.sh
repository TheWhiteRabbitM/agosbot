#!/bin/sh
# Entrypoint per il container Docker del bot Telegram.
# Scrive il serviceAccount.json dalla variabile d'ambiente prima di avviare il bot.

set -e

# Se FIREBASE_SERVICE_ACCOUNT_JSON è impostata, scrivila su file
if [ -n "$FIREBASE_SERVICE_ACCOUNT_JSON" ]; then
    printf '%s' "$FIREBASE_SERVICE_ACCOUNT_JSON" | python3 -c "
import sys, json
raw = sys.stdin.read()
data = json.loads(raw)
# Railway fa double-escape dei backslash: converte \\n letterali in newline reali
# (necessario per la private_key PEM del service account)
if 'private_key' in data:
    data['private_key'] = data['private_key'].replace('\\\\n', '\n')
with open('/app/serviceAccount.json', 'w') as f:
    json.dump(data, f, indent=2)
"
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
