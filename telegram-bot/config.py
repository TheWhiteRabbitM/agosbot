"""
Configurazione bot — legge dal file .env se presente,
altrimenti usa variabili d'ambiente di sistema.
"""

import os
from pathlib import Path

def _load_env():
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key not in os.environ:
            os.environ[key] = val

_load_env()

# Token del bot (da @BotFather)
BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# ID Telegram degli admin (hanno sempre accesso completo)
ADMIN_IDS: set[int] = {
    int(x.strip())
    for x in os.environ.get("ADMIN_TELEGRAM_IDS", "").split(",")
    if x.strip().isdigit()
}

# Percorso service account Firebase Admin
SERVICE_ACCOUNT_PATH: str = os.environ.get(
    "FIREBASE_SERVICE_ACCOUNT_PATH",
    str(Path(__file__).parent / "serviceAccount.json"),
)

# Nomi delle collezioni Firestore (stessi nomi usati dall'app web)
COLLECTIONS = {
    "guide":      "guide",
    "servizi":    "servizi",
    "agenzie":    "agenzie",
    "tours":      "tours",
    "fatture":    "fatture",
    "tg_users":   "telegram_users",
    "tg_pending": "telegram_users_pending",
}

if not BOT_TOKEN:
    raise SystemExit(
        "❌  TELEGRAM_BOT_TOKEN non configurato.\n"
        "    Copia .env.example in .env e inserisci il token."
    )
