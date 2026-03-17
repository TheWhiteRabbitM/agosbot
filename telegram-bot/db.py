"""
Accesso Firestore e logica di dominio condivisa.
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore

from config import SERVICE_ACCOUNT_PATH, COLLECTIONS

log = logging.getLogger(__name__)


# ─── Markdown escaping ────────────────────────────────────────────────────────

def esc(s: object) -> str:
    """
    Escapa i caratteri speciali di Telegram Markdown v1 nei valori provenienti
    da Firestore (nomi, label, hotel...) usati come testo normale nei messaggi.
    Evita che nomi con *, _, `, [ rompano la formattazione dei messaggi.
    """
    return (
        str(s)
        .replace("\\", "\\\\")
        .replace("*",  "\\*")
        .replace("_",  "\\_")
        .replace("`",  "\\`")
        .replace("[",  "\\[")
    )

# ─── Cache file + in-memory ───────────────────────────────────────────────────
# Strategia a due livelli:
#   1. RAM:      dati in memoria, validi finché un listener non segnala una modifica
#   2. File:     .cache_data.json su disco, sopravvive ai riavvii del bot
#   3. Firestore: fetch completo solo quando RAM e file sono assenti/invalidati
#
# La cache NON ha TTL: viene invalidata in tempo reale dai Firestore listeners
# avviati con start_cache_listeners(). Il file viene usato solo al primo avvio
# (warm-start) per evitare un fetch a freddo se i dati sono già recenti.
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache_data.json")

_cache_data:       dict | None       = None
_cache_valid:      bool              = False   # True = RAM fresca, False = deve rileggere
_listeners_started: bool             = False
_cache_lock:       asyncio.Lock | None = None  # prevenzione cache stampede


def _cache_save(data: dict) -> None:
    """Serializza i dati su file JSON con timestamp. Permessi 0o600 (solo owner)."""
    try:
        payload = {"ts": time.time(), "data": data}
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str)
        os.chmod(CACHE_FILE, 0o600)   # solo owner può leggere/scrivere
    except Exception as exc:
        log.warning("[cache] impossibile salvare su disco: %s", exc)


def _cache_load() -> dict | None:
    """
    Carica i dati dal file se esistente e non troppo vecchio (max 1 ora).
    Usato solo al warm-start prima che i listener siano operativi.
    """
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if time.time() - payload.get("ts", 0) > 3600:   # scarta se più vecchio di 1h
            return None
        return payload.get("data")
    except Exception:
        return None


def invalidate_cache() -> None:
    """
    Invalida RAM e file. Chiamata automaticamente dai Firestore listeners
    ogni volta che il portale web salva qualcosa.
    """
    global _cache_valid, _cache_data
    _cache_valid = False
    _cache_data  = None
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception as exc:
        log.warning("[cache] impossibile rimuovere cache file: %s", exc)


# ─── Callback assegnazione servizio ──────────────────────────────────────────
# bot.py registra una funzione qui con register_assignment_callback().
# Viene chiamata (da thread Firestore) quando un servizio viene assegnato a
# una guida: callback(servizio_dict, guida_id).

_assignment_callback: Any = None   # callable(svc: dict, guida_id: str) | None

def register_assignment_callback(fn) -> None:
    """Registra la funzione da chiamare quando un servizio viene assegnato a una guida."""
    global _assignment_callback
    _assignment_callback = fn


def start_cache_listeners() -> None:
    """
    Avvia listener Firestore real-time su tutte le collection principali.

    - Invalida la cache locale ad ogni modifica (portale web → bot aggiornato).
    - Sul listener dei servizi: rileva assegnazioni di guidaId e chiama
      _assignment_callback(svc, guida_id) per notificare la guida in Telegram.

    I listener girano su thread di background gestiti dall'SDK Firebase.
    Il primo snapshot viene usato solo per inizializzare il tracking dei guidaId;
    non invalida la cache né manda notifiche.

    Chiamare una sola volta all'avvio del bot (bot.py → main()).
    """
    global _listeners_started
    if _listeners_started:
        return
    _listeners_started = True

    fs = get_firestore()
    seen_initial: set[str] = set()

    # Traccia guidaId precedente per ogni servizio: {service_id: guida_id | None}
    # Permette di rilevare SOLO i cambi reali (non ogni modifica generica al servizio).
    _prev_guida: dict[str, str | None] = {}

    def _cb_servizi(snapshots, changes, read_time):
        nonlocal _prev_guida
        if "servizi" not in seen_initial:
            # Inizializzazione: popola _prev_guida senza invalidare né notificare
            seen_initial.add("servizi")
            for doc in snapshots:
                d = doc.to_dict() or {}
                _prev_guida[doc.id] = d.get("guidaId")
            return

        invalidate_cache()

        # Controlla se qualche guidaId è cambiato → notifica la guida
        if _assignment_callback is None:
            return
        for change in changes:
            doc_id   = change.document.id
            new_data = {"id": doc_id, **(change.document.to_dict() or {})}
            new_guida = new_data.get("guidaId")
            old_guida = _prev_guida.get(doc_id)
            _prev_guida[doc_id] = new_guida
            # Notifica solo se la guida è stata assegnata (o cambiata)
            if new_guida and new_guida != old_guida:
                try:
                    _assignment_callback(new_data, new_guida)
                except Exception:
                    log.exception("[notify] errore callback assegnazione")

    def _make_cb(name: str):
        def _cb(snapshots, changes, read_time):
            if name not in seen_initial:
                seen_initial.add(name)
                return
            log.info("[cache] modifica in '%s' → cache invalidata", name)
            invalidate_cache()
        return _cb

    col_servizi = COLLECTIONS.get("servizi", "servizi")
    fs.collection(col_servizi).on_snapshot(_cb_servizi)

    for col_key, col_name in COLLECTIONS.items():
        if col_key == "servizi":
            continue  # già registrato sopra con logica speciale
        fs.collection(col_name).on_snapshot(_make_cb(col_key))

    fs.collection("settings").on_snapshot(_make_cb("settings"))


# ─── Inizializzazione Firebase ────────────────────────────────────────────────

def _init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()

_firestore_client = None

def get_firestore():
    global _firestore_client
    if _firestore_client is None:
        _firestore_client = _init_firebase()
    return _firestore_client


# ─── Caricamento dati ─────────────────────────────────────────────────────────

_FETCH_LIMIT = 5000   # cap di sicurezza per ogni collection principale

def _fetch(collection_key: str) -> list[dict]:
    fs = get_firestore()
    return [
        {"id": doc.id, **doc.to_dict()}
        for doc in fs.collection(COLLECTIONS[collection_key]).limit(_FETCH_LIMIT).stream()
    ]

def _fetch_settings_doc(doc_id: str) -> dict:
    """
    Legge un documento singolo dalla collection 'settings'.
    Struttura: { guidaId: [data1, data2, ...], ... }
    """
    fs = get_firestore()
    doc = fs.collection("settings").document(doc_id).get()
    return doc.to_dict() or {} if doc.exists else {}

def _fetch_all() -> dict:
    """Fetch completo da Firestore — eseguito in thread per non bloccare il loop."""
    return {
        "guide":         _fetch("guide"),
        "servizi":       _fetch("servizi"),
        "agenzie":       _fetch("agenzie"),
        "tours":         _fetch("tours"),
        "fatture":       _fetch("fatture"),
        "disponibilita": _fetch_settings_doc("disponibilita"),
        "occupato":      _fetch_settings_doc("occupato"),
    }


async def get_data(force: bool = False) -> dict:
    """
    Carica tutte le collezioni principali + disponibilità guide.
    Livelli di cache (in ordine):
      1. RAM:      _cache_valid=True → risposta immediata, zero letture Firestore
      2. File:     .cache_data.json su disco → warm-start dopo riavvio del bot
      3. Firestore: fetch completo in asyncio.to_thread (non blocca il loop)

    La cache viene invalidata in tempo reale dai listener avviati con
    start_cache_listeners(): ogni salvataggio dal portale web trigghera
    invalidate_cache() e il prossimo get_data() rilegge dati freschi.

    Il lock previene cache stampede: se N richieste arrivano contemporaneamente
    con cache invalida, solo la prima va su Firestore; le altre aspettano e
    trovano la cache già riempita.

    Passa force=True per saltare tutti i livelli (es. morning report).
    """
    global _cache_data, _cache_valid, _cache_lock

    # Inizializza il lock al primo utilizzo (deve vivere nello stesso event loop)
    if _cache_lock is None:
        _cache_lock = asyncio.Lock()

    # Fast path senza lock — se la cache è già valida non serve sincronizzare
    if not force and _cache_valid and _cache_data is not None:
        return _cache_data

    async with _cache_lock:
        # Ricontrolla dopo aver acquisito il lock (un'altra coroutine potrebbe
        # aver già riempito la cache mentre aspettavamo)
        if not force and _cache_valid and _cache_data is not None:
            return _cache_data

        # Livello 2 — file su disco (warm-start dopo riavvio)
        if not force:
            from_file = _cache_load()
            if from_file is not None:
                _cache_data  = from_file
                _cache_valid = True
                return _cache_data

        # Livello 3 — Firestore (in thread separato per non bloccare il loop)
        log.info("[cache] fetch Firestore...")
        _cache_data  = await asyncio.to_thread(_fetch_all)
        _cache_valid = True
        _cache_save(_cache_data)
        log.info("[cache] dati aggiornati (%d servizi)", len(_cache_data.get("servizi", [])))
        return _cache_data


# ─── Lookup Telegram ID per guidaId ──────────────────────────────────────────

def get_telegram_id_per_guida(guida_id: str) -> int | None:
    """
    Restituisce il telegramId dell'utente abbinato a questa guida,
    oppure None se la guida non ha un account Telegram registrato e abilitato.
    Lettura diretta Firestore (non usa cache — usato solo per le notifiche push).
    """
    fs = get_firestore()
    docs = (
        fs.collection(COLLECTIONS["tg_users"])
        .where("guidaId", "==", guida_id)
        .where("abilitato", "==", True)
        .limit(1)
        .stream()
    )
    for doc in docs:
        d = doc.to_dict() or {}
        try:
            return int(d["telegramId"])
        except (KeyError, ValueError, TypeError):
            return None
    return None


# ─── Autenticazione utenti Telegram ──────────────────────────────────────────

async def get_utente(telegram_id: int) -> dict | None:
    from config import ADMIN_IDS
    if telegram_id in ADMIN_IDS:
        return {"role": "admin", "abilitato": True, "nome": "Admin", "guidaId": None}

    def _query() -> dict | None:
        fs = get_firestore()
        doc = fs.collection(COLLECTIONS["tg_users"]).document(str(telegram_id)).get()
        return doc.to_dict() if doc.exists else None

    return await asyncio.to_thread(_query)

async def registra_richiesta(telegram_id: int, username: str, nome: str):
    fs = get_firestore()
    fs.collection(COLLECTIONS["tg_pending"]).document(str(telegram_id)).set({
        "telegramId": str(telegram_id),
        "telegramUsername": username or "",
        "nome": nome,
        "richiestaAt": firestore.SERVER_TIMESTAMP,
    }, merge=True)

async def autorizza_utente(telegram_id: int, ruolo: str, guida_id: str | None, admin_id: int):
    fs = get_firestore()
    fs.collection(COLLECTIONS["tg_users"]).document(str(telegram_id)).set({
        "telegramId": str(telegram_id),
        "role": ruolo,
        "guidaId": guida_id,
        "abilitato": True,
        "autorizzatoDa": str(admin_id),
    }, merge=True)

async def disabilita_utente(telegram_id: int):
    fs = get_firestore()
    fs.collection(COLLECTIONS["tg_users"]).document(str(telegram_id)).set(
        {"abilitato": False}, merge=True
    )

async def get_pending() -> list[dict]:
    fs = get_firestore()
    return [d.to_dict() for d in fs.collection(COLLECTIONS["tg_pending"]).limit(500).stream()]

async def get_utenti_attivi() -> list[dict]:
    fs = get_firestore()
    return [d.to_dict() for d in fs.collection(COLLECTIONS["tg_users"]).limit(500).stream()]


# ─── Usage tracking ───────────────────────────────────────────────────────────

def _write_uso_sync(telegram_id: int, nome: str, ruolo: str, funzione: str) -> None:
    """Scrive un record di utilizzo su Firestore (sincrono, gira in thread)."""
    try:
        fs = get_firestore()
        fs.collection("bot_uso").add({
            "uid":   telegram_id,
            "nome":  nome,
            "ruolo": ruolo,
            "fn":    funzione,
            "ts":    firestore.SERVER_TIMESTAMP,
        })
    except Exception as exc:
        log.warning("[uso] errore scrittura: %s", exc)


async def log_uso(telegram_id: int, nome: str, ruolo: str, funzione: str) -> None:
    """
    Registra un utilizzo del bot su Firestore (fire-and-forget, non blocca il router).
    La scrittura avviene in un task di background: il router non aspetta il risultato.
    """
    async def _task():
        try:
            await asyncio.to_thread(_write_uso_sync, telegram_id, nome, ruolo, funzione)
        except Exception as exc:
            log.warning("[uso] task fallita: %s", exc)
    asyncio.create_task(_task())


def _fetch_uso_sync(limit: int = 500) -> list[dict]:
    """Legge i record di utilizzo degli ultimi 7 giorni da Firestore."""
    import datetime as _dt
    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=7)
    fs = get_firestore()
    docs = (
        fs.collection("bot_uso")
        .where("ts", ">=", cutoff)
        .order_by("ts", direction="DESCENDING")
        .limit(limit)
        .stream()
    )
    return [d.to_dict() for d in docs]


async def get_uso_stats() -> list[dict]:
    """Restituisce i record di utilizzo degli ultimi 7 giorni (non blocca il loop)."""
    return await asyncio.to_thread(_fetch_uso_sync)


# ─── Logica di dominio ────────────────────────────────────────────────────────

def servizio_contabile(s: dict) -> bool:
    """
    Un servizio conta (guida, fattura, contabilità) solo se:
      NORMAL   → sempre
      ANREISE  → solo se guidaNecessaria non è False
      HEIMREISE → mai
    """
    tipo = s.get("tipo", "NORMAL")
    if tipo == "HEIMREISE":
        return False
    if tipo == "ANREISE":
        return s.get("guidaNecessaria", True) is not False
    return True


# ─── Helpers date ─────────────────────────────────────────────────────────────

def oggi_str() -> str:
    return date.today().isoformat()

def domani_str() -> str:
    return (date.today() + timedelta(days=1)).isoformat()

def lunedi_str() -> str:
    d = date.today()
    return (d - timedelta(days=d.weekday())).isoformat()

def domenica_str() -> str:
    d = date.today()
    return (d + timedelta(days=6 - d.weekday())).isoformat()

def lunedi_prossimo_str() -> str:
    d = date.today()
    return (d + timedelta(days=7 - d.weekday())).isoformat()

def domenica_prossima_str() -> str:
    d = date.today()
    return (d + timedelta(days=13 - d.weekday())).isoformat()

def sabato_str() -> str:
    """Sabato della settimana corrente (o prossimo sabato se già passato)."""
    d = date.today()
    return (d + timedelta(days=5 - d.weekday())).isoformat()

def mese_corrente() -> str:
    return date.today().strftime("%Y-%m")

GIORNI_ITA  = ["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"]
GIORNI_BREVI = ["Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom"]
MESI_ITA    = ["", "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
               "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
MESI_BREVI  = ["", "Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
               "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]

def data_it(iso: str) -> str:
    """15/03/2026"""
    if not iso:
        return "—"
    y, m, d = iso.split("-")
    return f"{d}/{m}/{y}"

def data_bella(iso: str) -> str:
    """Dom 15 Mar"""
    if not iso:
        return "—"
    d = date.fromisoformat(iso)
    return f"{GIORNI_BREVI[d.weekday()]} {d.day} {MESI_BREVI[d.month]}"

def data_estesa(iso: str) -> str:
    """Domenica 15 Marzo"""
    if not iso:
        return "—"
    d = date.fromisoformat(iso)
    return f"{GIORNI_ITA[d.weekday()]} {d.day} {MESI_ITA[d.month]}"

def giorno_sett(iso: str) -> str:
    d = date.fromisoformat(iso)
    return GIORNI_BREVI[d.weekday()]

def eur(n) -> str:
    try:
        return f"€{float(n):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "€0,00"
