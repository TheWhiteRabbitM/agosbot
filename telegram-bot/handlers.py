"""
Handler dei comandi e messaggi del bot.
Ogni funzione riceve (update, context) da python-telegram-bot v21.
"""

from __future__ import annotations
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

import db
from db import (
    get_data, get_utente, registra_richiesta, autorizza_utente,
    disabilita_utente, get_pending, get_utenti_attivi,
    log_uso, get_uso_stats,
    servizio_contabile, oggi_str, domani_str, lunedi_str, domenica_str,
    lunedi_prossimo_str, domenica_prossima_str, sabato_str,
    mese_corrente, data_it, data_bella, data_estesa, giorno_sett, eur,
    MESI_ITA, MESI_BREVI, esc,
)

SEP = "—" * 20   # separatore visivo per i messaggi
from config import ADMIN_IDS
import time
from collections import defaultdict

# ─── Rate limiter per utente ──────────────────────────────────────────────────
# Finestra scorrevole: max _RATE_MAX comandi in _RATE_WINDOW secondi per utente.
# Protegge da spam e da query Firestore accidentalmente ripetute.
_rate_timestamps: dict[int, list[float]] = defaultdict(list)
_RATE_WINDOW = 60    # secondi
_RATE_MAX    = 30    # max comandi per finestra

def _rate_ok(uid: int) -> bool:
    now = time.monotonic()
    ts  = _rate_timestamps[uid]
    ts[:] = [t for t in ts if now - t < _RATE_WINDOW]
    if len(ts) >= _RATE_MAX:
        return False
    ts.append(now)
    return True


# ─── Tastiere per ruolo ───────────────────────────────────────────────────────

KB_ADMIN = ReplyKeyboardMarkup([
    ["📅 Oggi",        "📆 Settimana",       "⏭️ Sett. Prossima"],
    ["🗓️ Weekend",    "👥 Guide",            "🟢 Guide Stato"],
    ["🏢 Agenzie",    "💶 Fatturato",        "💵 Incassi Cash"],
    ["⚠️ Alert",      "📊 Riepilogo Mese"],
    ["🔧 Gestisci utenti", "👁️ Simula Guida", "📈 Statistiche"],
], resize_keyboard=True)

# Tastiera guide — bottoni VOLUTAMENTE diversi da quelli admin/viewer
# per garantire che il router non li possa mai smistare verso handler sbagliati.
KB_GUIDA = ReplyKeyboardMarkup([
    ["📅 I Miei Oggi"],
    ["📆 La Mia Settimana", "⏭️ La Mia Prossima"],
    ["📋 Il Mio Mese"],
], resize_keyboard=True)

# Tastiera simulazione: identica a KB_GUIDA + pulsante di uscita
KB_SIMULA = ReplyKeyboardMarkup([
    ["📅 I Miei Oggi"],
    ["📆 La Mia Settimana", "⏭️ La Mia Prossima"],
    ["📋 Il Mio Mese"],
    ["🔚 Esci simulazione"],
], resize_keyboard=True)

# Stato simulazione per-utente: telegram_id → guida_id simulata
# Usato solo per gli admin. Le guide non entrano mai in questo dict.
_simulazione_attiva: dict[int, str] = {}

KB_VIEWER = ReplyKeyboardMarkup([
    ["📅 Oggi",   "📆 Settimana", "⏭️ Sett. Prossima"],
    ["🗓️ Weekend", "👥 Guide",   "🟢 Guide Stato"],
], resize_keyboard=True)

def tastiera(role: str) -> ReplyKeyboardMarkup:
    return {"admin": KB_ADMIN, "guida": KB_GUIDA}.get(role, KB_VIEWER)


# ─── Middleware autenticazione ────────────────────────────────────────────────

async def _autenticato(update: Update, ruoli: list[str] | None):
    """
    Ritorna l'utente se autenticato e autorizzato, altrimenti invia un messaggio
    e ritorna None.
    """
    uid = update.effective_user.id
    utente = await get_utente(uid)

    if utente is None:
        await registra_richiesta(
            uid,
            update.effective_user.username or "",
            f"{update.effective_user.first_name or ''} {update.effective_user.last_name or ''}".strip(),
        )
        await update.message.reply_text(
            f"👋 Ciao *{update.effective_user.first_name}*\\!\n\n"
            f"La tua richiesta di accesso è stata registrata\\.\n"
            f"Un amministratore dovrà autorizzarti\\.\n\n"
            f"Il tuo ID Telegram: `{uid}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if not utente.get("abilitato"):
        await update.message.reply_text("⛔ Account disabilitato\\. Contatta l'amministratore\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return None

    if ruoli and utente.get("role") not in ruoli:
        await update.message.reply_text("⛔ Non hai i permessi per questo comando\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return None

    return utente


# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await get_utente(update.effective_user.id)
    if utente is None:
        await registra_richiesta(
            update.effective_user.id,
            update.effective_user.username or "",
            f"{update.effective_user.first_name or ''} {update.effective_user.last_name or ''}".strip(),
        )
        await update.message.reply_text(
            f"👋 Ciao *{update.effective_user.first_name}*!\n\n"
            f"Richiesta di accesso registrata.\n"
            f"Comunica il tuo ID all'amministratore: `{update.effective_user.id}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if not utente.get("abilitato"):
        await update.message.reply_text("⛔ Account disabilitato.")
        return

    role = utente.get("role", "viewer")
    await update.message.reply_text(
        f"✅ Bentornato *{utente.get('nome', update.effective_user.first_name)}*!\n"
        f"Ruolo: *{role}*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=tastiera(role),
    )


# ─── /autorizza /disabilita ───────────────────────────────────────────────────

async def cmd_autorizza(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return
    args = context.args  # [telegram_id, ruolo, guida_id?]
    if len(args) < 2 or args[1] not in ("admin", "guida", "viewer"):
        await update.message.reply_text(
            "Uso: `/autorizza <telegram_id> <admin|guida|viewer> [guida_id]`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text(
            "⚠️ ID non valido. Deve essere un numero intero (es. `123456789`).",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    ruolo     = args[1]
    guida_id  = args[2] if len(args) > 2 else None
    await autorizza_utente(target_id, ruolo, guida_id, update.effective_user.id)
    await update.message.reply_text(
        f"✅ Utente `{target_id}` autorizzato come *{ruolo}*"
        + (f" (guida: `{guida_id}`)" if guida_id else ""),
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_disabilita(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return
    if not context.args:
        await update.message.reply_text("Uso: `/disabilita <telegram_id>`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "⚠️ ID non valido. Deve essere un numero intero (es. `123456789`).",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await disabilita_utente(target_id)
    await update.message.reply_text(f"🚫 Utente `{target_id}` disabilitato.", parse_mode=ParseMode.MARKDOWN)


# ─── /mioid — mostra il proprio ID Telegram ──────────────────────────────────

async def cmd_mioid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(
        f"🆔 Il tuo ID Telegram: `{uid}`\n\n"
        f"Comunicalo all'amministratore per essere autorizzato:\n"
        f"`/autorizza {uid} <admin|guida|viewer>`",
        parse_mode=ParseMode.MARKDOWN,
    )


# ─── /ruolo — mostra il proprio ruolo attuale ────────────────────────────────

async def cmd_ruolo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await get_utente(update.effective_user.id)
    if not utente:
        await update.message.reply_text(
            f"❌ Non sei ancora registrato.\nIl tuo ID: `{update.effective_user.id}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    role    = utente.get("role", "—")
    abil    = "✅ Abilitato" if utente.get("abilitato") else "🚫 Disabilitato"
    guida   = utente.get("guidaId")
    testo   = f"👤 *Il tuo profilo*\n\nRuolo: *{role}*\nStato: {abil}\n"
    if guida:
        testo += f"Guida ID: `{guida}`\n"
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── Helper: blocco servizi raggruppato per agenzia ──────────────────────────

def _ico_tipo(tipo: str) -> str:
    return {"ANREISE": "✈️", "HEIMREISE": "🚌"}.get(tipo, "🥾")

def _build_hotel_map(tutti_servizi: list) -> dict:
    """
    Costruisce un indice gruppo_key → lista ordinata di (data, hotel).
    Indicizza TUTTI i servizi con hotel valorizzato (non solo ANREISE),
    così gestisce sia hotel fisso per tutto il soggiorno sia cambi giornalieri.

    Chiave: s['gruppo'] se presente, altrimenti 'bus_{bus}_{agenziaId}'.
    """
    raw: dict[str, list] = {}
    for s in tutti_servizi:
        if s.get("hotel") and s.get("data"):
            key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
            if key:
                raw.setdefault(key, []).append((s["data"], s["hotel"]))
    # Ordina per data ascendente
    return {k: sorted(v) for k, v in raw.items()}

def _hotel_servizio(s: dict, hotel_map: dict) -> str:
    """
    Restituisce l'hotel valido per questo servizio alla sua data.
    Regola: se il servizio ha hotel proprio → usalo (cambio hotel quel giorno).
    Altrimenti → prendi l'hotel più recente del gruppo con data ≤ s['data'].
    Gestisce sia soggiorni in hotel fisso che gruppi che cambiano hotel ogni giorno.
    """
    if s.get("hotel"):
        return s["hotel"]
    key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
    if not key:
        return ""
    entries = hotel_map.get(key, [])
    if not entries:
        return ""
    data_s = s.get("data") or ""
    # L'hotel più recente con data <= data_s
    result = ""
    for data_h, hotel_h in entries:
        if data_h <= data_s:
            result = hotel_h
        else:
            break
    return result

def _blocco_servizi(
    servizi: list, guide_map: dict, agenzie_map: dict,
    hotel_map: dict | None = None, mostra_guida: bool = True,
) -> str:
    """
    Lista servizi in ordine cronologico, 3 righe per servizio:
      🏢 Agenzia
      Bus N - Guida - Tipo  🕐 orario
      📍 Punto - 🏨 Hotel
    mostra_guida=False omette il nome guida (usato nella vista guida personale).
    """
    hmap  = hotel_map or {}
    testo = ""
    for s in sorted(servizi, key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
        ag      = agenzie_map.get(s.get("agenziaId") or "")
        ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
        guida   = guide_map.get(s.get("guidaId") or "")
        label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
        hotel   = esc(_hotel_servizio(s, hmap))
        punto   = esc(s.get("punto_incontro") or "IN HTL")

        # Riga 2: Bus N - [Guida -] Tipo  🕐 orario
        parti = []
        if s.get("bus"):
            parti.append(f"Bus {esc(s['bus'])}")
        if mostra_guida:
            parti.append(esc(guida["nome"]) if guida else "⚠️ guida mancante")
        parti.append(label)
        riga2 = " - ".join(parti)
        if s.get("orarioPartenza"):
            riga2 += f"  🕐 {s['orarioPartenza']}"

        # Riga 3: 📍 Punto - 🏨 Hotel
        riga3 = f"📍 {punto}"
        if hotel:
            riga3 += f" - 🏨 {hotel}"

        testo += f"🏢 *{ag_nome}*\n{riga2}\n{riga3}\n\n"

    return testo


# Alias per compatibilità con eventuali chiamate esterne
def _blocco_per_agenzia(
    servizi: list, guide_map: dict, agenzie_map: dict,
    hotel_map: dict | None = None, dettaglio: bool = False,
) -> str:
    return _blocco_servizi(servizi, guide_map, agenzie_map, hotel_map=hotel_map)


# ─── Helper: rendering calendario (settimana attuale o prossima) ──────────────

async def _render_settimana(update: Update, utente: dict, lun: str, dom: str, titolo: str):
    dati = await get_data()
    servizi = [
        s for s in dati["servizi"]
        if lun <= (s.get("data") or "") <= dom and servizio_contabile(s)
    ]
    if utente.get("role") == "guida" and utente.get("guidaId"):
        servizi = [s for s in servizi if s.get("guidaId") == utente["guidaId"]]

    ico = "🗓️" if "Weekend" in titolo else "📆"
    testo = f"{ico} *{titolo}*\n_{data_bella(lun)} — {data_bella(dom)}_\n`{SEP}`\n\n"

    if not servizi:
        testo += "_Nessun servizio programmato._"
        await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)
        return

    guide_map   = {g["id"]: g for g in dati["guide"]}
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    hotel_map   = _build_hotel_map(dati["servizi"])

    per_giorno: dict[str, list] = {}
    for s in servizi:
        per_giorno.setdefault(s["data"], []).append(s)

    for i, giorno in enumerate(sorted(per_giorno)):
        if i > 0:
            testo += f"`{SEP}`\n"
        n = len(per_giorno[giorno])
        testo += f"📆 *{data_bella(giorno)}* — {n} {'servizio' if n == 1 else 'servizi'}\n"
        testo += _blocco_servizi(per_giorno[giorno], guide_map, agenzie_map,
                                 hotel_map=hotel_map)

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── OGGI ─────────────────────────────────────────────────────────────────────

async def handle_oggi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, None)
    if not utente:
        return

    data_str = oggi_str()
    dati = await get_data()
    servizi = [s for s in dati["servizi"] if s.get("data") == data_str and servizio_contabile(s)]

    if utente.get("role") == "guida" and utente.get("guidaId"):
        servizi = [s for s in servizi if s.get("guidaId") == utente["guidaId"]]

    senza_g = sum(1 for s in servizi if not s.get("guidaId"))
    if not servizi:
        testo = f"📅 *Oggi — {data_estesa(data_str)}*\n`{SEP}`\n\n_Nessun servizio in programma._"
        await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)
        return

    alert = f"  ⚠️ {senza_g} senza guida" if senza_g else ""
    testo  = f"📅 *Oggi — {data_estesa(data_str)}*\n`{SEP}`\n"
    testo += f"_{len(servizi)} servizi{alert}_\n\n"

    guide_map   = {g["id"]: g for g in dati["guide"]}
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    hotel_map   = _build_hotel_map(dati["servizi"])
    testo += _blocco_servizi(servizi, guide_map, agenzie_map, hotel_map=hotel_map)
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── SETTIMANA ────────────────────────────────────────────────────────────────

async def handle_settimana(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, None)
    if not utente:
        return
    await _render_settimana(update, utente, lunedi_str(), domenica_str(), "Settimana")


# ─── SETTIMANA PROSSIMA ───────────────────────────────────────────────────────

async def handle_settimana_prossima(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, None)
    if not utente:
        return
    await _render_settimana(update, utente, lunedi_prossimo_str(), domenica_prossima_str(), "Settimana prossima")


# ─── WEEKEND (Sab–Lun) ───────────────────────────────────────────────────────

async def handle_weekend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, None)
    if not utente:
        return
    sab = sabato_str()
    lun = lunedi_prossimo_str()
    await _render_settimana(update, utente, sab, lun, "Weekend")


# ─── GUIDE STATO (Libere / Occupate Oggi) ────────────────────────────────────

async def handle_guide_stato(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin", "viewer"])
    if not utente:
        return

    oggi = oggi_str()
    dom  = domani_str()
    dati = await get_data()
    agenzie_map   = {a["id"]: a for a in dati["agenzie"]}
    disponibilita = dati.get("disponibilita") or {}
    occupato_ext  = dati.get("occupato") or {}
    hotel_map     = _build_hotel_map(dati["servizi"])

    def _sv_per_guida(data: str) -> dict[str, list]:
        out: dict[str, list] = {}
        for s in dati["servizi"]:
            if s.get("data") == data and s.get("guidaId") and servizio_contabile(s):
                out.setdefault(s["guidaId"], []).append(s)
        return out

    def _stato_giorno(data: str):
        sv_pg = _sv_per_guida(data)
        occ_i, occ_e, disp = [], [], []
        for g in sorted(dati["guide"], key=lambda x: x.get("nome", "")):
            gid = g["id"]
            svs = sv_pg.get(gid, [])
            if svs:
                occ_i.append((g, svs))
            elif data in (occupato_ext.get(gid) or []):
                occ_e.append(g)
            elif data in (disponibilita.get(gid) or []):
                disp.append(g)
        return occ_i, occ_e, disp

    def _blocco_stato(data: str, titolo: str) -> str:
        occ_interni, occ_esterni, disponibili = _stato_giorno(data)
        t = f"*{titolo}*\n`{SEP}`\n\n"

        if occ_interni:
            t += f"🔴 *In servizio — {len(occ_interni)}*\n\n"
            for g, svs in occ_interni:
                tel = f"  📞 {esc(g['telefono'])}" if g.get("telefono") else ""
                t += f"👤 *{esc(g['nome'])}*{tel}\n"
                for s in sorted(svs, key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
                    ag      = agenzie_map.get(s.get("agenziaId") or "")
                    ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
                    label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
                    hotel   = esc(_hotel_servizio(s, hotel_map))
                    punto   = esc(s.get("punto_incontro") or "IN HTL")
                    parti   = []
                    if s.get("bus"):
                        parti.append(f"Bus {esc(s['bus'])}")
                    parti.append(label)
                    riga2 = " - ".join(parti)
                    if s.get("orarioPartenza"):
                        riga2 += f"  🕐 {s['orarioPartenza']}"
                    riga3 = f"📍 {punto}"
                    if hotel:
                        riga3 += f" - 🏨 {hotel}"
                    t += f"🏢 *{ag_nome}*\n{riga2}\n{riga3}\n"
                t += "\n"

        if occ_esterni:
            t += f"🟠 *Occupate esternamente — {len(occ_esterni)}*\n"
            for g in occ_esterni:
                tel = f"  📞 {esc(g['telefono'])}" if g.get("telefono") else ""
                t += f"  {esc(g['nome'])}{tel}\n"
            t += "\n"

        if disponibili:
            t += f"🟢 *Disponibili — {len(disponibili)}*\n"
            for g in disponibili:
                tel = f"  📞 {esc(g['telefono'])}" if g.get("telefono") else ""
                t += f"  {esc(g['nome'])}{tel}\n"
            t += "\n"

        if not any([occ_interni, occ_esterni, disponibili]):
            t += "_Nessuna segnalazione._\n"

        return t

    testo  = f"👤 *Guide Stato*\n\n"
    testo += _blocco_stato(oggi, f"📅 Oggi — {data_estesa(oggi)}")
    testo += _blocco_stato(dom,  f"📆 Domani — {data_estesa(dom)}")

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── GUIDE ────────────────────────────────────────────────────────────────────

async def handle_guide(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin", "viewer"])
    if not utente:
        return

    oggi = oggi_str()
    dom  = domani_str()
    dati = await get_data()

    testo  = f"👥 *Guide — {data_estesa(oggi)}*\n"
    testo += f"`{SEP}`\n\n"
    hotel_map   = _build_hotel_map(dati["servizi"])
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    for g in sorted(dati["guide"], key=lambda x: x.get("nome", "")):
        sv_oggi = [s for s in dati["servizi"] if s.get("data") == oggi and s.get("guidaId") == g["id"] and servizio_contabile(s)]
        sv_dom  = [s for s in dati["servizi"] if s.get("data") == dom  and s.get("guidaId") == g["id"] and servizio_contabile(s)]
        ico_stato = "🔴" if sv_oggi else "🟢"
        tel_str   = f"  📞 {esc(g['telefono'])}" if g.get("telefono") else ""
        testo += f"{ico_stato} *{esc(g['nome'])}*{tel_str}\n"
        for s in sorted(sv_oggi, key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
            ag      = agenzie_map.get(s.get("agenziaId") or "")
            ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
            label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
            hotel   = esc(_hotel_servizio(s, hotel_map))
            punto   = esc(s.get("punto_incontro") or "IN HTL")
            parti   = []
            if s.get("bus"):
                parti.append(f"Bus {esc(s['bus'])}")
            parti.append(label)
            riga2 = " - ".join(parti)
            if s.get("orarioPartenza"):
                riga2 += f"  🕐 {s['orarioPartenza']}"
            riga3 = f"📍 {punto}"
            if hotel:
                riga3 += f" - 🏨 {hotel}"
            testo += f"  🏢 *{ag_nome}*\n  {riga2}\n  {riga3}\n"
        if sv_dom:
            labels = " / ".join(
                esc(s.get("tipoLabel") or s.get("tipo", "")) +
                (f" Bus {esc(s['bus'])}" if s.get("bus") else "")
                for s in sv_dom
            )
            testo += f"  ↳ Domani: {labels}\n"
        testo += "\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── AGENZIE ──────────────────────────────────────────────────────────────────

async def handle_agenzie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    oggi = oggi_str()
    dati = await get_data()
    testo = "🏢 *Agenzie — Situazione*\n\n"

    for a in sorted(dati["agenzie"], key=lambda x: x.get("nome", "")):
        is_cash = bool(a.get("incassoCash"))
        sv_futuri   = [s for s in dati["servizi"] if s.get("agenziaId") == a["id"] and (s.get("data") or "") >= oggi and servizio_contabile(s)]
        testo += f"{'💵' if is_cash else '🧾'} *{a['nome']}*{' (CASH)' if is_cash else ''}\n"
        testo += f"   Servizi in corso/futuri: {len(sv_futuri)}\n"

        if is_cash:
            sv_eseguiti  = [s for s in dati["servizi"] if s.get("agenziaId") == a["id"] and (s.get("data") or "") < oggi and servizio_contabile(s)]
            tot_eseguiti = sum(s.get("incasso", 0) or 0 for s in sv_eseguiti)
            tot_conf     = sum(s.get("incasso", 0) or 0 for s in sv_eseguiti if s.get("incassatoCash"))
            testo += f"   ✅ Incassato cash: {eur(tot_conf)}\n"
            testo += f"   ⏳ Da incassare: {eur(tot_eseguiti - tot_conf)}\n"
        else:
            fat_aperte = [f for f in dati["fatture"] if f.get("agenziaId") == a["id"] and f.get("stato") in ("emessa", "scaduta")]
            tot_aperte = sum(f.get("importo", 0) or 0 for f in fat_aperte)
            if tot_aperte > 0:
                testo += f"   🧾 Fatture aperte: {eur(tot_aperte)}\n"
        testo += "\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── FATTURATO ────────────────────────────────────────────────────────────────

async def handle_fatturato(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    mese = mese_corrente()
    oggi = oggi_str()
    dati = await get_data()

    fatture_mese = [f for f in dati["fatture"] if (f.get("dataEmissione") or f.get("createdAt") or "").startswith(mese)]
    tot_pagate   = sum(f.get("importo", 0) or 0 for f in fatture_mese if f.get("stato") == "pagata")
    tot_emesse   = sum(f.get("importo", 0) or 0 for f in fatture_mese if f.get("stato") in ("emessa", "scaduta"))
    tot_scadute  = sum(f.get("importo", 0) or 0 for f in fatture_mese if f.get("stato") == "scaduta")

    sv_mese   = [s for s in dati["servizi"] if (s.get("data") or "").startswith(mese) and s.get("data") <= oggi and servizio_contabile(s)]
    ag_map    = {a["id"]: a for a in dati["agenzie"]}
    tot_cash  = sum(s.get("incasso", 0) or 0 for s in sv_mese if s.get("incassatoCash"))
    tot_da_cash = sum(
        s.get("incasso", 0) or 0 for s in sv_mese
        if ag_map.get(s.get("agenziaId") or {}, {}).get("incassoCash") and not s.get("incassatoCash")
    )

    anno, mese_num = mese.split("-")
    testo  = f"💶 *Fatturato — {MESI_ITA[int(mese_num)]} {anno}*\n`{SEP}`\n\n"
    testo += f"📤 Tot. fatturato: {eur(tot_pagate + tot_emesse)}\n"
    testo += f"✅ Incassato bonifico: {eur(tot_pagate)}\n"
    testo += f"⏳ Da incassare (fattura): {eur(tot_emesse)}\n"
    if tot_scadute > 0:
        testo += f"🔴 Di cui scadute: {eur(tot_scadute)}\n"
    testo += f"\n💵 Incassato cash: {eur(tot_cash)}\n"
    if tot_da_cash > 0:
        testo += f"⏳ Da incassare cash: {eur(tot_da_cash)}\n"
    testo += f"\n💰 *Totale incassato: {eur(tot_pagate + tot_cash)}*\n"

    # Top agenzie per volume fatture
    per_ag: dict[str, float] = {}
    for f in fatture_mese:
        ag_id = f.get("agenziaId") or ""
        per_ag[ag_id] = per_ag.get(ag_id, 0) + (f.get("importo") or 0)
    top = sorted(per_ag.items(), key=lambda x: -x[1])[:5]
    if top:
        testo += "\n*Top agenzie:*\n"
        for ag_id, tot in top:
            ag = ag_map.get(ag_id)
            testo += f"  • {ag['nome'] if ag else ag_id}: {eur(tot)}\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── INCASSI CASH ─────────────────────────────────────────────────────────────

async def handle_incassi_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    oggi = oggi_str()
    dati = await get_data()
    ag_cash = [a for a in dati["agenzie"] if a.get("incassoCash")]

    if not ag_cash:
        await update.message.reply_text("💵 Nessuna agenzia configurata per incasso cash.")
        return

    testo = "💵 *Incassi Cash — Situazione*\n\n"
    for a in sorted(ag_cash, key=lambda x: x.get("nome", "")):
        sv = [s for s in dati["servizi"] if s.get("agenziaId") == a["id"] and (s.get("data") or "") < oggi and servizio_contabile(s)]
        tot_tot  = sum(s.get("incasso", 0) or 0 for s in sv)
        tot_conf = sum(s.get("incasso", 0) or 0 for s in sv if s.get("incassatoCash"))
        da_inc   = tot_tot - tot_conf

        testo += f"🏢 *{a['nome']}*\n"
        testo += f"   Totale servizi: {eur(tot_tot)}\n"
        testo += f"   ✅ Incassato: {eur(tot_conf)}\n"
        if da_inc > 0:
            testo += f"   🔴 Da incassare: *{eur(da_inc)}*\n"
        else:
            testo += f"   🟢 Tutto incassato\n"
        testo += "\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── ALERT ────────────────────────────────────────────────────────────────────

async def handle_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    from datetime import date, timedelta
    oggi = oggi_str()
    dom  = domani_str()
    tre_giorni_fa = (date.today() - timedelta(days=3)).isoformat()
    dati = await get_data()
    ag_map = {a["id"]: a for a in dati["agenzie"]}
    alerts = []

    # Gite oggi/domani senza guida
    for s in dati["servizi"]:
        if s.get("data") in (oggi, dom) and not s.get("guidaId") and servizio_contabile(s):
            quando = "OGGI" if s.get("data") == oggi else "DOMANI"
            label  = s.get("tipoLabel") or s.get("tipo", "")
            bus    = f" Bus {s['bus']}" if s.get("bus") else ""
            alerts.append(f"🔴 *[{quando}]* {label}{bus} — guida non assegnata")

    # Fatture scadute
    for f in dati["fatture"]:
        if f.get("stato") == "scaduta":
            ag = ag_map.get(f.get("agenziaId") or "")
            alerts.append(f"🟠 Fattura *{f.get('numero') or f['id']}* scaduta — {ag['nome'] if ag else '?'} ({eur(f.get('importo', 0))})")

    # Cash non confermati da più di 3 giorni
    for a in dati["agenzie"]:
        if not a.get("incassoCash"):
            continue
        in_attesa = [
            s for s in dati["servizi"]
            if s.get("agenziaId") == a["id"] and (s.get("data") or "") < tre_giorni_fa
            and not s.get("incassatoCash") and servizio_contabile(s)
        ]
        if in_attesa:
            tot = sum(s.get("incasso", 0) or 0 for s in in_attesa)
            alerts.append(f"🟡 *{a['nome']}* — {len(in_attesa)} servizi cash non confermati ({eur(tot)})")

    if not alerts:
        await update.message.reply_text(
            f"✅ *Tutto in ordine!*\nNessun alert per {data_bella(oggi)}.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    testo = f"⚠️ *Alert — {data_bella(oggi)}*\n`{SEP}`\n\n" + "\n".join(alerts)
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── RIEPILOGO MESE ───────────────────────────────────────────────────────────

async def handle_riepilogo_mese(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    mese = mese_corrente()
    oggi = oggi_str()
    dati = await get_data()
    ag_map = {a["id"]: a for a in dati["agenzie"]}

    sv_mese = [
        s for s in dati["servizi"]
        if (s.get("data") or "").startswith(mese) and (s.get("data") or "") <= oggi and servizio_contabile(s)
    ]
    tot_incasso  = sum(s.get("incasso", 0) or 0 for s in sv_mese)
    senza_guida  = sum(1 for s in sv_mese if not s.get("guidaId"))
    guide_attive = len({s["guidaId"] for s in sv_mese if s.get("guidaId")})

    anno, mese_num = mese.split("-")
    testo  = f"📊 *Riepilogo {MESI_ITA[int(mese_num)]} {anno}*\n`{SEP}`\n\n"
    testo += f"🥾 Servizi eseguiti: *{len(sv_mese)}*\n"
    testo += f"👤 Guide attive: *{guide_attive}*\n"
    if senza_guida:
        testo += f"⚠️ Senza guida: *{senza_guida}*\n"
    testo += f"\n💰 Volume totale: *{eur(tot_incasso)}*\n"

    per_ag: dict[str, dict] = {}
    for s in sv_mese:
        ag_id = s.get("agenziaId") or "?"
        entry = per_ag.setdefault(ag_id, {"n": 0, "tot": 0.0})
        entry["n"] += 1
        entry["tot"] += s.get("incasso", 0) or 0

    top = sorted(per_ag.items(), key=lambda x: -x[1]["tot"])[:5]
    if top:
        testo += "\n*Per agenzia:*\n"
        for ag_id, v in top:
            ag = ag_map.get(ag_id)
            testo += f"  • {ag['nome'] if ag else ag_id}: {v['n']} servizi — {eur(v['tot'])}\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── MIO MESE (guida) ─────────────────────────────────────────────────────────

async def handle_mio_mese(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["guida", "admin"])
    if not utente:
        return

    guida_id = utente.get("guidaId")
    if not guida_id:
        await update.message.reply_text("⚠️ Nessuna guida associata al tuo account.")
        return

    mese = mese_corrente()
    oggi = oggi_str()
    dati = await get_data()
    ag_map = {a["id"]: a for a in dati["agenzie"]}

    guida = next((g for g in dati["guide"] if g["id"] == guida_id), None)
    sv = sorted(
        [s for s in dati["servizi"] if s.get("guidaId") == guida_id and (s.get("data") or "").startswith(mese) and servizio_contabile(s)],
        key=lambda x: x.get("data", ""),
    )
    eseguiti = [s for s in sv if (s.get("data") or "") <= oggi]
    futuri   = [s for s in sv if (s.get("data") or "") > oggi]

    anno, mese_num = mese.split("-")
    nome   = guida["nome"] if guida else "La mia agenda"
    testo  = f"📋 *{nome} — {MESI_ITA[int(mese_num)]} {anno}*\n\n"
    testo += f"✅ Servizi svolti: *{len(eseguiti)}*\n"
    testo += f"📅 Prossimi: *{len(futuri)}*\n\n"

    if futuri:
        testo += "*Prossimi servizi:*\n"
        for s in futuri[:8]:
            ag = ag_map.get(s.get("agenziaId") or "")
            label = s.get("tipoLabel") or s.get("tipo", "")
            riga  = f"  📅 {data_it(s['data'])} — {label}"
            if s.get("bus"):
                riga += f" Bus {s['bus']}"
            if ag:
                riga += f" ({ag['nome']})"
            testo += riga + "\n"
        if len(futuri) > 8:
            testo += f"  _...e altri {len(futuri)-8}_\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── VISTA GUIDA — helper condiviso ──────────────────────────────────────────

def _build_miei_servizi(servizi_filtrati: list, agenzie_map: dict, hotel_map: dict) -> str:
    """
    Costruisce il blocco testo per i servizi della guida autenticata.
    Chiamato SOLO da handler che hanno già filtrato per guidaId — non espone mai
    dati di altre guide.
    """
    if not servizi_filtrati:
        return "_Nessun servizio in programma._\n"

    testo = ""
    per_giorno: dict[str, list] = {}
    for s in servizi_filtrati:
        per_giorno.setdefault(s.get("data", ""), []).append(s)

    for i, giorno in enumerate(sorted(per_giorno)):
        if i > 0:
            testo += f"`{SEP}`\n"
        testo += f"*{data_bella(giorno)}*\n"
        for s in sorted(per_giorno[giorno], key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
            ag      = agenzie_map.get(s.get("agenziaId") or "")
            ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
            label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
            hotel   = esc(_hotel_servizio(s, hotel_map))
            punto   = esc(s.get("punto_incontro") or "IN HTL")
            parti   = []
            if s.get("bus"):
                parti.append(f"Bus {esc(s['bus'])}")
            parti.append(label)
            riga2 = " - ".join(parti)
            if s.get("orarioPartenza"):
                riga2 += f"  🕐 {s['orarioPartenza']}"
            riga3 = f"📍 {punto}"
            if hotel:
                riga3 += f" - 🏨 {hotel}"
            testo += f"🏢 *{ag_nome}*\n{riga2}\n{riga3}\n\n"
    return testo


# ── Funzioni interne di render: accettano guida_id esplicito ─────────────────
# Usate sia dagli handler guida che dal router in modalità simulazione.

async def _render_miei_oggi(update: Update, guida_id: str) -> None:
    oggi = oggi_str()
    dati = await get_data()
    servizi = [
        s for s in dati["servizi"]
        if s.get("data") == oggi
        and s.get("guidaId") == guida_id
        and servizio_contabile(s)
    ]
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    hotel_map   = _build_hotel_map(dati["servizi"])
    testo  = f"📅 *{data_estesa(oggi)}*\n`{SEP}`\n\n"
    testo += _build_miei_servizi(servizi, agenzie_map, hotel_map)
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


async def _render_mia_settimana(update: Update, guida_id: str) -> None:
    lun  = lunedi_str()
    dom  = domenica_str()
    dati = await get_data()
    servizi = [
        s for s in dati["servizi"]
        if lun <= (s.get("data") or "") <= dom
        and s.get("guidaId") == guida_id
        and servizio_contabile(s)
    ]
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    hotel_map   = _build_hotel_map(dati["servizi"])
    testo  = f"📆 *La mia settimana*\n`{SEP}`\n\n"
    testo += _build_miei_servizi(servizi, agenzie_map, hotel_map)
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


async def _render_mia_prossima(update: Update, guida_id: str) -> None:
    lun  = lunedi_prossimo_str()
    dom  = domenica_prossima_str()
    dati = await get_data()
    servizi = [
        s for s in dati["servizi"]
        if lun <= (s.get("data") or "") <= dom
        and s.get("guidaId") == guida_id
        and servizio_contabile(s)
    ]
    agenzie_map = {a["id"]: a for a in dati["agenzie"]}
    hotel_map   = _build_hotel_map(dati["servizi"])
    testo  = f"⏭️ *La mia prossima settimana*\n`{SEP}`\n\n"
    testo += _build_miei_servizi(servizi, agenzie_map, hotel_map)
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


async def _render_mio_mese_guida(update: Update, guida_id: str) -> None:
    mese = mese_corrente()
    oggi = oggi_str()
    dati = await get_data()
    ag_map = {a["id"]: a for a in dati["agenzie"]}
    guida  = next((g for g in dati["guide"] if g["id"] == guida_id), None)
    sv = sorted(
        [s for s in dati["servizi"] if s.get("guidaId") == guida_id
         and (s.get("data") or "").startswith(mese) and servizio_contabile(s)],
        key=lambda x: x.get("data", ""),
    )
    eseguiti = [s for s in sv if (s.get("data") or "") <= oggi]
    futuri   = [s for s in sv if (s.get("data") or "") > oggi]
    anno, mese_num = mese.split("-")
    nome  = guida["nome"] if guida else "Guida"
    testo = f"📋 *{nome} — {MESI_ITA[int(mese_num)]} {anno}*\n\n"
    testo += f"✅ Servizi svolti: *{len(eseguiti)}*\n"
    testo += f"📅 Prossimi: *{len(futuri)}*\n\n"
    if futuri:
        testo += "*Prossimi servizi:*\n"
        for s in futuri[:8]:
            ag    = ag_map.get(s.get("agenziaId") or "")
            label = s.get("tipoLabel") or s.get("tipo", "")
            riga  = f"  📅 {data_it(s['data'])} — {label}"
            if s.get("bus"):
                riga += f" Bus {s['bus']}"
            if ag:
                riga += f" ({ag['nome']})"
            testo += riga + "\n"
        if len(futuri) > 8:
            testo += f"  _...e altri {len(futuri)-8}_\n"
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ── Handler pubblici: autenticano e delegano ai render interni ────────────────

async def handle_miei_oggi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["guida"])
    if not utente:
        return
    guida_id = utente.get("guidaId")
    if not guida_id:
        await update.message.reply_text("⚠️ Account non collegato a nessuna guida. Contatta l'amministratore.")
        return
    await _render_miei_oggi(update, guida_id)


async def handle_mia_settimana(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["guida"])
    if not utente:
        return
    guida_id = utente.get("guidaId")
    if not guida_id:
        await update.message.reply_text("⚠️ Account non collegato a nessuna guida. Contatta l'amministratore.")
        return
    await _render_mia_settimana(update, guida_id)


async def handle_mia_prossima_settimana(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["guida"])
    if not utente:
        return
    guida_id = utente.get("guidaId")
    if not guida_id:
        await update.message.reply_text("⚠️ Account non collegato a nessuna guida. Contatta l'amministratore.")
        return
    await _render_mia_prossima(update, guida_id)


# ─── LISTA GUIDE (per abbinamento account) ───────────────────────────────────

async def cmd_listaguide(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra tutte le guide con il loro ID Firestore e lo stato abbinamento Telegram."""
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    dati   = await get_data()
    attivi = await get_utenti_attivi()

    # guidaId → telegram user abbinato
    abbinati: dict[str, dict] = {}
    for u in attivi:
        gid = u.get("guidaId")
        if gid:
            abbinati[gid] = u

    testo  = f"👥 *Guide — ID per abbinamento bot*\n`{SEP}`\n\n"
    testo += "Usa `/autorizza <tg_id> guida <guida_id>` per abbinare.\n\n"

    for g in sorted(dati["guide"], key=lambda x: x.get("nome", "")):
        gid    = g["id"]
        nome   = g.get("nome", "—")
        tel    = g.get("telefono", "")
        linked = abbinati.get(gid)

        if linked:
            tg_nome = linked.get("nome") or linked.get("telegramUsername") or "?"
            tg_id   = linked.get("telegramId", "?")
            stato   = f"✅ @{linked.get('telegramUsername') or tg_id} ({tg_nome})"
        else:
            stato = "⚪ non abbinata"

        testo += f"👤 *{nome}*"
        if tel:
            testo += f" · {tel}"
        testo += f"\n   ID: `{gid}`\n   {stato}\n\n"

    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── GESTISCI UTENTI ─────────────────────────────────────────────────────────

async def handle_gestisci_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE):
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    pending = await get_pending()
    attivi  = await get_utenti_attivi()

    testo  = "🔧 *Gestione Utenti*\n"
    testo += f"`{SEP}`\n\n"
    testo += f"👥 Utenti attivi: *{len(attivi)}*\n"

    if pending:
        testo += f"\n⏳ *Richieste in attesa ({len(pending)}):*\n"
        for u in pending:
            nome = u.get("nome") or "?"
            user = u.get("telegramUsername") or ""
            user_str = f" @{user}" if user else ""
            testo += f"  `{u.get('telegramId')}` — {nome}{user_str}\n"
        testo += (
            "\n*Per autorizzare:*\n"
            "`/autorizza <id> admin`\n"
            "`/autorizza <id> viewer`\n"
            "`/autorizza <id> guida <guida_id>`\n\n"
            "Per vedere gli ID guida: /listaguide\n"
        )
    else:
        testo += "\n✅ Nessuna richiesta in attesa.\n"

    testo += "\n`/disabilita <id>` — disabilita utente"
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


# ─── SIMULAZIONE GUIDA (solo admin) ─────────────────────────────────────────

async def handle_simula_guida(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra elenco guide con ID — solo admin."""
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return
    # Se è già in simulazione, mostra lo stato attuale
    uid  = update.effective_user.id
    gid_attivo = _simulazione_attiva.get(uid)
    dati  = await get_data()
    guide = sorted(dati["guide"], key=lambda x: x.get("nome", ""))
    testo = f"👁️ *Simulazione vista guida*\n`{SEP}`\n\n"
    if gid_attivo:
        g_att = next((g for g in guide if g["id"] == gid_attivo), None)
        nome_att = g_att["nome"] if g_att else gid_attivo
        testo += f"⚠️ Simulazione attiva: *{nome_att}*\nUsa `/esci` per tornare alla vista admin.\n\n"
    testo += "Usa `/simula <nome>` per entrare nella vista di una guida:\n\n"
    for g in guide:
        tel = f" · {esc(g['telefono'])}" if g.get("telefono") else ""
        testo += f"  *{g.get('nome','?')}*{tel}\n  `{g['id']}`\n\n"
    await update.message.reply_text(testo, parse_mode=ParseMode.MARKDOWN)


async def cmd_simula(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /simula <nome_o_id> — entra in modalità simulazione per quella guida.
    L'admin riceve la tastiera della guida e vede esattamente la sua vista.
    /esci per tornare alla vista admin.
    """
    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    uid  = update.effective_user.id
    args = context.args

    if not args:
        await handle_simula_guida(update, context)
        return

    cerca    = " ".join(args).strip().lower()
    dati     = await get_data()
    guide    = dati["guide"]

    # Prima cerca per ID esatto, poi per nome parziale
    guida = next((g for g in guide if g["id"] == cerca), None)
    if not guida:
        guida = next((g for g in guide if cerca in g.get("nome", "").lower()), None)

    if not guida:
        await update.message.reply_text(
            f"⚠️ Guida *{cerca}* non trovata. Premi *👁️ Simula Guida* per l'elenco.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    guida_id = guida["id"]
    nome     = guida.get("nome", guida_id)

    # Entra in simulazione
    _simulazione_attiva[uid] = guida_id

    await update.message.reply_text(
        f"👁️ *Modalità simulazione attiva*\n`{SEP}`\n\n"
        f"Stai vedendo il bot come lo vede *{nome}*.\n"
        f"Usa i pulsanti in basso per navigare.\n\n"
        f"Premi *🔚 Esci simulazione* o digita `/esci` per tornare alla vista admin.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=KB_SIMULA,
    )


async def cmd_esci(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Esce dalla modalità simulazione guida e ripristina la tastiera admin."""
    uid = update.effective_user.id
    if uid not in _simulazione_attiva:
        await update.message.reply_text("Nessuna simulazione attiva.")
        return
    del _simulazione_attiva[uid]
    utente = await get_utente(uid)
    role   = utente.get("role", "viewer") if utente else "viewer"
    await update.message.reply_text(
        "✅ Simulazione terminata. Sei tornato alla vista admin.",
        reply_markup=tastiera(role),
    )


# ─── STATISTICHE UTILIZZO (solo admin) ───────────────────────────────────────

async def handle_statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra le statistiche di utilizzo del bot degli ultimi 7 giorni — solo admin."""
    from collections import Counter

    utente = await _autenticato(update, ["admin"])
    if not utente:
        return

    await update.message.reply_text("⏳ Caricamento statistiche…")

    records = await get_uso_stats()

    if not records:
        await update.message.reply_text("📊 Nessun dato di utilizzo disponibile per gli ultimi 7 giorni.")
        return

    fn_count:   Counter = Counter()
    user_count: Counter = Counter()
    day_count:  Counter = Counter()
    nomi: dict[str, str] = {}

    for r in records:
        fn   = r.get("fn")   or "?"
        uid  = str(r.get("uid") or "?")
        nome = r.get("nome") or uid
        nomi[uid] = nome
        fn_count[fn]   += 1
        user_count[uid] += 1
        ts = r.get("ts")
        if ts:
            try:
                day = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
                day_count[day] += 1
            except Exception:
                pass

    total = sum(fn_count.values())

    msg  = f"📊 *Statistiche utilizzo bot*\n`{SEP}`\n\n"
    msg += f"*Totale richieste \\(7 gg\\):* {total}\n\n"

    msg += "*📌 Funzioni più usate:*\n"
    for fn, cnt in fn_count.most_common(12):
        bar = "█" * min(cnt, 12)
        msg += f"  `{bar}` {esc(fn)} — {cnt}\n"

    msg += f"\n*👤 Utenti più attivi:*\n"
    for uid_k, cnt in user_count.most_common(8):
        nome = esc(nomi.get(uid_k, uid_k))
        msg += f"  {nome} — {cnt}\n"

    if day_count:
        msg += f"\n*📅 Attività per giorno:*\n"
        for day in sorted(day_count)[-7:]:
            cnt = day_count[day]
            bar = "█" * min(cnt, 15)
            label = esc(data_bella(day))
            msg += f"  `{bar}` {label} — {cnt}\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ─── Router messaggi testuale (pulsanti tastiera) ─────────────────────────────
#
# SICUREZZA: le guide hanno una dispatch table SEPARATA e CHIUSA.
# Un utente con ruolo "guida" non può mai raggiungere handler admin/viewer
# indipendentemente dal testo che invia, anche digitandolo manualmente.

# Bottoni esclusivi per le guide (non presenti nella dispatch admin/viewer)
_DISPATCH_GUIDA = {
    "📅 I Miei Oggi":      handle_miei_oggi,
    "📆 La Mia Settimana": handle_mia_settimana,
    "⏭️ La Mia Prossima":  handle_mia_prossima_settimana,
    "📋 Il Mio Mese":      handle_mio_mese,
}

# Bottoni per admin e viewer
_DISPATCH_ADMIN = {
    "📅 Oggi":              handle_oggi,
    "📆 Settimana":         handle_settimana,
    "⏭️ Sett. Prossima":   handle_settimana_prossima,
    "🗓️ Weekend":          handle_weekend,
    "🟢 Guide Stato":      handle_guide_stato,
    "👥 Guide":            handle_guide,
    "🏢 Agenzie":          handle_agenzie,
    "💶 Fatturato":        handle_fatturato,
    "💵 Incassi Cash":     handle_incassi_cash,
    "⚠️ Alert":            handle_alert,
    "📊 Riepilogo Mese":   handle_riepilogo_mese,
    "📋 Mio Mese":         handle_mio_mese,
    "🔧 Gestisci utenti":  handle_gestisci_utenti,
    "👁️ Simula Guida":    handle_simula_guida,
    "📈 Statistiche":      handle_statistiche,
}


async def router_testo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt    = (update.message.text or "").strip()
    uid    = update.effective_user.id

    # ── Rate limit globale per utente ─────────────────────────────────────────
    if not _rate_ok(uid):
        await update.message.reply_text(
            "⏳ Stai inviando troppi comandi. Aspetta qualche secondo."
        )
        return

    utente = await get_utente(uid)

    # ── Guida reale: dispatch chiusa, solo vista propria ─────────────────────
    if utente and utente.get("role") == "guida":
        fn = _DISPATCH_GUIDA.get(txt)
        if fn:
            await log_uso(uid, utente.get("nome", ""), "guida", txt)
            await fn(update, context)
        return

    # ── Admin in modalità simulazione ────────────────────────────────────────
    guida_simulata = _simulazione_attiva.get(uid)
    if guida_simulata:
        nome_admin = (utente or {}).get("nome", "Admin")
        if txt == "🔚 Esci simulazione":
            await log_uso(uid, nome_admin, "admin_sim", f"ESCI:{guida_simulata}")
            await cmd_esci(update, context)
            return
        # Mappa bottoni guida → funzioni render interne con guida_id forzato
        _sim_dispatch = {
            "📅 I Miei Oggi":      _render_miei_oggi,
            "📆 La Mia Settimana": _render_mia_settimana,
            "⏭️ La Mia Prossima":  _render_mia_prossima,
            "📋 Il Mio Mese":      _render_mio_mese_guida,
        }
        fn = _sim_dispatch.get(txt)
        if fn:
            await log_uso(uid, nome_admin, "admin_sim", f"SIM:{guida_simulata}:{txt}")
            await fn(update, guida_simulata)
        return

    # ── Admin e viewer: dispatch standard ────────────────────────────────────
    fn = _DISPATCH_ADMIN.get(txt)
    if fn:
        ruolo = (utente or {}).get("role", "admin")
        nome  = (utente or {}).get("nome", "Admin")
        await log_uso(uid, nome, ruolo, txt)
        await fn(update, context)
