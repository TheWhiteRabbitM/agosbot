"""
Report mattutino automatico — Gestionale Pro
Inviato ogni giorno alle 8:00 agli amministratori.

Esecuzione manuale:
    python morning_report.py
"""

from __future__ import annotations
import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from telegram import Bot
from telegram.constants import ParseMode

from config import BOT_TOKEN, ADMIN_IDS, COLLECTIONS
from db import (
    get_data, get_firestore, servizio_contabile, eur,
    data_estesa, data_bella, MESI_ITA, esc,
)

def _build_hotel_map(servizi: list) -> dict:
    """Indice gruppo_key → lista ordinata (data, hotel) per tutti i servizi con hotel."""
    raw: dict[str, list] = {}
    for s in servizi:
        if s.get("hotel") and s.get("data"):
            key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
            if key:
                raw.setdefault(key, []).append((s["data"], s["hotel"]))
    return {k: sorted(v) for k, v in raw.items()}

def _hotel_gruppo(s: dict, hotel_map: dict) -> str:
    """
    Hotel valido per questo servizio alla sua data.
    Se il servizio ha hotel proprio → usalo (cambio hotel quel giorno).
    Altrimenti → hotel più recente del gruppo con data ≤ s['data'].
    """
    if s.get("hotel"):
        return s["hotel"]
    key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
    if not key:
        return ""
    entries = hotel_map.get(key, [])
    data_s = s.get("data") or ""
    result = ""
    for data_h, hotel_h in entries:
        if data_h <= data_s:
            result = hotel_h
        else:
            break
    return result

SEP = "—" * 20


# ─── Recupero destinatari admin ───────────────────────────────────────────────

async def _get_admin_ids() -> list[int]:
    ids: set[int] = set(ADMIN_IDS)
    try:
        fs = get_firestore()
        for doc in (
            fs.collection(COLLECTIONS["tg_users"])
            .where("role", "==", "admin")
            .where("abilitato", "==", True)
            .limit(100)
            .stream()
        ):
            uid = doc.to_dict().get("telegramId")
            if uid:
                try:
                    ids.add(int(uid))
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        print(f"⚠️  Errore lettura admin da Firestore: {e}")
    return list(ids)


# ─── Costruzione messaggio ────────────────────────────────────────────────────

def _build_report(dati: dict) -> str:
    oggi   = date.today().isoformat()
    domani = (date.today() + timedelta(days=1)).isoformat()
    tre_fa = (date.today() - timedelta(days=3)).isoformat()

    ag_map    = {a["id"]: a for a in dati["agenzie"]}
    guide_map = {g["id"]: g for g in dati["guide"]}

    sv_oggi   = [s for s in dati["servizi"] if s.get("data") == oggi   and servizio_contabile(s)]
    sv_domani = [s for s in dati["servizi"] if s.get("data") == domani and servizio_contabile(s)]

    guide_attive  = len({s["guidaId"] for s in sv_oggi if s.get("guidaId")})
    senza_guida   = [s for s in sv_oggi if not s.get("guidaId")]

    # ── Intestazione ──
    msg  = f"🌅 *Buongiorno! — {data_estesa(oggi)}*\n"
    msg += f"`{SEP}`\n\n"

    if not sv_oggi:
        msg += "_Nessun servizio programmato per oggi._\n\n"
    else:
        msg += f"📊 *{len(sv_oggi)} servizi · {guide_attive} guide attive*"
        if senza_guida:
            msg += f" · ⚠️ {len(senza_guida)} senza guida"
        msg += "\n\n"

        hotel_map = _build_hotel_map(dati["servizi"])

        for s in sorted(sv_oggi, key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
            ag      = ag_map.get(s.get("agenziaId") or "")
            ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
            guida   = guide_map.get(s.get("guidaId") or "")
            label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
            hotel   = esc(_hotel_gruppo(s, hotel_map))
            punto   = esc(s.get("punto_incontro") or "IN HTL")
            parti   = []
            if s.get("bus"):
                parti.append(f"Bus {esc(s['bus'])}")
            parti.append(esc(guida["nome"]) if guida else "⚠️ guida mancante")
            parti.append(label)
            riga2 = " - ".join(parti)
            if s.get("orarioPartenza"):
                riga2 += f"  🕐 {s['orarioPartenza']}"
            riga3 = f"📍 {punto}"
            if hotel:
                riga3 += f" - 🏨 {hotel}"
            msg += f"🏢 *{ag_nome}*\n{riga2}\n{riga3}\n\n"

    # ── Alert ──
    alerts: list[str] = []

    # Guide mancanti oggi
    for s in senza_guida:
        label  = s.get("tipoLabel") or s.get("tipo", "")
        bus    = f" Bus {s['bus']}" if s.get("bus") else ""
        ag     = ag_map.get(s.get("agenziaId") or "")
        ag_str = f" ({ag['nome']})" if ag else ""
        alerts.append(f"🔴 OGGI {label}{bus}{ag_str} — guida non assegnata")

    # Guide mancanti domani
    for s in [s for s in sv_domani if not s.get("guidaId")]:
        label  = s.get("tipoLabel") or s.get("tipo", "")
        bus    = f" Bus {s['bus']}" if s.get("bus") else ""
        alerts.append(f"🟡 DOMANI {label}{bus} — guida non assegnata")

    # Cash scaduti (>3 giorni non confermati)
    for a in dati["agenzie"]:
        if not a.get("incassoCash"):
            continue
        pendenti = [
            s for s in dati["servizi"]
            if s.get("agenziaId") == a["id"]
            and (s.get("data") or "") < tre_fa
            and not s.get("incassatoCash")
            and servizio_contabile(s)
        ]
        if pendenti:
            tot = sum(s.get("incasso", 0) or 0 for s in pendenti)
            alerts.append(f"🟠 Cash {a['nome']}: {len(pendenti)} da confermare ({eur(tot)})")

    # Fatture scadute
    fat_sc = [f for f in dati["fatture"] if f.get("stato") == "scaduta"]
    if fat_sc:
        tot_sc = sum(f.get("importo", 0) or 0 for f in fat_sc)
        alerts.append(f"🔴 {len(fat_sc)} fatture scadute ({eur(tot_sc)})")

    msg += f"`{SEP}`\n"
    if alerts:
        msg += f"⚠️ *Alert ({len(alerts)})*\n"
        for a in alerts:
            msg += f"  {a}\n"
    else:
        msg += "✅ *Tutto in ordine — nessun alert*\n"

    return msg


# ─── Invio ────────────────────────────────────────────────────────────────────

async def send_morning_report():
    print("📋 Caricamento dati Firestore...")
    dati = await get_data(force=True)

    msg = _build_report(dati)

    admin_ids = await _get_admin_ids()
    if not admin_ids:
        print("⚠️  Nessun admin trovato — report non inviato.")
        return

    bot  = Bot(token=BOT_TOKEN)
    sent = 0
    for uid in admin_ids:
        try:
            await bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.MARKDOWN)
            print(f"✅ Inviato a {uid}")
            sent += 1
        except Exception as e:
            print(f"❌ Errore invio a {uid}: {e}")

    print(f"📤 Report mattutino inviato a {sent}/{len(admin_ids)} admin.")


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(send_morning_report())
