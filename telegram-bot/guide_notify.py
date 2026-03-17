"""
Notifica periodica per le guide — Gestionale Pro
Inviata ogni 2 giorni: ciascuna guida riceve i propri servizi dei prossimi 14 giorni.

Esecuzione manuale:
    python guide_notify.py

Schedulazione (cron ogni 2 giorni alle 9:00):
    0 9 */2 * * cd /path/to/telegram-bot && python guide_notify.py
"""

from __future__ import annotations
import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from telegram import Bot
from telegram.constants import ParseMode

from config import BOT_TOKEN, COLLECTIONS
from db import (
    get_data, get_firestore, servizio_contabile,
    data_bella, data_estesa, MESI_ITA, esc,
)

SEP = "—" * 20


def _hotel_per_servizio(s: dict, hotel_map: dict) -> str:
    if s.get("hotel"):
        return s["hotel"]
    key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
    if not key:
        return ""
    entries = hotel_map.get(key, [])
    data_s  = s.get("data") or ""
    result  = ""
    for data_h, hotel_h in entries:
        if data_h <= data_s:
            result = hotel_h
        else:
            break
    return result


def _build_hotel_map(servizi: list) -> dict:
    raw: dict[str, list] = {}
    for s in servizi:
        if s.get("hotel") and s.get("data"):
            key = s.get("gruppo") or f"bus_{s.get('bus')}_{s.get('agenziaId')}"
            if key:
                raw.setdefault(key, []).append((s["data"], s["hotel"]))
    return {k: sorted(v) for k, v in raw.items()}


def _ico_tipo(tipo: str) -> str:
    return {"NORMAL": "🥾", "ANREISE": "✈️", "HEIMREISE": "🏠"}.get(tipo, "📋")


def _build_riepilogo_guida(guida: dict, servizi_guida: list, ag_map: dict, hotel_map: dict) -> str:
    """Costruisce il messaggio di riepilogo per una singola guida."""
    oggi      = date.today().isoformat()
    nome      = guida.get("nome", "Guida")
    futuri    = [s for s in servizi_guida if (s.get("data") or "") >= oggi]
    futuri    = sorted(futuri, key=lambda x: x.get("data") or "")

    if not futuri:
        return ""   # nessun servizio nei prossimi 14 giorni → non inviare

    d_da = data_bella(futuri[0]["data"])
    d_a  = data_bella(futuri[-1]["data"])

    testo  = f"📆 *La tua agenda — {esc(nome)}*\n`{SEP}`\n"
    testo += f"_{d_da} → {d_a}_\n\n"

    per_giorno: dict[str, list] = {}
    for s in futuri:
        per_giorno.setdefault(s["data"], []).append(s)

    for i, giorno in enumerate(sorted(per_giorno)):
        if i > 0:
            testo += f"`{SEP}`\n"
        testo += f"*{data_bella(giorno)}*\n"
        for s in sorted(per_giorno[giorno], key=lambda x: (x.get("orarioPartenza") or "99", x.get("bus") or 0)):
            ag      = ag_map.get(s.get("agenziaId") or "")
            ag_nome = esc(ag["nome"]) if ag else "Sconosciuta"
            label   = esc(s.get("tipoLabel") or s.get("tipo", ""))
            hotel   = esc(_hotel_per_servizio(s, hotel_map))
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


async def send_guide_notifications() -> None:
    """Invia a ogni guida il riepilogo dei propri servizi per i prossimi 14 giorni."""
    print("📋 Caricamento dati Firestore...")
    dati = await get_data(force=True)

    oggi    = date.today().isoformat()
    fra_14  = (date.today() + timedelta(days=14)).isoformat()

    hotel_map = _build_hotel_map(dati["servizi"])
    ag_map    = {a["id"]: a for a in dati["agenzie"]}

    # Recupera gli account Telegram abbinati alle guide
    fs = get_firestore()
    tg_docs = fs.collection(COLLECTIONS["tg_users"]).where("abilitato", "==", True).limit(500).stream()
    guida_a_tg: dict[str, int] = {}   # guidaId → telegramId
    for doc in tg_docs:
        d = doc.to_dict() or {}
        if d.get("role") == "guida" and d.get("guidaId"):
            try:
                guida_a_tg[d["guidaId"]] = int(d["telegramId"])
            except (ValueError, KeyError):
                pass

    if not guida_a_tg:
        print("Nessuna guida con account Telegram abilitato.")
        return

    guide_map = {g["id"]: g for g in dati["guide"]}
    bot = Bot(token=BOT_TOKEN)
    inviati = 0

    for guida_id, tg_id in guida_a_tg.items():
        guida = guide_map.get(guida_id)
        if not guida:
            continue

        # Filtra servizi di questa guida nei prossimi 14 giorni
        servizi_guida = [
            s for s in dati["servizi"]
            if s.get("guidaId") == guida_id
            and oggi <= (s.get("data") or "") <= fra_14
            and servizio_contabile(s)
        ]

        msg = _build_riepilogo_guida(guida, servizi_guida, ag_map, hotel_map)
        if not msg:
            print(f"  → {guida.get('nome')}: nessun servizio, skip")
            continue

        try:
            await bot.send_message(chat_id=tg_id, text=msg, parse_mode=ParseMode.MARKDOWN)
            print(f"  ✅ Inviato a {guida.get('nome')} (tg:{tg_id})")
            inviati += 1
        except Exception as e:
            print(f"  ❌ Errore invio a {guida.get('nome')} (tg:{tg_id}): {e}")

    print(f"\n✅ Notifiche inviate: {inviati}/{len(guida_a_tg)}")


if __name__ == "__main__":
    asyncio.run(send_guide_notifications())
