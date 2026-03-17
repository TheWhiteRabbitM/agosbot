"""
Gestionale Pro — Bot Telegram
Punto di ingresso principale.

Setup rapido:
    1. cp .env.example .env          # configura token e admin IDs
    2. Inserisci serviceAccount.json  # da Firebase Console → Account di servizio
    3. pip install -r requirements.txt
    4. python bot.py
"""

import asyncio
import logging
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application, ApplicationBuilder, CommandHandler, MessageHandler, filters

import config  # valida BOT_TOKEN al momento dell'import
from db import start_cache_listeners, register_assignment_callback, get_telegram_id_per_guida
from db import get_data, data_bella, servizio_contabile
from handlers import (
    cmd_start, cmd_autorizza, cmd_disabilita, cmd_mioid, cmd_ruolo,
    cmd_listaguide,
    handle_oggi, handle_settimana, handle_settimana_prossima,
    handle_weekend, handle_guide_stato,
    handle_guide, handle_agenzie,
    handle_fatturato, handle_incassi_cash, handle_alert,
    handle_riepilogo_mese, handle_mio_mese, handle_gestisci_utenti,
    handle_miei_oggi, handle_mia_settimana, handle_mia_prossima_settimana,
    handle_simula_guida, cmd_simula, cmd_esci,
    router_testo,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# Event loop del bot — valorizzato in post_init, usato dalla callback Firestore
_bot_loop: asyncio.AbstractEventLoop | None = None
_bot_instance: Bot | None = None


# ─── Notifica assegnazione servizio ──────────────────────────────────────────

async def _invia_notifica_assegnazione(svc: dict, guida_id: str) -> None:
    """
    Invia un messaggio Telegram alla guida quando le viene assegnato un servizio.
    Chiamata dal loop del bot tramite run_coroutine_threadsafe.
    """
    if _bot_instance is None:
        return

    tg_id = get_telegram_id_per_guida(guida_id)
    if not tg_id:
        return  # guida senza account Telegram registrato

    # Recupera nome guida e agenzia dalla cache (già in RAM a questo punto)
    dati      = await get_data()
    guide_map = {g["id"]: g for g in dati["guide"]}
    ag_map    = {a["id"]: a for a in dati["agenzie"]}

    guida = guide_map.get(guida_id)
    nome  = guida["nome"] if guida else "Guida"

    data   = svc.get("data", "")
    label  = svc.get("tipoLabel") or svc.get("tipo", "Servizio")
    bus    = f" · Bus {svc['bus']}" if svc.get("bus") else ""
    ag     = ag_map.get(svc.get("agenziaId") or "")
    ag_str = f"\n🏢 {ag['nome']}" if ag else ""
    ora    = f"\n🕐 {svc['orarioPartenza']}" if svc.get("orarioPartenza") else ""
    hotel  = svc.get("hotel") or ""
    hotel_str = f"\n🏨 {hotel}" if hotel else ""
    punto  = svc.get("punto_incontro") or ""
    punto_str = f"\n📍 {punto}" if punto else ""

    testo = (
        f"📬 *Nuovo servizio assegnato*\n"
        f"`{'—' * 20}`\n\n"
        f"📅 {data_bella(data)}\n"
        f"🏷️ {label}{bus}"
        f"{ag_str}{ora}{hotel_str}{punto_str}"
    )

    try:
        await _bot_instance.send_message(
            chat_id=tg_id,
            text=testo,
            parse_mode=ParseMode.MARKDOWN,
        )
        log.info(f"[notify] assegnazione inviata a guida {guida_id} (tg:{tg_id})")
    except Exception as e:
        log.warning(f"[notify] errore invio a {tg_id}: {e}")


def _on_assegnazione_thread(svc: dict, guida_id: str) -> None:
    """
    Callback chiamata dal thread Firestore (non asyncio).
    Schedula la coroutine di notifica nel loop del bot.
    """
    if _bot_loop is None or not _bot_loop.is_running():
        return
    asyncio.run_coroutine_threadsafe(
        _invia_notifica_assegnazione(svc, guida_id),
        _bot_loop,
    )


# ─── Post-init: avvia listener dopo che il loop è pronto ─────────────────────

async def _post_init(app: Application) -> None:
    """Eseguita da python-telegram-bot dopo che il loop asyncio è operativo."""
    global _bot_loop, _bot_instance
    _bot_loop     = asyncio.get_event_loop()
    _bot_instance = app.bot

    register_assignment_callback(_on_assegnazione_thread)
    start_cache_listeners()
    log.info("✅ Cache listeners Firestore avviati.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    app = (
        ApplicationBuilder()
        .token(config.BOT_TOKEN)
        .post_init(_post_init)
        .build()
    )

    # Comandi slash
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("mioid",        cmd_mioid))
    app.add_handler(CommandHandler("ruolo",        cmd_ruolo))
    app.add_handler(CommandHandler("autorizza",    cmd_autorizza))
    app.add_handler(CommandHandler("disabilita",   cmd_disabilita))
    app.add_handler(CommandHandler("oggi",         handle_oggi))
    app.add_handler(CommandHandler("settimana",    handle_settimana))
    app.add_handler(CommandHandler("prossima",     handle_settimana_prossima))
    app.add_handler(CommandHandler("weekend",      handle_weekend))
    app.add_handler(CommandHandler("guidestato",   handle_guide_stato))
    app.add_handler(CommandHandler("guide",        handle_guide))
    app.add_handler(CommandHandler("agenzie",      handle_agenzie))
    app.add_handler(CommandHandler("fatturato",    handle_fatturato))
    app.add_handler(CommandHandler("incassi",      handle_incassi_cash))
    app.add_handler(CommandHandler("alert",        handle_alert))
    app.add_handler(CommandHandler("mese",         handle_riepilogo_mese))
    app.add_handler(CommandHandler("miomese",      handle_mio_mese))
    app.add_handler(CommandHandler("listaguide",   cmd_listaguide))
    app.add_handler(CommandHandler("utenti",       handle_gestisci_utenti))
    # Comandi slash per le guide (alternativa ai pulsanti tastiera)
    app.add_handler(CommandHandler("simula",       cmd_simula))
    app.add_handler(CommandHandler("esci",         cmd_esci))
    app.add_handler(CommandHandler("mieiogg",      handle_miei_oggi))
    app.add_handler(CommandHandler("miasett",      handle_mia_settimana))
    app.add_handler(CommandHandler("miaprossima",  handle_mia_prossima_settimana))

    # Pulsanti tastiera (messaggi testuali)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router_testo))

    log.info("🤖  Bot avviato. In ascolto...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    main()
