/**
 * Gestionale Pro — Bot Telegram
 * Connette al database Firestore e risponde a query su guide, gite, fatturato e incassi.
 *
 * Setup:
 *   1. cp .env.example .env  →  compila TELEGRAM_BOT_TOKEN e FIREBASE_SERVICE_ACCOUNT_PATH
 *   2. Scarica il service account da Firebase Console > Impostazioni > Account di servizio
 *   3. npm install
 *   4. node bot.js
 *
 * Ruoli supportati:
 *   admin  — accesso completo a tutte le statistiche
 *   guida  — solo i propri servizi e la propria agenda
 *   viewer — accesso in sola lettura a gite e guide (senza dati economici)
 *
 * Gli utenti vengono registrati nella collezione Firestore "telegram_users":
 *   { telegramId, telegramUsername, role, guidaId, nome, abilitato }
 * Un admin può aggiungere/modificare utenti direttamente dal gestionale web
 * oppure autorizzarli via bot con /autorizza <id> <ruolo>.
 */

'use strict';

// ─── Configurazione ──────────────────────────────────────────────────────────
// Supporta sia variabili d'ambiente native sia un file .env minimale
const fs = require('fs');
const path = require('path');

function loadEnv() {
    const envPath = path.join(__dirname, '.env');
    if (!fs.existsSync(envPath)) return;
    fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return;
        const idx = trimmed.indexOf('=');
        if (idx < 0) return;
        const key = trimmed.slice(0, idx).trim();
        const val = trimmed.slice(idx + 1).trim().replace(/^["']|["']$/g, '');
        if (!process.env[key]) process.env[key] = val;
    });
}
loadEnv();

const BOT_TOKEN   = process.env.TELEGRAM_BOT_TOKEN;
const ADMIN_IDS   = (process.env.ADMIN_TELEGRAM_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
const SA_PATH     = process.env.FIREBASE_SERVICE_ACCOUNT_PATH || './serviceAccount.json';
const SA_JSON_RAW = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

if (!BOT_TOKEN) {
    console.error('❌  TELEGRAM_BOT_TOKEN non configurato. Controlla il file .env');
    process.exit(1);
}

// ─── Firebase Admin ───────────────────────────────────────────────────────────
const admin = require('firebase-admin');

let serviceAccount;
if (SA_JSON_RAW) {
    serviceAccount = JSON.parse(SA_JSON_RAW);
} else {
    const saPath = path.resolve(__dirname, SA_PATH);
    if (!fs.existsSync(saPath)) {
        console.error('❌  Service account Firebase non trovato in:', saPath);
        console.error('    Scaricalo da Firebase Console → Impostazioni progetto → Account di servizio');
        process.exit(1);
    }
    serviceAccount = JSON.parse(fs.readFileSync(saPath, 'utf8'));
}

admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// ─── Telegram Bot ─────────────────────────────────────────────────────────────
const TelegramBot = require('node-telegram-bot-api');
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

console.log('🤖  Bot Telegram avviato. In ascolto...');

// ─── Helpers ──────────────────────────────────────────────────────────────────
function oggi()     { return new Date().toISOString().split('T')[0]; }
function domani()   { const d = new Date(); d.setDate(d.getDate()+1); return d.toISOString().split('T')[0]; }
function lunedi()   { const d = new Date(); d.setDate(d.getDate() - (d.getDay()||7) + 1); return d.toISOString().split('T')[0]; }
function domenica() { const d = new Date(); d.setDate(d.getDate() - (d.getDay()||7) + 7); return d.toISOString().split('T')[0]; }
function meseCorrente() { return new Date().toISOString().slice(0,7); }

function eur(n) { return '€' + Number(n||0).toLocaleString('it-IT', {minimumFractionDigits:2, maximumFractionDigits:2}); }

function dataIT(iso) {
    if (!iso) return '—';
    const [y,m,d] = iso.split('-');
    return `${d}/${m}/${y}`;
}

function giornoSettimana(iso) {
    const giorni = ['Dom','Lun','Mar','Mer','Gio','Ven','Sab'];
    return giorni[new Date(iso + 'T00:00:00').getDay()];
}

// Un servizio conta ai fini guida/fatturazione solo se:
//   NORMAL → sempre
//   ANREISE → solo se guidaNecessaria !== false
//   HEIMREISE → mai
function servizioContabile(s) {
    if (s.tipo === 'HEIMREISE') return false;
    if (s.tipo === 'ANREISE')   return s.guidaNecessaria !== false;
    return true;
}

// ─── Caricamento dati Firestore ───────────────────────────────────────────────
async function getData() {
    const [guide, servizi, agenzie, tours, fatture] = await Promise.all([
        db.collection('guide2026_guide').get(),
        db.collection('guide2026_servizi').get(),
        db.collection('guide2026_agenzie').get(),
        db.collection('guide2026_tours').get(),
        db.collection('guide2026_fatture').get(),
    ]);
    return {
        guide:   guide.docs.map(d => ({id:d.id, ...d.data()})),
        servizi: servizi.docs.map(d => ({id:d.id, ...d.data()})),
        agenzie: agenzie.docs.map(d => ({id:d.id, ...d.data()})),
        tours:   tours.docs.map(d => ({id:d.id, ...d.data()})),
        fatture: fatture.docs.map(d => ({id:d.id, ...d.data()})),
    };
}

// ─── Autenticazione utenti ────────────────────────────────────────────────────
async function getUtente(telegramId) {
    // Gli ADMIN_IDS nel .env hanno sempre accesso admin
    if (ADMIN_IDS.includes(String(telegramId))) {
        return { role: 'admin', abilitato: true, nome: 'Admin', guidaId: null };
    }
    const snap = await db.collection('telegram_users').doc(String(telegramId)).get();
    if (!snap.exists) return null;
    return snap.data();
}

async function registraRichiesta(msg) {
    const tid = String(msg.from.id);
    await db.collection('telegram_users_pending').doc(tid).set({
        telegramId:       tid,
        telegramUsername: msg.from.username || '',
        nome:             (msg.from.first_name||'') + ' ' + (msg.from.last_name||''),
        richiestaAt:      admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
}

// ─── Tastiera comandi per ruolo ───────────────────────────────────────────────
function keyboardAdmin() {
    return {
        keyboard: [
            ['📅 Oggi',       '📆 Settimana'],
            ['👥 Guide',       '🏢 Agenzie'],
            ['💶 Fatturato',   '💵 Incassi Cash'],
            ['⚠️ Alert',       '📊 Riepilogo Mese'],
            ['🔧 Gestisci utenti'],
        ],
        resize_keyboard: true,
    };
}

function keyboardGuida() {
    return {
        keyboard: [
            ['📅 Oggi',     '📆 Settimana'],
            ['📋 Mio Mese'],
        ],
        resize_keyboard: true,
    };
}

function keyboardViewer() {
    return {
        keyboard: [
            ['📅 Oggi', '📆 Settimana'],
            ['👥 Guide'],
        ],
        resize_keyboard: true,
    };
}

// ─── Comando /start ───────────────────────────────────────────────────────────
bot.onText(/\/start/, async msg => {
    const utente = await getUtente(msg.from.id);
    if (!utente) {
        await registraRichiesta(msg);
        return bot.sendMessage(msg.chat.id,
            `👋 Ciao *${msg.from.first_name}*!\n\n` +
            `La tua richiesta di accesso è stata registrata.\n` +
            `Un amministratore dovrà autorizzarti prima che tu possa usare il bot.\n\n` +
            `Il tuo ID Telegram è: \`${msg.from.id}\`\n` +
            `Comunicalo all'amministratore per essere abilitato.`,
            { parse_mode: 'Markdown' }
        );
    }
    if (!utente.abilitato) {
        return bot.sendMessage(msg.chat.id, '⛔ Il tuo account è disabilitato. Contatta l\'amministratore.');
    }
    const kb = utente.role === 'admin' ? keyboardAdmin() : utente.role === 'guida' ? keyboardGuida() : keyboardViewer();
    bot.sendMessage(msg.chat.id,
        `✅ Bentornato *${utente.nome || msg.from.first_name}*!\nRuolo: *${utente.role}*\n\nUsa i pulsanti per navigare.`,
        { parse_mode: 'Markdown', reply_markup: kb }
    );
});

// ─── Autorizzazione utenti (solo admin) ───────────────────────────────────────
bot.onText(/\/autorizza (\d+) (admin|guida|viewer)(?:\s+(\S+))?/, async (msg, match) => {
    const utente = await getUtente(msg.from.id);
    if (!utente || utente.role !== 'admin') return bot.sendMessage(msg.chat.id, '⛔ Solo gli admin possono autorizzare utenti.');
    const targetId = match[1];
    const ruolo    = match[2];
    const guidaId  = match[3] || null;
    await db.collection('telegram_users').doc(targetId).set({
        telegramId: targetId,
        role: ruolo,
        guidaId: guidaId,
        abilitato: true,
        autorizzatoDa: String(msg.from.id),
        autorizzatoAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    bot.sendMessage(msg.chat.id, `✅ Utente ${targetId} autorizzato come *${ruolo}*${guidaId ? ` (guida: ${guidaId})` : ''}.`, { parse_mode: 'Markdown' });
});

bot.onText(/\/disabilita (\d+)/, async (msg, match) => {
    const utente = await getUtente(msg.from.id);
    if (!utente || utente.role !== 'admin') return bot.sendMessage(msg.chat.id, '⛔ Solo gli admin.');
    await db.collection('telegram_users').doc(match[1]).set({ abilitato: false }, { merge: true });
    bot.sendMessage(msg.chat.id, `🚫 Utente ${match[1]} disabilitato.`);
});

// ─── Middleware autenticazione ────────────────────────────────────────────────
async function autenticato(msg, ruoliConsentiti, fn) {
    const utente = await getUtente(msg.from.id);
    if (!utente) {
        return bot.sendMessage(msg.chat.id,
            `⛔ Non sei registrato. Scrivi /start per richiedere l'accesso.\nIl tuo ID: \`${msg.from.id}\``,
            { parse_mode: 'Markdown' }
        );
    }
    if (!utente.abilitato) {
        return bot.sendMessage(msg.chat.id, '⛔ Account disabilitato.');
    }
    if (ruoliConsentiti && !ruoliConsentiti.includes(utente.role)) {
        return bot.sendMessage(msg.chat.id, '⛔ Non hai i permessi per questo comando.');
    }
    return fn(utente);
}

// ─── Risposta ai messaggi testuali (pulsanti tastiera) ────────────────────────
bot.on('message', async msg => {
    if (!msg.text || msg.text.startsWith('/')) return;
    const txt = msg.text.trim();

    if (txt === '📅 Oggi')             return handleOggi(msg);
    if (txt === '📆 Settimana')        return handleSettimana(msg);
    if (txt === '👥 Guide')            return handleGuide(msg);
    if (txt === '🏢 Agenzie')          return handleAgenzie(msg);
    if (txt === '💶 Fatturato')        return handleFatturato(msg);
    if (txt === '💵 Incassi Cash')     return handleIncassiCash(msg);
    if (txt === '⚠️ Alert')           return handleAlert(msg);
    if (txt === '📊 Riepilogo Mese')   return handleRiepilogoMese(msg);
    if (txt === '📋 Mio Mese')         return handleMioMese(msg);
    if (txt === '🔧 Gestisci utenti')  return handleGestisciUtenti(msg);
});

// ─── OGGI ─────────────────────────────────────────────────────────────────────
async function handleOggi(msg) {
    await autenticato(msg, null, async utente => {
        const data   = await getData();
        const data_  = oggi();
        let servizi  = data.servizi.filter(s => s.data === data_ && servizioContabile(s));

        // Le guide vedono solo i propri servizi
        if (utente.role === 'guida' && utente.guidaId) {
            servizi = servizi.filter(s => s.guidaId === utente.guidaId);
        }

        if (servizi.length === 0) {
            return bot.sendMessage(msg.chat.id, `📅 *Oggi ${dataIT(data_)}*\n\nNessun servizio in programma.`, { parse_mode: 'Markdown' });
        }

        let testo = `📅 *Oggi — ${dataIT(data_)}*\n\n`;
        servizi.forEach(s => {
            const guida   = data.guide.find(g => g.id === s.guidaId);
            const agenzia = data.agenzie.find(a => a.id === s.agenziaId);
            const tipo    = s.tipo === 'ANREISE' ? '✈️ Arrivo' : '🥾 Gita';
            testo += `${tipo} *${s.tipoLabel || s.tipo}*\n`;
            if (s.bus)      testo += `   🚌 Bus ${s.bus}\n`;
            if (agenzia)    testo += `   🏢 ${agenzia.nome}\n`;
            if (s.hotel)    testo += `   🏨 ${s.hotel}\n`;
            if (guida)      testo += `   👤 ${guida.nome}\n`;
            else            testo += `   ⚠️ _Guida non assegnata_\n`;
            if (s.orarioPartenza) testo += `   🕐 ${s.orarioPartenza}\n`;
            testo += '\n';
        });

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── SETTIMANA ────────────────────────────────────────────────────────────────
async function handleSettimana(msg) {
    await autenticato(msg, null, async utente => {
        const data  = await getData();
        const lun   = lunedi();
        const dom   = domenica();
        let servizi = data.servizi
            .filter(s => s.data >= lun && s.data <= dom && servizioContabile(s))
            .sort((a,b) => a.data.localeCompare(b.data));

        if (utente.role === 'guida' && utente.guidaId) {
            servizi = servizi.filter(s => s.guidaId === utente.guidaId);
        }

        if (servizi.length === 0) {
            return bot.sendMessage(msg.chat.id, '📆 Nessun servizio questa settimana.', { parse_mode: 'Markdown' });
        }

        // Raggruppa per giorno
        const perGiorno = {};
        servizi.forEach(s => {
            if (!perGiorno[s.data]) perGiorno[s.data] = [];
            perGiorno[s.data].push(s);
        });

        let testo = `📆 *Settimana ${dataIT(lun)} – ${dataIT(dom)}*\n\n`;
        Object.entries(perGiorno).forEach(([giorno, svs]) => {
            testo += `*${giornoSettimana(giorno)} ${dataIT(giorno)}*\n`;
            svs.forEach(s => {
                const guida = data.guide.find(g => g.id === s.guidaId);
                const tipo  = s.tipo === 'ANREISE' ? '✈️' : '🥾';
                const guidaNome = guida ? guida.nome.split(' ')[0] : '⚠️ TBD';
                testo += `  ${tipo} ${s.tipoLabel || s.tipo}`;
                if (s.bus) testo += ` · Bus ${s.bus}`;
                testo += ` · ${guidaNome}\n`;
            });
            testo += '\n';
        });

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── GUIDE ────────────────────────────────────────────────────────────────────
async function handleGuide(msg) {
    await autenticato(msg, ['admin', 'viewer'], async () => {
        const data = await getData();
        const oggiStr = oggi();
        const domStr  = domani();

        let testo = `👥 *Guide — Situazione Odierna*\n\n`;
        data.guide.sort((a,b) => (a.nome||'').localeCompare(b.nome||'')).forEach(g => {
            const svOggi = data.servizi.filter(s => s.data === oggiStr && s.guidaId === g.id && servizioContabile(s));
            const svDom  = data.servizi.filter(s => s.data === domStr  && s.guidaId === g.id && servizioContabile(s));
            const stato  = svOggi.length > 0 ? '🟢 Attivo' : '⚪ Libero';
            testo += `${stato} *${g.nome}*`;
            if (g.telefono) testo += ` — ${g.telefono}`;
            testo += '\n';
            svOggi.forEach(s => testo += `   📍 ${s.tipoLabel || s.tipo}${s.bus ? ' Bus '+s.bus : ''}\n`);
            if (svDom.length > 0) testo += `   📅 Domani: ${svDom.map(s => s.tipoLabel||s.tipo).join(', ')}\n`;
            testo += '\n';
        });

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── AGENZIE ──────────────────────────────────────────────────────────────────
async function handleAgenzie(msg) {
    await autenticato(msg, ['admin'], async () => {
        const data   = await getData();
        const oggiStr = oggi();

        let testo = `🏢 *Agenzie — Situazione*\n\n`;
        data.agenzie.sort((a,b) => (a.nome||'').localeCompare(b.nome||'')).forEach(a => {
            const svAttivi = data.servizi.filter(s => s.agenziaId === a.id && s.data >= oggiStr && servizioContabile(s));
            const isCash   = a.incassoCash;
            testo += isCash ? `💵 *${a.nome}* (CASH)\n` : `🧾 *${a.nome}*\n`;
            testo += `   Servizi in corso/futuri: ${svAttivi.length}\n`;

            if (isCash) {
                const svEseguiti   = data.servizi.filter(s => s.agenziaId === a.id && s.data < oggiStr && servizioContabile(s));
                const totEseguiti  = svEseguiti.reduce((s, x) => s + (x.incasso||0), 0);
                const totConfermati = svEseguiti.filter(s => s.incassatoCash).reduce((s,x) => s+(x.incasso||0), 0);
                testo += `   ✅ Incassato cash: ${eur(totConfermati)}\n`;
                testo += `   ⏳ Da incassare: ${eur(totEseguiti - totConfermati)}\n`;
            } else {
                const fattureAperte = data.fatture.filter(f => f.agenziaId === a.id && (f.stato === 'emessa' || f.stato === 'scaduta'));
                const totAperte = fattureAperte.reduce((s,f) => s+(f.importo||0), 0);
                if (totAperte > 0) testo += `   🧾 Fatture aperte: ${eur(totAperte)}\n`;
            }
            testo += '\n';
        });

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── FATTURATO ────────────────────────────────────────────────────────────────
async function handleFatturato(msg) {
    await autenticato(msg, ['admin'], async () => {
        const data = await getData();
        const mese = meseCorrente();

        const fattureMese = data.fatture.filter(f => (f.dataEmissione||f.createdAt||'').startsWith(mese));
        const totEmesse   = fattureMese.filter(f => f.stato !== 'pagata').reduce((s,f) => s+(f.importo||0), 0);
        const totPagate   = fattureMese.filter(f => f.stato === 'pagata').reduce((s,f) => s+(f.importo||0), 0);
        const totScadute  = fattureMese.filter(f => f.stato === 'scaduta').reduce((s,f) => s+(f.importo||0), 0);

        // Incassi cash del mese
        const serviziMese = data.servizi.filter(s => (s.data||'').startsWith(mese) && servizioContabile(s));
        const totCash     = serviziMese.filter(s => s.incassatoCash).reduce((s,x) => s+(x.incasso||0), 0);
        const totDaCash   = serviziMese.filter(s => {
            const ag = data.agenzie.find(a => a.id === s.agenziaId);
            return ag && ag.incassoCash && !s.incassatoCash;
        }).reduce((s,x) => s+(x.incasso||0), 0);

        const [anno, meseNum] = mese.split('-');
        const nomiMesi = ['','Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'];

        let testo = `💶 *Fatturato — ${nomiMesi[+meseNum]} ${anno}*\n\n`;
        testo += `📤 Fatture emesse: ${eur(totEmesse+totPagate)}\n`;
        testo += `✅ Incassato bonifico: ${eur(totPagate)}\n`;
        testo += `⏳ Da incassare (fattura): ${eur(totEmesse)}\n`;
        if (totScadute > 0) testo += `🔴 Di cui scadute: ${eur(totScadute)}\n`;
        testo += `\n💵 Incassato cash: ${eur(totCash)}\n`;
        if (totDaCash > 0) testo += `⏳ Da incassare cash: ${eur(totDaCash)}\n`;
        testo += `\n💰 *Totale incassato: ${eur(totPagate + totCash)}*\n`;

        // Top 3 agenzie per volume
        const perAgenzia = {};
        data.fatture.filter(f => (f.dataEmissione||'').startsWith(mese)).forEach(f => {
            perAgenzia[f.agenziaId] = (perAgenzia[f.agenziaId]||0) + (f.importo||0);
        });
        const topAg = Object.entries(perAgenzia).sort((a,b) => b[1]-a[1]).slice(0,3);
        if (topAg.length > 0) {
            testo += `\n📊 *Top agenzie:*\n`;
            topAg.forEach(([agId, tot]) => {
                const ag = data.agenzie.find(a => a.id === agId);
                testo += `  • ${ag ? ag.nome : agId}: ${eur(tot)}\n`;
            });
        }

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── INCASSI CASH ─────────────────────────────────────────────────────────────
async function handleIncassiCash(msg) {
    await autenticato(msg, ['admin'], async () => {
        const data    = await getData();
        const oggiStr = oggi();
        const agCash  = data.agenzie.filter(a => a.incassoCash);

        if (agCash.length === 0) {
            return bot.sendMessage(msg.chat.id, '💵 Nessuna agenzia configurata per incasso cash.');
        }

        let testo = `💵 *Incassi Cash — Situazione*\n\n`;
        agCash.forEach(a => {
            const svEseguiti = data.servizi.filter(s => s.agenziaId === a.id && s.data && s.data < oggiStr && servizioContabile(s));
            const totTotale  = svEseguiti.reduce((s,x) => s+(x.incasso||0), 0);
            const confermati = svEseguiti.filter(s => s.incassatoCash);
            const totConf    = confermati.reduce((s,x) => s+(x.incasso||0), 0);
            const daIncassare = totTotale - totConf;

            testo += `🏢 *${a.nome}*\n`;
            testo += `   Totale servizi: ${eur(totTotale)}\n`;
            testo += `   ✅ Incassato: ${eur(totConf)}\n`;
            if (daIncassare > 0) {
                testo += `   🔴 Da incassare: *${eur(daIncassare)}*\n`;
            } else {
                testo += `   🟢 Tutto incassato\n`;
            }
            testo += '\n';
        });

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── ALERT ────────────────────────────────────────────────────────────────────
async function handleAlert(msg) {
    await autenticato(msg, ['admin'], async () => {
        const data    = await getData();
        const oggiStr = oggi();
        const domStr  = domani();
        const alerts  = [];

        // Gite oggi/domani senza guida
        const svUrg = data.servizi.filter(s =>
            (s.data === oggiStr || s.data === domStr) &&
            !s.guidaId &&
            servizioContabile(s)
        );
        svUrg.forEach(s => {
            const quando = s.data === oggiStr ? 'OGGI' : 'DOMANI';
            alerts.push(`🔴 *[${quando}]* ${s.tipoLabel||s.tipo}${s.bus?' Bus '+s.bus:''} — guida non assegnata`);
        });

        // Fatture scadute
        const fatScadute = data.fatture.filter(f => f.stato === 'scaduta');
        fatScadute.forEach(f => {
            const ag = data.agenzie.find(a => a.id === f.agenziaId);
            alerts.push(`🟠 Fattura *${f.numero||f.id}* scaduta — ${ag?ag.nome:'?'} (${eur(f.importo)})`);
        });

        // Incassi cash in attesa da più di 3 giorni
        const treGiorniFa = new Date(); treGiorniFa.setDate(treGiorniFa.getDate()-3);
        const cutoff = treGiorniFa.toISOString().split('T')[0];
        data.agenzie.filter(a => a.incassoCash).forEach(a => {
            const inAttesa = data.servizi.filter(s =>
                s.agenziaId === a.id && s.data && s.data < cutoff && !s.incassatoCash && servizioContabile(s)
            );
            if (inAttesa.length > 0) {
                const tot = inAttesa.reduce((s,x)=>s+(x.incasso||0),0);
                alerts.push(`🟡 *${a.nome}* — ${inAttesa.length} servizi cash non confermati (${eur(tot)})`);
            }
        });

        if (alerts.length === 0) {
            return bot.sendMessage(msg.chat.id, '✅ *Nessun alert attivo!* Tutto in ordine.', { parse_mode: 'Markdown' });
        }

        let testo = `⚠️ *Alert (${alerts.length})*\n\n` + alerts.join('\n');
        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── RIEPILOGO MESE ───────────────────────────────────────────────────────────
async function handleRiepilogoMese(msg) {
    await autenticato(msg, ['admin'], async () => {
        const data    = await getData();
        const mese    = meseCorrente();
        const oggiStr = oggi();

        const serviziMese = data.servizi.filter(s =>
            s.data && s.data.startsWith(mese) && s.data <= oggiStr && servizioContabile(s)
        );
        const totServizi    = serviziMese.length;
        const totIncasso    = serviziMese.reduce((s,x)=>s+(x.incasso||0),0);
        const senzaGuida    = serviziMese.filter(s => !s.guidaId).length;
        const guide_attive  = [...new Set(serviziMese.map(s=>s.guidaId).filter(Boolean))].length;

        const [anno, meseNum] = mese.split('-');
        const nomiMesi = ['','Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'];

        let testo = `📊 *Riepilogo ${nomiMesi[+meseNum]} ${anno}*\n\n`;
        testo += `🥾 Servizi eseguiti: *${totServizi}*\n`;
        testo += `👤 Guide attive: *${guide_attive}*\n`;
        if (senzaGuida > 0) testo += `⚠️ Senza guida assegnata: *${senzaGuida}*\n`;
        testo += `\n💰 Volume totale: *${eur(totIncasso)}*\n`;

        // Per agenzia
        const perAg = {};
        serviziMese.forEach(s => {
            if (!perAg[s.agenziaId]) perAg[s.agenziaId] = { n:0, tot:0 };
            perAg[s.agenziaId].n++;
            perAg[s.agenziaId].tot += (s.incasso||0);
        });
        const top = Object.entries(perAg).sort((a,b)=>b[1].tot-a[1].tot).slice(0,5);
        if (top.length > 0) {
            testo += `\n*Per agenzia:*\n`;
            top.forEach(([agId, v]) => {
                const ag = data.agenzie.find(a=>a.id===agId);
                testo += `  • ${ag?ag.nome:agId}: ${v.n} servizi — ${eur(v.tot)}\n`;
            });
        }

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── MIO MESE (solo per guide) ────────────────────────────────────────────────
async function handleMioMese(msg) {
    await autenticato(msg, ['guida', 'admin'], async utente => {
        const data    = await getData();
        const mese    = meseCorrente();
        const oggiStr = oggi();
        const guidaId = utente.guidaId;

        if (!guidaId) return bot.sendMessage(msg.chat.id, '⚠️ Nessuna guida associata al tuo account.');

        const guida = data.guide.find(g => g.id === guidaId);
        const servizi = data.servizi.filter(s =>
            s.guidaId === guidaId && s.data && s.data.startsWith(mese) && servizioContabile(s)
        ).sort((a,b) => a.data.localeCompare(b.data));

        const eseguiti = servizi.filter(s => s.data <= oggiStr);
        const futuri   = servizi.filter(s => s.data > oggiStr);

        const [anno, meseNum] = mese.split('-');
        const nomiMesi = ['','Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'];

        let testo = `📋 *${guida ? guida.nome : 'La mia agenda'} — ${nomiMesi[+meseNum]} ${anno}*\n\n`;
        testo += `✅ Servizi svolti: *${eseguiti.length}*\n`;
        testo += `📅 Prossimi: *${futuri.length}*\n\n`;

        if (futuri.length > 0) {
            testo += `*Prossimi servizi:*\n`;
            futuri.slice(0,7).forEach(s => {
                const ag = data.agenzie.find(a => a.id === s.agenziaId);
                testo += `  📅 ${dataIT(s.data)} — ${s.tipoLabel||s.tipo}`;
                if (s.bus) testo += ` Bus ${s.bus}`;
                if (ag) testo += ` (${ag.nome})`;
                testo += '\n';
            });
            if (futuri.length > 7) testo += `  _...e altri ${futuri.length-7}_\n`;
        }

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── GESTISCI UTENTI (solo admin) ────────────────────────────────────────────
async function handleGestisciUtenti(msg) {
    await autenticato(msg, ['admin'], async () => {
        const pending = await db.collection('telegram_users_pending').get();
        const attivi  = await db.collection('telegram_users').get();

        let testo = `🔧 *Gestione Utenti*\n\n`;
        testo += `👥 Utenti attivi: *${attivi.size}*\n`;

        if (pending.size > 0) {
            testo += `\n⏳ *Richieste in attesa (${pending.size}):*\n`;
            pending.docs.forEach(d => {
                const u = d.data();
                testo += `  ID: \`${u.telegramId}\` — ${u.nome||'?'} (@${u.telegramUsername||'?'})\n`;
            });
            testo += `\nPer autorizzare:\n\`/autorizza <id> <admin|guida|viewer>\`\n`;
            testo += `Per guida: \`/autorizza <id> guida <guidaId>\`\n`;
        } else {
            testo += `\n✅ Nessuna richiesta in attesa.\n`;
        }

        testo += `\nPer disabilitare un utente:\n\`/disabilita <id>\`\n`;

        bot.sendMessage(msg.chat.id, testo, { parse_mode: 'Markdown' });
    });
}

// ─── Comandi slash ────────────────────────────────────────────────────────────
bot.onText(/\/oggi/,      msg => handleOggi(msg));
bot.onText(/\/settimana/, msg => handleSettimana(msg));
bot.onText(/\/guide/,     msg => handleGuide(msg));
bot.onText(/\/agenzie/,   msg => handleAgenzie(msg));
bot.onText(/\/fatturato/, msg => handleFatturato(msg));
bot.onText(/\/incassi/,   msg => handleIncassiCash(msg));
bot.onText(/\/alert/,     msg => handleAlert(msg));
bot.onText(/\/mese/,      msg => handleRiepilogoMese(msg));

// ─── Errori di polling ────────────────────────────────────────────────────────
bot.on('polling_error', err => console.error('Polling error:', err.message));

console.log('✅  Bot pronto. Comandi disponibili:');
console.log('   /oggi /settimana /guide /agenzie /fatturato /incassi /alert /mese');
console.log('   /autorizza <id> <ruolo>  — (admin) abilita un utente');
console.log('   /disabilita <id>         — (admin) disabilita un utente');
