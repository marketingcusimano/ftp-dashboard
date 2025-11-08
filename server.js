// server.js — Dashboard Agenti • Cotto Cusimano
// Node 18+ (Render/Heroku/Local). Legge CSV "flat" dall'FTP (o file locale) e lo espone in JSON.

// ---------------------- Dipendenze ----------------------
const express = require('express');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');
const { parse } = require('csv-parse/sync');

// FTP client (usiamo basic-ftp)
const ftp = require('basic-ftp');

// ---------------------- Configurazione ----------------------
const PORT = process.env.PORT || 3000;

// Sorgente dati: per default usa FTP; opzionalmente si può puntare a un file locale
// Imposta queste variabili su Render:
const FTP_HOST = process.env.FTP_HOST;          // es: "ftp.dominosoluzioni.com"
const FTP_USER = process.env.FTP_USER;          // es: "cusimano"
const FTP_PASS = process.env.FTP_PASS;          // es: "PraService25!"
const FTP_PATH = process.env.FTP_PATH || "/generalb2b_flat.csv";  // file flat normalizzato

// In alternativa (per debug locali) puoi usare un file fisico
const LOCAL_FILE = process.env.LOCAL_FILE || "";  // es: "./generalb2b_flat.csv"

// CORS: CORS=true per permettere qualsiasi origine; oppure CORS_ORIGIN con lista domini separati da virgola
const CORS = process.env.CORS || "false";
const CORS_ORIGIN = process.env.CORS_ORIGIN || ""; // "https://www.cottocusimano.com,https://cottocusimano.com"

// Cache: (in-memory) semplice per stabilizzare frequenti richieste
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 60_000); // 60s
let cache = { raw: null, data: null, t: 0 };

// ---------------------- App ----------------------
const app = express();
app.disable('x-powered-by');
app.use(compression());
app.use(morgan('tiny'));

// CORS opzionale
if (/^true$/i.test(CORS)) {
  const cors = require('cors');
  if (CORS_ORIGIN) {
    const origins = CORS_ORIGIN.split(',').map(s => s.trim()).filter(Boolean);
    app.use(cors({ origin: origins, methods: ['GET', 'HEAD', 'OPTIONS'] }));
  } else {
    app.use(cors()); // Access-Control-Allow-Origin: *
  }
}

// ---------------------- Utils ----------------------

// Scarica il CSV (raw stringa) da FTP o da file locale
async function fetchRawCSV() {
  // Cache semplice
  const now = Date.now();
  if (cache.raw && (now - cache.t) < CACHE_TTL_MS) return cache.raw;

  let raw = "";
  if (LOCAL_FILE) {
    // Lettura da file locale
    const fs = require('fs');
    raw = fs.readFileSync(path.resolve(LOCAL_FILE), 'utf-8');
  } else {
    // Lettura da FTP
    if (!FTP_HOST || !FTP_USER || !FTP_PASS) {
      throw new Error("FTP env non configurato. Imposta FTP_HOST, FTP_USER, FTP_PASS.");
    }
    const client = new ftp.Client(30_000);
    client.ftp.verbose = false;
    try {
      await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: false });
      const stream = await client.downloadTo(Buffer.alloc(0), FTP_PATH); // NOTA: downloadTo richiede writable
      // basic-ftp non ritorna la stringa; usiamo un workaround con downloadToTemp:
      // Facciamo un vero download su buffer manualmente:
    } catch (e) {
      // fallback: leggi in memoria usando downloadTo con file temporaneo
      try {
        const os = require('os'); const fs = require('fs');
        const tmp = path.join(os.tmpdir(), 'generalb2b_flat.csv');
        const client2 = new ftp.Client(30_000);
        await client2.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: false });
        await client2.downloadTo(tmp, FTP_PATH);
        raw = fs.readFileSync(tmp, 'utf-8');
        await client2.close();
      } catch (err2) {
        throw new Error(`FTP download fallito: ${e.message || e} / ${err2.message || err2}`);
      }
      // chiudi primo client
      try { await client.close(); } catch {}
      // aggiorna cache e ritorna
      cache.raw = raw; cache.t = Date.now();
      return raw;
    }

    // Se siamo qui, abbiamo usato la prima modalità (stream) e dobbiamo rifarla correttamente:
    // Usiamo downloadTo in memoria con un Writable custom.
    const { Writable } = require('stream');
    let buffers = [];
    const sink = new Writable({
      write(chunk, enc, cb) { buffers.push(Buffer.from(chunk)); cb(); }
    });
    try {
      await client.downloadTo(sink, FTP_PATH);
      raw = Buffer.concat(buffers).toString('utf-8');
    } finally {
      try { await client.close(); } catch {}
    }
  }
  // salva in cache
  cache.raw = raw; cache.t = Date.now();
  return raw;
}

// Parser CSV flat -> JSON
function parseFlatCSV(raw) {
  // Usa csv-parse con prima riga come header
  const rows = parse(raw, {
    delimiter: ',',        // se passerai a TSV, cambia in '\t'
    columns: true,
    bom: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true
  });

  // Normalizza tipi numerici principali (se sono stringhe con punto/virgola già corrette non serve)
  // NB: se il file flat è già numerico, questa parte è innocua.
  const numFields = new Set([
    "Anno_Precedente","Anno_Corrente","Obiettivo","Percentuale_Obiettivo",
    "Clienti_Anno_Precedente","Clienti_Anno_Corrente","Obiettivo_Clienti",
    "Percentuale_Obiettivo_Clienti","Clienti_Serviti","Fatturato",
    "Importo_Totale","Importo","Percentuale","Valore"
  ]);

  const cleanNum = v => {
    if (v === null || v === undefined || v === "") return null;
    if (typeof v === 'number') return v;
    // Assumiamo già formato macchina (es. 1234.56); se avessi virgole, convertirle qui:
    const s = String(v).trim().replace(/\s+/g, '');
    if (!s) return null;
    // gestisci eventuali percentuali scritte con simbolo
    const s2 = s.replace('%','');
    const n = Number(s2);
    return Number.isFinite(n) ? n : null;
  };

  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (numFields.has(k)) {
        r[k] = cleanNum(r[k]);
      }
    }
  }

  return {
    updatedAt: new Date().toISOString(),
    rowCount: rows.length,
    columns: Object.keys(rows[0] || {}),
    rows
  };
}

// ---------------------- Routes ----------------------
app.get('/healthz', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

app.get('/raw', async (req, res) => {
  try {
    const raw = await fetchRawCSV();
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(raw);
  } catch (err) {
    res.status(500).json({ error: 'FTP/Read failed', detail: String(err && err.message || err) });
  }
});

app.get('/data', async (req, res) => {
  try {
    // Cache JSON (derivato)
    const now = Date.now();
    if (cache.data && (now - cache.t) < CACHE_TTL_MS) {
      return res.json(cache.data);
    }

    const raw = await fetchRawCSV();
    const json = parseFlatCSV(raw);

    cache.data = json; // sincronizza cache con stessa t di raw
    return res.json(json);
  } catch (err) {
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err && err.message || err) });
  }
});

// Static: serve index.html se presente nella root del progetto
app.use(express.static(path.join(__dirname, '/')));

// Fallback 404 (dopo static)
app.use((req, res) => {
  res.status(404).json({ error: 'Not Found', path: req.path });
});

// ---------------------- Avvio ----------------------
app.listen(PORT, () => {
  console.log(`Dashboard API running on http://localhost:${PORT}`);
  console.log(`Source: ${LOCAL_FILE ? 'LOCAL_FILE=' + LOCAL_FILE : `FTP ${FTP_HOST}${FTP_PATH}`}`);
});
