// server.js — Dashboard Agenti (CommonJS) — Cotto Cusimano

const express = require('express');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');
const { parse } = require('csv-parse/sync');
const ftp = require('basic-ftp');
const fs = require('fs');
const os = require('os');
const { Writable } = require('stream');

const PORT = process.env.PORT || 3000;

// Manteniamo il nome storico del file per non toccare env su Render
const FTP_HOST = process.env.FTP_HOST;                 // es: ftp.dominosoluzioni.com
const FTP_USER = process.env.FTP_USER;                 // es: cusimano
const FTP_PASS = process.env.FTP_PASS;                 // es: ****
const FTP_PATH = process.env.FTP_PATH || "/generalb2b.csv"; // <-- carica il flat con questo nome

// Per test in locale opzionale: node server.js con LOCAL_FILE=./generalb2b.csv
const LOCAL_FILE = process.env.LOCAL_FILE || "";

const CORS = process.env.CORS || "false";
const CORS_ORIGIN = process.env.CORS_ORIGIN || ""; // "https://www.cottocusimano.com,https://cottocusimano.com"

const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 60_000); // 60s
let cache = { raw: null, data: null, t: 0 };

const app = express();
app.disable('x-powered-by');
app.use(compression());
app.use(morgan('tiny'));

// CORS (opzionale)
if (/^true$/i.test(CORS)) {
  const cors = require('cors');
  if (CORS_ORIGIN) {
    const origins = CORS_ORIGIN.split(',').map(s => s.trim()).filter(Boolean);
    app.use(cors({ origin: origins, methods: ['GET','HEAD','OPTIONS'] }));
  } else {
    app.use(cors()); // allow *
  }
}

/* ----------------------- Helpers ----------------------- */

// Scarica CSV in memoria (da FTP o da file locale). Cache semplice in-RAM.
async function fetchRawCSV() {
  const now = Date.now();
  if (cache.raw && (now - cache.t) < CACHE_TTL_MS) return cache.raw;

  let raw = "";
  if (LOCAL_FILE) {
    raw = fs.readFileSync(path.resolve(LOCAL_FILE), 'utf-8');
  } else {
    if (!FTP_HOST || !FTP_USER || !FTP_PASS) {
      throw new Error("FTP non configurato (mancano FTP_HOST/FTP_USER/FTP_PASS).");
    }
    const client = new ftp.Client(30000);
    client.ftp.verbose = false;

    try {
      await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: false });

      // Scarico in memoria con uno stream Writable
      let chunks = [];
      const sink = new Writable({
        write(chunk, enc, cb) { chunks.push(Buffer.from(chunk)); cb(); }
      });

      await client.downloadTo(sink, FTP_PATH);
      raw = Buffer.concat(chunks).toString('utf-8');
    } catch (err) {
      // Fallback su file temporaneo
      const tmp = path.join(os.tmpdir(), 'generalb2b.csv');
      try {
        await client.downloadTo(tmp, FTP_PATH);
        raw = fs.readFileSync(tmp, 'utf-8');
      } catch (err2) {
        throw new Error(`FTP download fallito: ${err.message} / ${err2.message}`);
      }
    } finally {
      try { client.close(); } catch {}
    }
  }
  cache.raw = raw; cache.t = Date.now();
  return raw;
}

// Parser CSV "flat" (prima riga = header). Delimiter: virgola (se passi a TSV usa '\t').
function parseFlatCSV(raw) {
  const rows = parse(raw, {
    delimiter: ',',          // cambia in '\t' se usi TSV
    columns: true,           // prima riga come header
    bom: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true
  });

  // Normalizzazione numeri (idempotente se già numerici)
  const numFields = new Set([
    "Anno_Precedente","Anno_Corrente","Obiettivo","Percentuale_Obiettivo",
    "Clienti_Anno_Precedente","Clienti_Anno_Corrente","Obiettivo_Clienti",
    "Percentuale_Obiettivo_Clienti","Clienti_Serviti","Fatturato",
    "Importo_Totale","Importo","Percentuale","Valore"
  ]);
  const cleanNum = v => {
    if (v === "" || v == null) return null;
    if (typeof v === 'number') return v;
    const n = Number(String(v).replace('%','').trim());
    return Number.isFinite(n) ? n : null;
  };
  for (const r of rows) for (const k in r) if (numFields.has(k)) r[k] = cleanNum(r[k]);

  return {
    updatedAt: new Date().toISOString(),
    rowCount: rows.length,
    columns: Object.keys(rows[0] || {}),
    rows
  };
}

/* ----------------------- Routes ----------------------- */

app.get('/healthz', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

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
    const now = Date.now();
    if (cache.data && (now - cache.t) < CACHE_TTL_MS) return res.json(cache.data);
    const raw = await fetchRawCSV();
    const json = parseFlatCSV(raw);
    cache.data = json; // sincronizza cache
    res.json(json);
  } catch (err) {
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err && err.message || err) });
  }
});

// Static (index.html nella root del repo)
app.use(express.static(path.join(__dirname, '/')));

// 404
app.use((req, res) => res.status(404).json({ error: 'Not Found', path: req.path }));

/* ----------------------- Start ----------------------- */

app.listen(PORT, () => {
  console.log(`Server up on http://localhost:${PORT}`);
  console.log(`Source: ${LOCAL_FILE ? 'LOCAL_FILE='+LOCAL_FILE : `FTP ${FTP_HOST}${FTP_PATH}`}`);
});
