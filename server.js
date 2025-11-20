// server.js — Dashboard Agenti (CommonJS) — robusto su FTP e path

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

// === ENV ===
const FTP_HOST   = process.env.FTP_HOST;                // es: ftp.dominosoluzioni.com
const FTP_USER   = process.env.FTP_USER;
const FTP_PASS   = process.env.FTP_PASS;
const FTP_PATH   = process.env.FTP_PATH || "generalb2b.csv"; // <-- nome storico (senza / davanti)
const FTP_SECURE = /^true$/i.test(process.env.FTP_SECURE || "false"); // true/false

const LOCAL_FILE = process.env.LOCAL_FILE || ""; // per test locale
const CORS       = process.env.CORS || "false";
const CORS_ORIGIN= process.env.CORS_ORIGIN || "";
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 60_000); // 60s

let cache = { raw: null, data: null, t: 0 };

const app = express();
app.disable('x-powered-by');
app.use(compression());
app.use(morgan('tiny'));

// CORS opzionale
if (/^true$/i.test(CORS)) {
  const cors = require('cors');
  if (CORS_ORIGIN) {
    const origins = CORS_ORIGIN.split(',').map(s => s.trim()).filter(Boolean);
    app.use(cors({ origin: origins, methods: ['GET','HEAD','OPTIONS'] }));
  } else {
    app.use(cors());
  }
}

/* ---------------- Utils ---------------- */

async function ftpDownloadToString({ host, user, password, secure }, remotePath) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  try {
    await client.access({ host, user, password, secure });
    let chunks = [];
    const sink = new Writable({
      write(chunk, enc, cb) { chunks.push(Buffer.from(chunk)); cb(); }
    });
    await client.downloadTo(sink, remotePath);
    return Buffer.concat(chunks).toString('utf-8');
  } finally {
    try { client.close(); } catch {}
  }
}

// Prova più varianti di path: "p", "/p" (e viceversa)
async function fetchFromFTPWithFallbacks() {
  if (!FTP_HOST || !FTP_USER || !FTP_PASS) {
    throw new Error("FTP non configurato: mancano FTP_HOST / FTP_USER / FTP_PASS.");
  }
  const creds = { host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: FTP_SECURE };
  const candidates = Array.from(new Set([
    FTP_PATH,
    FTP_PATH.startsWith('/') ? FTP_PATH.slice(1) : `/${FTP_PATH}`
  ]));

  const errors = [];
  for (const p of candidates) {
    try {
      return await ftpDownloadToString(creds, p);
    } catch (e) {
      errors.push(`${p}: ${e.message || e}`);
    }
  }
  throw new Error(`FTP download fallito. Tentativi: ${errors.join(' | ')}`);
}

// Scarica CSV (cache)
async function fetchRawCSV() {
  const now = Date.now();
  if (cache.raw && (now - cache.t) < CACHE_TTL_MS) return cache.raw;

  let raw = "";
  if (LOCAL_FILE) {
    raw = fs.readFileSync(path.resolve(LOCAL_FILE), 'utf-8');
  } else {
    raw = await fetchFromFTPWithFallbacks();
  }
  cache.raw = raw; cache.t = Date.now();
  return raw;
}

// Parser CSV flat (header in prima riga)
function parseFlatCSV(raw) {
  const rows = parse(raw, {
    delimiter: ',',         // se passi a TSV usa '\t'
    columns: true,
    bom: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true
  });

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

/* ---------------- Routes ---------------- */

app.get('/healthz', (req,res)=> res.json({ ok:true, ts:new Date().toISOString() }));

app.get('/raw', async (req,res)=>{
  try {
    const raw = await fetchRawCSV();
    res.setHeader('Content-Type','text/plain; charset=utf-8');
    res.send(raw);
  } catch (err) {
    res.status(500).json({ error: 'FTP/Read failed', detail: String(err && err.message || err) });
  }
});

app.get('/data', async (req,res)=>{
  try {
    const now = Date.now();
    if (cache.data && (now - cache.t) < CACHE_TTL_MS) return res.json(cache.data);
    const raw = await fetchRawCSV();

    // Hard check: se la prima riga NON ha header attesi, avvisa chiaramente
    const firstLine = (raw.split(/\r?\n/)[0] || '').toLowerCase();
    if (!/tipo_dato/.test(firstLine)) {
      throw new Error("Il CSV sull'FTP non è il 'flat' con intestazione unica. Carica 'generalb2b.csv' FLAT.");
    }

    const json = parseFlatCSV(raw);
    cache.data = json;
    res.json(json);
  } catch (err) {
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err && err.message || err) });
  }
});

// Static (serve index.html)
app.use(express.static(path.join(__dirname, '/')));

// 404
app.use((req,res)=> res.status(404).json({ error:'Not Found', path:req.path }));

/* ---------------- Start ---------------- */

app.listen(PORT, ()=>{
  console.log(`Server up on http://localhost:${PORT}`);
  console.log(`Source: ${LOCAL_FILE ? 'LOCAL_FILE='+LOCAL_FILE : `FTP ${FTP_HOST} | PATH=${FTP_PATH} | SECURE=${FTP_SECURE}`}`);
});
