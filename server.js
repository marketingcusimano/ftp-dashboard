// server.js — CommonJS — Cotto Cusimano

const express = require('express');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');
const { parse } = require('csv-parse/sync');
const ftp = require('basic-ftp');
const fs = require('fs');
const { Writable } = require('stream');

const PORT = process.env.PORT || 3000;

const FTP_HOST   = process.env.FTP_HOST;
const FTP_USER   = process.env.FTP_USER;
const FTP_PASS   = process.env.FTP_PASS;
const FTP_PATH   = process.env.FTP_PATH || "generalb2b.csv"; // FLAT
const FTP_SECURE = /^true$/i.test(process.env.FTP_SECURE || "false");

const LOCAL_FILE = process.env.LOCAL_FILE || "";
const CORS       = process.env.CORS || "false";
const CORS_ORIGIN= process.env.CORS_ORIGIN || "";
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 60_000);

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

/* ---------- Utils FTP ---------- */

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

async function fetchFromFTPWithFallbacks() {
  if (!FTP_HOST || !FTP_USER || !FTP_PASS) {
    throw new Error("FTP non configurato: mancano FTP_HOST / FTP_USER / FTP_PASS.");
  }

  const creds = { host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: FTP_SECURE };
  const candidates = Array.from(new Set([
    FTP_PATH,
    FTP_PATH.startsWith('/') ? FTP_PATH.slice(1) : `/${FTP_PATH}`
  ]));

  const errs = [];
  for (const p of candidates) {
    try {
      return await ftpDownloadToString(creds, p);
    } catch (e) {
      errs.push(`${p}: ${e.message || e}`);
    }
  }
  throw new Error(`FTP download fallito. Tentativi: ${errs.join(' | ')}`);
}

// Scarica CSV (con cache base)
async function fetchRawCSV() {
  const now = Date.now();
  if (cache.raw && (now - cache.t) < CACHE_TTL_MS) return cache.raw;

  let raw;
  if (LOCAL_FILE) {
    raw = fs.readFileSync(path.resolve(LOCAL_FILE), 'utf-8');
  } else {
    raw = await fetchFromFTPWithFallbacks();
  }

  cache.raw = raw;
  cache.t = Date.now();
  return raw;
}

/* ---------- Parsing & struttura /data ---------- */

function parseFlatCSV(raw) {
  // 1) parse grezzo mantenendo i NOMI DELLE COLONNE esattamente come nel CSV
  const rows = parse(raw, {
    delimiter: ',',     // se passi a TSV cambia in '\t'
    columns: true,      // header 1a riga
    bom: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true
  });

  // 2) normalizza SOLO i campi numerici (senza toccare i nomi)
  const numFields = new Set([
    "Anno_Precedente","Anno_Corrente","Obiettivo","Percentuale_Obiettivo",
    "Clienti_Anno_Precedente","Clienti_Anno_Corrente","Obiettivo_Clienti",
    "Percentuale_Obiettivo_Clienti","Clienti_Serviti","Fatturato",
    "Importo_Totale","Importo","Percentuale","Valore"
  ]);

  const cleanNum = v => {
    if (v === "" || v == null) return null;
    if (typeof v === "number") return v;
    const n = Number(String(v).replace('%','').trim().replace('.','').replace(',','.'));
    return Number.isFinite(n) ? n : null;
  };

  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (numFields.has(k)) r[k] = cleanNum(r[k]);
    }
  }

  // 3) garantisci che esista una riga OBIETTIVI / Generale con Anno_Precedente
  let obGenerale = rows.find(r => r.Tipo_Dato === "OBIETTIVI" && r.Provincia === "Generale");

  if (!obGenerale) {
    const tuttiOb = rows.filter(r => r.Tipo_Dato === "OBIETTIVI");
    if (tuttiOb.length) {
      const agg = {
        // chiavi esistenti + valori aggregati
        Tipo_Dato: "OBIETTIVI",
        Provincia: "Generale",
        Anno_Precedente: tuttiOb.reduce((s,r)=> s + (r.Anno_Precedente || 0), 0) || null,
        Anno_Corrente:   tuttiOb.reduce((s,r)=> s + (r.Anno_Corrente   || 0), 0) || null,
        Obiettivo:       tuttiOb.reduce((s,r)=> s + (r.Obiettivo       || 0), 0) || null,
        Percentuale_Obiettivo: null,
        Clienti_Anno_Precedente: tuttiOb.reduce((s,r)=> s + (r.Clienti_Anno_Precedente || 0), 0) || null,
        Clienti_Anno_Corrente:   tuttiOb.reduce((s,r)=> s + (r.Clienti_Anno_Corrente   || 0), 0) || null,
        Obiettivo_Clienti:       tuttiOb.reduce((s,r)=> s + (r.Obiettivo_Clienti       || 0), 0) || null,
        Percentuale_Obiettivo_Clienti: null
      };
      // copia eventuali altre colonne presenti negli OBIETTIVI (per non romperti il frontend)
      for (const extraKey of Object.keys(tuttiOb[0])) {
        if (!(extraKey in agg)) agg[extraKey] = null;
      }
      rows.push(agg);
      obGenerale = agg;
    }
  }

  // 4) calcola un piccolo summary per il trend (facoltativo per il frontend)
  let fattPrev = null;
  let fattCurr = null;
  if (obGenerale) {
    fattPrev = obGenerale.Anno_Precedente ?? null;
    fattCurr = obGenerale.Anno_Corrente   ?? null;
  }

  // 5) lista ordinata delle colonne effettive (case-sensitive dal CSV)
  const columns = Array.from(
    new Set(rows.flatMap(r => Object.keys(r)))
  );

  return {
    updatedAt: new Date().toISOString(),
    rowCount: rows.length,
    columns,           // elenco stringhe dei campi
    rows,              // array di oggetti come nel CSV
    summary: {         // opzionale, per calcolo rapido trend
      Fatturato_Anno_Precedente: fattPrev,
      Fatturato_Anno_Corrente:   fattCurr
    }
  };
}

/* ---------- Routes ---------- */

app.get('/healthz', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

app.get('/raw', async (_req, res) => {
  try {
    const raw = await fetchRawCSV();
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(raw);
  } catch (err) {
    res.status(500).json({ error: 'FTP/Read failed', detail: String(err && err.message || err) });
  }
});

app.get('/data', async (_req, res) => {
  try {
    const now = Date.now();
    if (cache.data && (now - cache.t) < CACHE_TTL_MS) return res.json(cache.data);

    const raw = await fetchRawCSV();
    const firstLine = (raw.split(/\r?\n/)[0] || '').toLowerCase();
    if (!/tipo_dato/.test(firstLine)) {
      throw new Error("Il CSV sull'FTP non è il FLAT con intestazione unica (Tipo_Dato,...).");
    }

    const json = parseFlatCSV(raw);
    cache.data = json;
    res.json(json);
  } catch (err) {
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err && err.message || err) });
  }
});

// static (index.html nella root)
app.use(express.static(path.join(__dirname, '/')));

app.use((req, res) => {
  res.status(404).json({ error: 'Not Found', path: req.path });
});

/* ---------- Start ---------- */

app.listen(PORT, () => {
  console.log(`Server up on http://localhost:${PORT}`);
  console.log(`Source: ${LOCAL_FILE ? 'LOCAL_FILE='+LOCAL_FILE : `FTP ${FTP_HOST} | PATH=${FTP_PATH} | SECURE=${FTP_SECURE}`}`);
});
