// server.js — Dashboard FTP Parser & API
const express = require('express');
const compression = require('compression');
const ftp = require('basic-ftp');
const { parse } = require('csv-parse/sync');
const path = require('path');
const fs = require('fs');
const { Writable } = require('stream');

// ==== VARIABILI D’AMBIENTE ====
const {
  FTP_HOST,
  FTP_USER,
  FTP_PASS,
  FTP_FILE,
  FTP_SECURE = 'false',
  CORS = 'false'
} = process.env;

// ==== FUNZIONI UTILI ====
const ITnum = (s) => {
  if (s == null) return null;
  const v = String(s).trim();
  if (!v) return null;
  if (/%$/.test(v)) {
    const n = ITnum(v.replace('%', ''));
    if (n === 999999) return null;
    return n;
  }
  const t = v.replace(/\./g, '').replace(/,/g, '.');
  if (/^[+-]?\d+(\.\d+)?$/.test(t)) return Number(t);
  return v;
};

// ==== FUNZIONE DI PARSING ====
function parseSmart(rawText) {
  const text = rawText.replace(/^\uFEFF/, '');

  // 1) prendi una riga "dati" utile
  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(l => l && !/^#{1,2}\s/.test(l));
  const sample = lines.find(l => /^(OBIETTIVI|CLIENTI_PROVINCIA|VENDITE_CLIENTE|CATEGORIA_RIEPILOGO|RIPARTIZIONE|TOTALE)\b/.test(l)) || lines[0] || '';

  // 2) auto-detect delimitatore
  const candidates = ['\t', ';', ','];
  let delimiter = '\t', bestCount = -1;
  for (const d of candidates) {
    const cnt = (sample.match(new RegExp(`\\${d}`, 'g')) || []).length;
    if (cnt > bestCount) { bestCount = cnt; delimiter = d; }
  }

  // 3) parse con relax_quotes per gestire virgolette
  const rowsArr = parse(text, {
    delimiter,
    relax_quotes: true,
    relax_column_count: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
    columns: false
  }).filter(arr => (arr.join(delimiter) || '').trim() && !/^#{1,2}\s/.test(arr[0] || ''));

  const isTipo = v => ['OBIETTIVI','CLIENTI_PROVINCIA','VENDITE_CLIENTE','CATEGORIA_RIEPILOGO','RIPARTIZIONE','TOTALE'].includes(String(v || '').trim());

  const out = [];
  for (let arr of rowsArr) {
    const tipo = (arr[0] || '').trim();
    if (!isTipo(tipo)) continue;

    if (tipo === 'OBIETTIVI') {
      out.push({
        Tipo_Dato: 'OBIETTIVI',
        Provincia: arr[1] ?? null,
        Anno_Precedente: ITnum(arr[2]),
        Anno_Corrente: ITnum(arr[3]),
        Obiettivo: ITnum(arr[4]),
        Percentuale_Obiettivo: ITnum(arr[5]),
        Clienti_Anno_Precedente: ITnum(arr[6]),
        Clienti_Anno_Corrente: ITnum(arr[7]),
        Obiettivo_Clienti: ITnum(arr[8]),
        Percentuale_Obiettivo_Clienti: ITnum(arr[9])
      });
      continue;
    }

    if (tipo === 'CLIENTI_PROVINCIA') {
      out.push({
        Tipo_Dato: 'CLIENTI_PROVINCIA',
        Provincia: arr[1] ?? null,
        Clienti_Serviti: ITnum(arr[2]),
        Fatturato: ITnum(arr[arr.length - 1])
      });
      continue;
    }

    if (tipo === 'VENDITE_CLIENTE') {
      const provincia = arr[1] ?? null;
      const fatt = arr[arr.length - 1];
      const nome = arr.slice(2, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'VENDITE_CLIENTE',
        Provincia: provincia,
        Ragione_Sociale_Cliente: nome,
        Fatturato: ITnum(fatt)
      });
      continue;
    }

    if (tipo === 'CATEGORIA_RIEPILOGO') {
      const importo = arr[arr.length - 1];
      const categoria = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'CATEGORIA_RIEPILOGO',
        Categoria: categoria || null,
        Importo_Totale: ITnum(importo)
      });
      continue;
    }

    if (tipo === 'RIPARTIZIONE') {
      const percent = arr[arr.length - 1];
      const importo = arr[arr.length - 2];
      const cat = arr.slice(1, arr.length - 2).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'RIPARTIZIONE',
        Categoria_Prodotto: cat || null,
        Importo: ITnum(importo),
        Percentuale: ITnum(percent)
      });
      continue;
    }

    if (tipo === 'TOTALE') {
      const valore = arr[arr.length - 1];
      const descr = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'TOTALE',
        Descrizione: descr || null,
        Valore: ITnum(valore)
      });
      continue;
    }
  }

  const columns = Array.from(out.reduce((s, r) => { for (const k in r) s.add(k); return s; }, new Set()));
  return { delimiter, columns, rowCount: out.length, rows: out };
}

// ==== DOWNLOAD FTP ====
async function downloadFromFtpAsText() {
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE)
    throw new Error('Mancano var. ambiente FTP_HOST/USER/PASS/FILE');

  const client = new ftp.Client(15000);
  client.ftp.verbose = false;

  const chunks = [];
  const sink = new Writable({
    write(chunk, enc, cb) { chunks.push(Buffer.from(chunk)); cb(); }
  });

  try {
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: /^true$/i.test(FTP_SECURE)
    });
    await client.downloadTo(sink, FTP_FILE);
    return Buffer.concat(chunks).toString('utf8');
  } finally {
    client.close();
  }
}

// ==== EXPRESS SERVER ====
const app = express();
app.disable('x-powered-by');
app.use(compression());
if (/^true$/i.test(CORS)) app.use(require('cors')());

// Serve index.html da /public o root
const publicDir = path.join(__dirname, 'public');
if (fs.existsSync(publicDir)) app.use(express.static(publicDir));
app.use(express.static(__dirname));

// HEALTH
app.get('/healthz', (req, res) => res.type('text/plain').send('ok'));

// RAW
app.get('/raw', async (req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    res.type('text/plain').send(txt);
  } catch (err) {
    res.status(500).json({ error: 'FTP error', detail: String(err?.message || err) });
  }
});

// DATA
app.get('/data', async (req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    const parsed = parseSmart(txt);
    res.json({ updatedAt: new Date().toISOString(), ...parsed });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err?.message || err) });
  }
});

// FALLBACK INDEX
app.get('*', (req, res) => {
  const idxPublic = path.join(publicDir, 'index.html');
  const idxRoot = path.join(__dirname, 'index.html');
  if (fs.existsSync(idxPublic)) return res.sendFile(idxPublic);
  if (fs.existsSync(idxRoot)) return res.sendFile(idxRoot);
  res.status(404).send('index.html non trovato');
});

// START
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`✅ Server avviato su http://localhost:${PORT}`));
