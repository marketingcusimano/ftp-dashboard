// server.cjs — versione CommonJS, resta in ascolto su PORT
require('dotenv').config();
const express = require('express');
const fs = require('fs');
const os = require('os');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------- static ---------- */
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));
app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});
app.get('/healthz', (_, res) => res.send('ok'));

/* ---------- parsing robusto (NO virgola come separatore) ---------- */
const U_NBSP = /\u00A0/g;
const normalizeLine = s =>
  String(s ?? '')
    .replace(/^\uFEFF/, '')
    .replace(U_NBSP, ' ')
    .replace(/\t+/g, '\t')
    .replace(/\s+$/,'')
    .trim();

function pickDelimiterFromHeader(line) {
  const candidates = [/\t+/, / {2,}/, /;/, /\|/]; // mai la virgola
  const norm = line.replace(U_NBSP, ' ');
  let best = { rx: null, cols: 1 };
  for (const rx of candidates) {
    const arr = norm.split(rx).map(s => s.trim()).filter(Boolean);
    if (arr.length > best.cols) best = { rx, cols: arr.length };
  }
  return best.rx || /\t+| {2,}/;
}
const trimQuotes = s => s.replace(/^["']|["']$/g, '').replace(/\s+/g, ' ').trim();

function parseTextToRows(text) {
  const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').map(normalizeLine);
  const rows = [];
  let section = null, agenteCodice = null, agenteNome = null, periodo = null;

  // trova tutti gli header (righe con "Tipo_Dato")
  const headerIdx = [];
  for (let i=0;i<lines.length;i++){
    const L = lines[i];
    if (!L) continue;

    const mCod = L.match(/^#\s*Codice\s+Agente:\s*(.+)$/i);
    if (mCod){ agenteCodice = trimQuotes(mCod[1]); continue; }
    const mNom = L.match(/^#\s*Ragione\s+Sociale:\s*(.+)$/i);
    if (mNom){ agenteNome = trimQuotes(mNom[1]); continue; }
    const mPer = L.match(/^#\s*Periodo:\s*(.+)$/i);
    if (mPer){ periodo = trimQuotes(mPer[1]); continue; }

    const sec = L.match(/^##\s*(.+)$/);
    if (sec){ section = sec[1].trim(); continue; }

    if (/^#/.test(L)) continue;
    if (/tipo[_ ]?dato/i.test(L)) headerIdx.push({ i, sectionAt: section ?? null });
  }

  for (let h=0; h<headerIdx.length; h++){
    const start = headerIdx[h].i;
    const end   = (h+1<headerIdx.length ? headerIdx[h+1].i : lines.length);
    const sectionHere = headerIdx[h].sectionAt;

    const headerLine = lines[start];
    const delim = pickDelimiterFromHeader(headerLine);
    const header = headerLine.split(delim).map(s=>s.trim());
    if (!header.length || !/tipo[_ ]?dato/i.test(header[0])) continue;

    for (let r=start+1; r<end; r++){
      let L = lines[r];
      if (!L) continue;
      if (/^#/.test(L)) continue;
      if (/^##\s+/.test(L)) break;

      let cells = L.split(delim).map(s=>s.trim());
      if (cells.length < header.length) while (cells.length < header.length) cells.push('');
      else if (cells.length > header.length) cells = cells.slice(0, header.length);

      const obj = {};
      for (let c=0;c<header.length;c++){
        const key = (header[c]||'').trim(); if (!key) continue;
        let val = (cells[c] ?? '').replace(/^["']|["']$/g,'').trim();

        const isPercentKey = key.toLowerCase().includes('percentuale');

        if (/%\s*$/.test(val)){
          let n = val.replace('%','').replace(/\./g,'').replace(',', '.').trim();
          let num = Number(n);
          if (!Number.isFinite(num) || num>1000) num = null;
          obj[key] = num;
        } else if (/^[0-9.\s,]+$/.test(val)){
          let n = val.replace(/\./g,'').replace(',', '.').replace(/\s/g,'');
          let num = Number(n);
          if (!Number.isFinite(num)) num = null;
          if (num===999999 || num===99999900) num = null;
          if (isPercentKey && num>1000) num = null;
          obj[key] = num;
        } else {
          obj[key] = val || null;
        }
      }

      if (Object.keys(obj).length){
        if (sectionHere) obj['__Sezione'] = sectionHere;
        if (agenteCodice) obj['Agente_Codice'] = agenteCodice;
        if (agenteNome)   obj['Agente_Nome']   = agenteNome;
        if (periodo)      obj['Periodo']       = periodo;
        rows.push(obj);
      }
    }
  }
  return rows;
}

/* ---------- FTP ---------- */
async function fetchTextFromFtp() {
  const {
    FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
    FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME
  } = process.env;
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Mancano FTP_HOST, FTP_USER, FTP_PASS o FTP_FILE.');
  }

  const { Client } = require('basic-ftp');
  const client = new Client(Number(FTP_TIMEOUT || 25000));
  client.ftp.verbose = false;

  const tmp = path.join(os.tmpdir(), `generalb2b_${Date.now()}.txt`);
  try {
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: String(FTP_SECURE).toLowerCase() === 'true',
      secureOptions: {
        rejectUnauthorized: String(FTP_TLS_INSECURE).toLowerCase() === 'true' ? false : true,
        servername: FTP_TLS_SERVERNAME || FTP_HOST
      }
    });
    await client.downloadTo(tmp, FTP_FILE);
    return fs.readFileSync(tmp, 'utf8');
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch {}
  }
}

/* ---------- API ---------- */
app.get('/data', async (_req, res) => {
  try {
    const txt  = await fetchTextFromFtp();
    const rows = parseTextToRows(txt);
    res.set('Cache-Control','no-store');
    res.json({
      updatedAt: new Date().toISOString(),
      rowCount: rows.length,
      columns: rows.length ? Object.keys(rows[0]) : [],
      rows
    });
  } catch (e) {
    console.error('Errore /data:', e);
    res.status(500).json({ error:'Lettura/parse fallita', details:String(e?.message||e) });
  }
});

app.get('/raw', async (_req,res)=>{
  try { res.type('text/plain').send(await fetchTextFromFtp()); }
  catch(e){ res.status(500).send(String(e?.message||e)); }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server su http://0.0.0.0:${PORT}`);
});
