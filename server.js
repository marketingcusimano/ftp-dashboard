import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------- Static ---------- */
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));
app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});
app.get('/healthz', (_, res) => res.send('ok'));

/* ---------- Parser multi-sezione (TAB o spazi) ---------- */
const splitLines = t => t.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
const isDataLine = line => {
  const t = line.trim();
  return t !== '' && !t.startsWith('#') && !t.startsWith('##') && (/\t/.test(line) || / {2,}/.test(line));
};
const smartSplit = line => line.replace(/\u00A0/g, ' ').split(/\t+| {2,}/).map(s => s.trim());
const sectionTitleFrom = line => {
  const m = line.match(/^##\s*(.+)$/); return m ? m[1].trim() : null;
};
function parseMultiSectionFlexible(text) {
  const lines = splitLines(text);
  let currentSection = null, currentHeader = null;
  const headerRegex = /^tipo[_\s]?dato$/i;
  const rows = [];

  for (const raw of lines) {
    const maybe = sectionTitleFrom(raw.trim());
    if (maybe) { currentSection = maybe; continue; }
    if (!isDataLine(raw)) continue;

    const parts = smartSplit(raw);
    const first = (parts[0] || '').trim();

    if (!currentHeader && headerRegex.test(first)) { currentHeader = parts; continue; }
    if (headerRegex.test(first)) { currentHeader = parts; continue; }

    if (currentHeader) {
      const obj = {};
      for (let i = 0; i < currentHeader.length; i++) {
        const k = (currentHeader[i] || '').trim();
        if (!k) continue;
        obj[k] = (parts[i] ?? '').toString().trim();
      }
      if (currentSection) obj['__Sezione'] = currentSection;
      rows.push(obj);
    }
  }
  return rows;
}

// normalizza numeri “italiani” -> float JS
function parseEuroNumber(s) {
  if (s == null) return null;
  const t = String(s).replace(/\./g, '').replace(',', '.').replace(/\s/g, '');
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

/* ---------- FTP fetch + parse ---------- */
async function fetchRows() {
  const { FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE, FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME } = process.env;
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE)
    throw new Error('Variabili FTP mancanti. Imposta FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE.');

  const { Client } = await import('basic-ftp');
  const client = new Client(Number(FTP_TIMEOUT || 25000));
  client.ftp.verbose = false;

  const tmp = path.join(os.tmpdir(), `generalb2b_${Date.now()}.txt`);
  try {
    await client.access({
      host: FTP_HOST, user: FTP_USER, password: FTP_PASS,
      secure: String(FTP_SECURE).toLowerCase() === 'true',
      secureOptions: {
        rejectUnauthorized: String(FTP_TLS_INSECURE).toLowerCase() === 'true' ? false : true,
        servername: FTP_TLS_SERVERNAME || FTP_HOST
      }
    });
    await client.downloadTo(tmp, FTP_FILE);
    const text = fs.readFileSync(tmp, 'utf8');
    const rows = parseMultiSectionFlexible(text);

    // tipizzazione base: per colonne note prova a convertire a numero
    const numKeys = new Set([
      'Anno_Precedente','Anno_Corrente','Obiettivo','Percentuale_Obiettivo',
      'Clienti_Anno_Precedente','Clienti_Anno_Corrente','Obiettivo_Clienti',
      'Percentuale_Obiettivo_Clienti','Fatturato','Importo_Totale','Importo','Percentuale','Valore'
    ]);
    for (const r of rows) {
      for (const k of Object.keys(r)) {
        if (numKeys.has(k)) {
          const v = parseEuroNumber(r[k]);
          if (v != null) r[k] = v;
        }
      }
    }
    return rows;
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch {}
  }
}

/* ---------- API ---------- */
app.get('/data', async (_req, res) => {
  try {
    const rows = await fetchRows();
    res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({ updatedAt: new Date().toISOString(), rows, rowCount: rows.length, columns: rows[0] ? Object.keys(rows[0]) : [] });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: "Impossibile leggere il CSV dall'FTP", details: String(err?.message || err) });
  }
});

/* ---------- Start ---------- */
app.listen(PORT, '0.0.0.0', () => console.log(`Server avviato su http://0.0.0.0:${PORT}`));
