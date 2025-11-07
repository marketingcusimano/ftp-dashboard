import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------------- Static ---------------- */
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));
app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});
app.get('/healthz', (_, res) => res.send('ok'));

/* ---------------- Helpers parsing ---------------- */
const splitLines = t =>
  t.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');

function bestSplit(line) {
  const norm = line.replace(/\u00A0/g, ' ');
  const cand = [/\t+/, / {2,}/, /;/, /,/, /\|/];
  let bestArr = [norm], bestCount = 1;
  for (const rx of cand) {
    const arr = norm.split(rx).map(s => s.trim());
    const cnt = arr.filter(Boolean).length;
    if (cnt > bestCount) { bestArr = arr; bestCount = cnt; }
  }
  return bestArr;
}

const isComment = l => {
  const t = l.trim().replace(/^\uFEFF/, '');
  return !t || t.startsWith('#');
};
const looksLikeHeader = cells => {
  const k = (cells[0] || '').toLowerCase().replace(/[\s_]/g, '');
  return k.startsWith('tipodato');
};
const sectionTitle = line => {
  const m = line.trim().match(/^##\s*(.+)$/);
  return m ? m[1].trim() : null;
};

function euroToNumber(s) {
  if (s == null) return null;
  const t = String(s).replace(/\./g, '').replace(',', '.').replace(/\s/g, '');
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

/** Parser multi-sezione robusto: restituisce array di oggetti tipizzati. */
function parseTextToRows(text) {
  const lines = splitLines(text);
  let section = null, header = null;
  const rows = [];

  for (const raw of lines) {
    if (isComment(raw)) continue;

    const sec = sectionTitle(raw);
    if (sec) { section = sec; header = null; continue; }

    const cells = bestSplit(raw);

    // header
    if (!header && looksLikeHeader(cells)) { header = cells; continue; }
    if (header && looksLikeHeader(cells)) { header = cells; continue; }
    if (!header) continue;

    const obj = {};
    for (let i = 0; i < header.length; i++) {
      const key = (header[i] || '').trim();
      if (!key) continue;
      let val = (cells[i] ?? '').toString().trim();
      val = val.replace(/^["']|["']$/g, '').replace(/\s+/g, ' ').trim();

      const isPercentKey = key.toLowerCase().includes('percentuale');

      if (/%\s*$/.test(val)) {
        // percentuale in 0–100 (es. "66,51%") -> 66.51
        let n = val.replace('%', '').trim().replace(/\./g, '').replace(',', '.');
        let num = Number(n);
        if (!Number.isFinite(num) || num > 1000) num = null;
        val = num;
      } else if (/^[0-9.,]+$/.test(val)) {
        let n = val.replace(/\./g, '').replace(',', '.');
        let num = Number(n);
        if (!Number.isFinite(num)) num = null;
        if (num === 999999 || num === 99999900) num = null; // placeholder
        if (isPercentKey && num > 1000) num = null;
        val = num;
      }

      obj[key] = val;
    }

    if (Object.keys(obj).length) {
      if (section) obj['__Sezione'] = section;
      rows.push(obj);
    }
  }

  return rows;
}

/* ---------------- FTP fetch + parse ---------------- */
async function fetchTextFromFtp() {
  const {
    FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
    FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME
  } = process.env;

  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Mancano FTP_HOST, FTP_USER, FTP_PASS o FTP_FILE.');
  }

  const { Client } = await import('basic-ftp');
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

/* ---------------- API ---------------- */
// Grezzo (per debug): mostra il testo del file
app.get('/raw', async (_req, res) => {
  try {
    const txt = await fetchTextFromFtp();
    res.set('Cache-Control', 'no-store');
    res.type('text/plain').send(txt);
  } catch (e) {
    console.error('Errore /raw:', e);
    res.status(500).send(String(e?.message || e));
  }
});

// Pulito (per la dashboard): JSON tipizzato
app.get('/data', async (_req, res) => {
  try {
    const txt = await fetchTextFromFtp();
    const rows = parseTextToRows(txt);
    res.set('Cache-Control', 'no-store');
    res.json({
      updatedAt: new Date().toISOString(),
      rowCount: rows.length,
      columns: rows.length ? Object.keys(rows[0]) : [],
      rows
    });
  } catch (e) {
    console.error('Errore /data:', e);
    res.status(500).json({ error: 'Lettura/parse fallita', details: String(e?.message || e) });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server su http://0.0.0.0:${PORT}`);
});
