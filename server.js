import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

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

/* ---------- helpers ---------- */
const splitLines = t =>
  t.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');

const looksLikeHeader = cells => {
  const k = (cells[0] || '').toLowerCase().replace(/[\s_]/g, '');
  return k.startsWith('tipodato');
};
const sectionTitle = line => {
  const m = line.trim().match(/^##\s*(.+)$/);
  return m ? m[1].trim() : null;
};
const trimQuotes = s => s.replace(/^["']|["']$/g, '').replace(/\s+/g, ' ').trim();

function euroToNumber(s) {
  if (s == null) return null;
  const t = String(s).replace(/\./g, '').replace(',', '.').replace(/\s/g, '');
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

/** Sceglie 1 separatore a partire dall'header (NO virgola). */
function pickDelimiterFromHeader(line) {
  const norm = line.replace(/\u00A0/g, ' ');
  const candidates = [/\t+/, / {2,}/, /;/, /\|/]; // <-- niente virgola!
  let best = { rx: /\t+/, cols: 1, arr: [norm] };
  for (const rx of candidates) {
    const arr = norm.split(rx).map(s => s.trim());
    const cols = arr.filter(Boolean).length;
    if (cols > best.cols) best = { rx, cols, arr };
  }
  return best.rx;
}

/** Parser multi-sezione + metadata Agente, con separatore coerente per tabella */
function parseTextToRows(text) {
  const lines = splitLines(text);

  let section = null;
  let header = null;
  let delimiter = null; // scelto sull'header e riusato per le righe
  let agenteCodice = null, agenteNome = null, periodo = null;

  const rows = [];

  for (const raw0 of lines) {
    const raw = raw0 ?? '';
    const line = raw.trim().replace(/^\uFEFF/, '');
    if (!line) continue;

    // --- metadata agente dai commenti ---
    const mCod = line.match(/^#\s*Codice\s+Agente:\s*(.+)$/i);
    if (mCod) { agenteCodice = trimQuotes(mCod[1]); continue; }
    const mNome = line.match(/^#\s*Ragione\s+Sociale:\s*(.+)$/i);
    if (mNome) { agenteNome = trimQuotes(mNome[1]); continue; }
    const mPer = line.match(/^#\s*Periodo:\s*(.+)$/i);
    if (mPer) { periodo = trimQuotes(mPer[1]); continue; }

    // --- titolo sezione ---
    const sec = sectionTitle(line);
    if (sec) { section = sec; header = null; delimiter = null; continue; }

    // altre righe di commento -> skip
    if (line.startsWith('#')) continue;

    // se non ho ancora header, provo a individuarlo e scelgo il delimitatore
    if (!header) {
      const rx = pickDelimiterFromHeader(raw);
      const cells = raw.replace(/\u00A0/g, ' ').split(rx).map(s => s.trim());
      if (looksLikeHeader(cells)) {
        header = cells;
        delimiter = rx;         // <---- scelto qui
        continue;
      } else {
        // non è header e non ho header -> salto
        continue;
      }
    }

    // ho un header e un delimiter: split coerente
    const cells = raw.replace(/\u00A0/g, ' ').split(delimiter).map(s => s.trim());

    // se la riga ha meno colonne dell'header, padding
    if (cells.length < header.length) {
      while (cells.length < header.length) cells.push('');
    }

    // se ne ha molte di più è sintomo di linea sporca: tronco alle colonne dell’header
    if (cells.length > header.length) {
      cells.length = header.length;
    }

    // costruzione oggetto
    const obj = {};
    for (let i = 0; i < header.length; i++) {
      const key = (header[i] || '').trim();
      if (!key) continue;
      let val = trimQuotes((cells[i] ?? '').toString());

      const isPercentKey = key.toLowerCase().includes('percentuale');

      if (/%\s*$/.test(val)) {
        // percentuali 0–100
        let n = val.replace('%','').trim().replace(/\./g,'').replace(',', '.');
        let num = Number(n);
        if (!Number.isFinite(num) || num > 1000) num = null;
        val = num;
      } else if (/^[0-9.\s,]+$/.test(val)) {
        let num = euroToNumber(val);
        if (num === 999999 || num === 99999900) num = null;
        if (isPercentKey && num > 1000) num = null;
        val = num;
      }
      obj[key] = val;
    }
    if (!Object.keys(obj).length) continue;

    if (section) obj['__Sezione'] = section;
    if (agenteCodice) obj['Agente_Codice'] = agenteCodice;
    if (agenteNome)   obj['Agente_Nome']   = agenteNome;
    if (periodo)      obj['Periodo']       = periodo;

    rows.push(obj);
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

/* ---------- API ---------- */
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
