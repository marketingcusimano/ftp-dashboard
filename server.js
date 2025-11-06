import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3000;

// Static dir (public se esiste, altrimenti root)
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));

app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});

app.get('/healthz', (_, res) => res.send('ok'));

// --- Helpers ---------------------------------------------------------

function splitLines(text) {
  return text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
}

// se la riga ha TAB e non è vuota, non è commento
function isDataLine(line) {
  const t = line.trim();
  return t !== '' && !t.startsWith('#') && !t.startsWith('##') && t.includes('\t');
}

// estrae eventuale titolo sezione da riga tipo "## SEZIONE 1: RIEPILOGO"
function sectionTitleFrom(line) {
  const m = line.match(/^##\s*(.+)$/);
  return m ? m[1].trim() : null;
}

// converte array di valori in oggetto secondo header
function rowToObject(header, row) {
  const obj = {};
  for (let i = 0; i < header.length; i++) {
    const key = (header[i] || '').trim();
    if (!key) continue;
    obj[key] = (row[i] ?? '').toString().trim();
  }
  return obj;
}

// parser “section-aware” per file tab-delimited con più header sparsi
function parseTabbedMultiSection(csvText) {
  const lines = splitLines(csvText);

  let currentSection = null;            // testo titolo sezione (se presente)
  let currentHeader = null;             // array di colonne correnti
  const records = [];

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];

    // aggiorna titolo sezione quando vedi "## ..."
    const maybeTitle = sectionTitleFrom(raw.trim());
    if (maybeTitle) {
      currentSection = maybeTitle;
      continue;
    }

    if (!isDataLine(raw)) continue;

    const parts = raw.split('\t');

    // header di una sezione: riga che inizia con "Tipo_Dato" (o simile)
    if (!currentHeader && /^tipo[_\s]?dato$/i.test((parts[0] || '').trim())) {
      currentHeader = parts.map(s => s.trim());
      continue;
    }

    // può capitare che l'header compaia di nuovo (nuova sezione)
    if (/^tipo[_\s]?dato$/i.test((parts[0] || '').trim())) {
      currentHeader = parts.map(s => s.trim());
      continue;
    }

    // riga dati: serve un header attivo
    if (currentHeader) {
      const obj = rowToObject(currentHeader, parts);
      if (Object.keys(obj).length > 0) {
        if (currentSection) obj['__Sezione'] = currentSection;
        records.push(obj);
      }
    }
  }

  return records;
}

// --- FTP fetch + parse ------------------------------------------------

async function fetchCsvFromFtp() {
  const {
    FTP_HOST,
    FTP_USER,
    FTP_PASS,
    FTP_FILE,
    FTP_SECURE,
    FTP_TIMEOUT,
    FTP_TLS_INSECURE,
    FTP_TLS_SERVERNAME
  } = process.env;

  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Variabili FTP mancanti. Imposta FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE.');
  }

  const { Client } = await import('basic-ftp');
  const client = new Client(Number(FTP_TIMEOUT || 25000));
  client.ftp.verbose = false;
  const tmpFile = path.join(os.tmpdir(), `generalb2b_${Date.now()}.csv`);

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

    await client.downloadTo(tmpFile, FTP_FILE);
    const csvText = fs.readFileSync(tmpFile, 'utf8');

    // 1) pulizia base: teniamo tutto (anche righe sezione) per inferenza
    // 2) parser multi-sezione tab-delimited
    const rows = parseTabbedMultiSection(csvText);

    return rows;
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch {}
  }
}

// --- API --------------------------------------------------------------

app.get('/data', async (_req, res) => {
  try {
    const rows = await fetchCsvFromFtp();
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      updatedAt: new Date().toISOString(),
      rows,
      rowCount: rows.length,
      columns: rows.length ? Object.keys(rows[0]) : []
    });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({
      error: "Impossibile leggere il CSV dall'FTP",
      details: String(err?.message || err)
    });
  }
});

// ---------------------------------------------------------------------

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server avviato su http://0.0.0.0:${PORT}`);
});
