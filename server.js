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

/* ---------- Utils parsing ---------- */
const splitLines = t => t.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');

// Split riga: se contiene TAB usa TAB, altrimenti spezza su 2+ spazi consecutivi
function splitFlexible(line) {
  const norm = line.replace(/\u00A0/g, ' ');
  if (norm.includes('\t')) return norm.split(/\t+/).map(s => s.trim());
  return norm.split(/ {2,}/).map(s => s.trim());
}

// true se è riga “header” di sezione (inizia con Tipo_Dato)
function isHeaderLine(line) {
  const firstCell = (splitFlexible(line)[0] || '').trim();
  return /^tipo[_\s]?dato$/i.test(firstCell);
}

// normalizza numeri in formato it (punti migliaia, virgola decimali)
function parseEuroNumber(s) {
  if (s == null) return null;
  const t = String(s).replace(/\./g, '').replace(',', '.').replace(/\s/g, '');
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

/* ---------- Parser multi-sezione “a segmenti” ---------- */
function parseSections(text) {
  const lines = splitLines(text);
  const out = [];

  let currentSection = null;   // es: "SEZIONE 3: DETTAGLIO VENDITE PER CLIENTE"
  let header = null;           // array colonne correnti

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const t = raw.trim();

    // salta vuote
    if (t === '') continue;

    // commenti / metadata
    if (t.startsWith('#')) {
      // cattura eventuale titolo sezione dai “## …”
      const m = t.match(/^##\s*(.+)$/);
      if (m) {
        currentSection = m[1].trim();
        header = null; // nuova sezione: header verrà riletto
      }
      continue;
    }

    // header
    if (isHeaderLine(raw)) {
      header = splitFlexible(raw);
      continue;
    }

    // se non ho un header attivo, è testo libero: ignora
    if (!header) continue;

    // riga dati
    const cells = splitFlexible(raw);
    const obj = {};

    for (let c = 0; c < header.length; c++) {
      const key = (header[c] || '').trim();
      if (!key) continue;
      obj[key] = (cells[c] ?? '').toString().trim();
    }

    // arricchisci con sezione
    if (currentSection) obj['__Sezione'] = currentSection;

    // tipizza i campi numerici più comuni
    const numericKeys = [
      'Anno_Precedente','Anno_Corrente','Obiettivo','Percentuale_Obiettivo',
      'Clienti_Anno_Precedente','Clienti_Anno_Corrente','Obiettivo_Clienti','Percentuale_Obiettivo_Clienti',
      'Fatturato','Importo_Totale','Importo','Percentuale','Valore','Clienti_Serviti'
    ];
    for (const k of numericKeys) {
      if (k in obj) {
        const v = parseEuroNumber(obj[k]);
        if (v !== null) obj[k] = v;
      }
    }

    out.push(obj);
  }

  return out;
}

/* ---------- FTP fetch + parse ---------- */
async function fetchRows() {
  const {
    FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
    FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME
  } = process.env;

  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Variabili FTP mancanti. Imposta FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE.');
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
    const text = fs.readFileSync(tmp, 'utf8');

    const rows = parseSections(text);
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
    res.json({
      updatedAt: new Date().toISOString(),
      rows,
      rowCount: rows.length,
      columns: rows.length ? Object.keys(rows[0]) : []
    });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: "Impossibile leggere il CSV dall'FTP", details: String(err?.message || err) });
  }
});

/* ---------- Start ---------- */
app.listen(PORT, '0.0.0.0', () => console.log(`Server avviato su http://0.0.0.0:${PORT}`));
