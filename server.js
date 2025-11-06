import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { parse } from 'csv-parse/sync';

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
  const client = new Client(Number(FTP_TIMEOUT || 15000));
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

    // 🔹 1. Filtra righe non utili (#, vuote)
    const cleanText = csvText
      .split(/\r?\n/)
      .filter(line =>
        line.trim() !== '' &&
        !line.startsWith('#') &&
        !line.startsWith('##') &&
        line.includes('\t')
      )
      .join('\n');

    // 🔹 2. Parso con separatore TAB
    const records = parse(cleanText, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      bom: true,
      delimiter: '\t',
      relax_column_count: true,
      relax_quotes: true
    });

    return records;
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch {}
  }
}

app.get('/data', async (_req, res) => {
  try {
    const data = await fetchCsvFromFtp();
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      updatedAt: new Date().toISOString(),
      rows: data,
      rowCount: data.length,
      columns: Object.keys(data[0] || {})
    });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({
      error: "Impossibile leggere il CSV dall'FTP",
      details: String(err?.message || err)
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server avviato su http://0.0.0.0:${PORT}`);
});
