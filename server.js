import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { parse } from 'csv-parse/sync';

const app = express();
const PORT = process.env.PORT || 3000;

/**
 * Static files: usa /public se esiste, altrimenti la root del repo.
 */
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));

/**
 * Home → serve index.html (sia da /public che da root).
 */
app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});

/**
 * Healthcheck semplice.
 */
app.get('/healthz', (_, res) => res.send('ok'));

/**
 * Scarica e parsa il CSV dall'FTP/FTPS ad ogni chiamata.
 */
async function fetchCsvFromFtp() {
  const {
    FTP_HOST,
    FTP_USER,
    FTP_PASS,
    FTP_FILE,
    FTP_SECURE,
    FTP_TIMEOUT,
    FTP_TLS_INSECURE,
    FTP_TLS_SERVERNAME,
    CSV_DELIMITER
  } = process.env;

  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Variabili FTP mancanti. Imposta FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE.');
  }

  // Import lazy per avvio più rapido
  const { Client } = await import('basic-ftp');

  const client = new Client(Number(FTP_TIMEOUT || 15000));
  client.ftp.verbose = false;

  // File temporaneo
  const tmpFile = path.join(os.tmpdir(), `generalb2b_${Date.now()}.csv`);

  try {
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: String(FTP_SECURE).toLowerCase() === 'true', // FTPS esplicito se true
      // Opzioni TLS per gestire SNI/mismatch certificato
      secureOptions: {
        // Se FTP_TLS_INSECURE=true non rifiutare certificati mismatch/self-signed
        rejectUnauthorized: String(FTP_TLS_INSECURE).toLowerCase() === 'true' ? false : true,
        // Forza SNI se il certificato è emesso per un altro hostname (es. sp1-euweb.it)
        servername: FTP_TLS_SERVERNAME || FTP_HOST
      }
    });

    // Scarica il CSV
    await client.downloadTo(tmpFile, FTP_FILE);

    const csvBuffer = fs.readFileSync(tmpFile);

    // Delimitatore: usa CSV_DELIMITER se valido, altrimenti tenta auto-detect su set comune
    const allowed = [',', ';', '\t', '|'];
    // Se l'utente ha impostato un delimitatore valido usalo, altrimenti passa l'array per auto-detect
    const csvDelimiter = CSV_DELIMITER && allowed.includes(CSV_DELIMITER) ? CSV_DELIMITER : allowed;

    const records = parse(csvBuffer, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      bom: true,                    // gestisce BOM UTF-8
      delimiter: csvDelimiter,      // ',' ';' TAB '|' oppure auto-detect tra questi
      relax_column_count: true,     // tollera righe con colonne in più/meno
      relax_quotes: true
    });

    return records; // Array di oggetti
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch {}
  }
}

/**
 * Endpoint dati: rilegge dall'FTP ad ogni richiesta.
 */
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

/**
 * Bind esplicito su 0.0.0.0 (richiesto da Render).
 */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server avviato su http://0.0.0.0:${PORT}`);
});
