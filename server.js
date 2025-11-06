import 'dotenv/config';
import express from 'express';
import { Client } from 'basic-ftp';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { parse } from 'csv-parse/sync';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static('public', { maxAge: 0 }));

async function fetchCsvFromFtp() {
  const client = new Client(Number(process.env.FTP_TIMEOUT || 15000));
  client.ftp.verbose = false;
  const tmpFile = path.join(os.tmpdir(), `generalb2b_${Date.now()}.csv`);
  try {
    await client.access({
      host: process.env.FTP_HOST,
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: String(process.env.FTP_SECURE).toLowerCase() === 'true',
    });
    await client.downloadTo(tmpFile, process.env.FTP_FILE);
    const csvBuffer = fs.readFileSync(tmpFile);
    const records = parse(csvBuffer, { columns: true, skip_empty_lines: true, trim: true });
    return records;
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch {}
  }
}

app.get('/data', async (req, res) => {
  try {
    const data = await fetchCsvFromFtp();
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      updatedAt: new Date().toISOString(),
      rows: data,
      rowCount: data.length,
      columns: Object.keys(data[0] || {}),
    });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: "Impossibile leggere il CSV dall'FTP", details: String(err?.message || err) });
  }
});

app.listen(PORT, () => console.log(`Server avviato su http://localhost:${PORT}`));
