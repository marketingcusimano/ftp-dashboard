import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3000;

// Static (usa /public se esiste, altrimenti root)
const staticDir = fs.existsSync('public') ? 'public' : '.';
app.use(express.static(staticDir, { maxAge: 0 }));

app.get('/', (req, res) => {
  const file = path.join(process.cwd(), staticDir, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send('index.html non trovato');
});

app.get('/healthz', (_, res) => res.send('ok'));

// /raw -> scarica dal FTP e restituisce il TESTO GREZZO (nessun parsing lato server)
app.get('/raw', async (_req, res) => {
  const {
    FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
    FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME
  } = process.env;

  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    return res.status(500).send('Mancano variabili FTP (FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE).');
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

    res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');
    res.type('text/plain').send(text);
  } catch (e) {
    console.error('Errore /raw:', e);
    res.status(500).type('text/plain').send(String(e?.message || e));
  } finally {
    try { client.close(); } catch {}
    try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch {}
  }
});

// compat: /data reindirizza a /raw (il parsing avviene nel browser)
app.get('/data', async (_req, res) => {
  res.redirect(307, '/raw');
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server avviato su http://0.0.0.0:${PORT}`);
});
