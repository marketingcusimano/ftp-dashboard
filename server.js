// server.js
const express = require('express');
const compression = require('compression');
const ftp = require('basic-ftp');
const { parse } = require('csv-parse/sync');
const path = require('path');
const fs = require('fs');
const { Writable } = require('stream');

const {
  FTP_HOST,
  FTP_USER,
  FTP_PASS,
  FTP_FILE,
  FTP_SECURE = 'false',
  CORS = 'false'
} = process.env;

/* ---------- parsing utils ---------- */
const ITnum = (s) => {
  if (s == null) return null;
  const v = String(s).trim();
  if (!v) return null;
  if (/%$/.test(v)) {
    const n = ITnum(v.replace('%',''));
    if (n === 999999) return null; // sentinella
    return n; // 0..100
  }
  const t = v.replace(/\./g,'').replace(/,/g,'.');
  if (/^[+-]?\d+(\.\d+)?$/.test(t)) return Number(t);
  return v;
};

function cleanLines(text) {
  return text
    .replace(/^\uFEFF/, '')
    .split(/\r?\n/)
    .filter(line => line.trim() !== '' && !/^#{1,2}\s/.test(line));
}

const schemas = {
  'Tipo_Dato\tProvincia\tAnno_Precedente\tAnno_Corrente\tObiettivo\tPercentuale_Obiettivo\tClienti_Anno_Precedente\tClienti_Anno_Corrente\tObiettivo_Clienti\tPercentuale_Obiettivo_Clienti':
    { cols: ['Tipo_Dato','Provincia','Anno_Precedente','Anno_Corrente','Obiettivo','Percentuale_Obiettivo','Clienti_Anno_Precedente','Clienti_Anno_Corrente','Obiettivo_Clienti','Percentuale_Obiettivo_Clienti'] },
  'Tipo_Dato\tProvincia\tClienti_Serviti\tFatturato':
    { cols: ['Tipo_Dato','Provincia','Clienti_Serviti','Fatturato'] },
  'Tipo_Dato\tProvincia\tRagione_Sociale_Cliente\tFatturato':
    { cols: ['Tipo_Dato','Provincia','Ragione_Sociale_Cliente','Fatturato'] },
  'Tipo_Dato\tCategoria\tImporto_Totale':
    { cols: ['Tipo_Dato','Categoria','Importo_Totale'] },
  'Tipo_Dato\tCategoria_Prodotto\tImporto\tPercentuale':
    { cols: ['Tipo_Dato','Categoria_Prodotto','Importo','Percentuale'] },
  'Tipo_Dato\tDescrizione\tValore':
    { cols: ['Tipo_Dato','Descrizione','Valore'] },
};

function parseSmart(rawText) {
  const lines = cleanLines(rawText);
  const hasTSVHeaders = lines.some(l => schemas[l]);

  if (!hasTSVHeaders) {
    const first = (rawText.replace(/^\uFEFF/, '').split(/\r?\n/).find(x => x.trim() && !/^#{1,2}\s/.test(x))) || '';
    const candidates = [',',';','\t','|'];
    let best = ',', bestCount = 0;
    for (const d of candidates) {
      const count = (first.match(new RegExp(`\\${d}(?=(?:[^"]*"[^"]*")*[^"]*$)`, 'g')) || []).length;
      if (count > bestCount) { best = d; bestCount = count; }
    }
    const records = parse(rawText, {
      delimiter: best,
      relax_quotes: true,
      relax_column_count: true,
      skip_empty_lines: true,
      bom: true,
      columns: true,
      trim: true
    }).map(obj => Object.fromEntries(Object.entries(obj).map(([k,v]) => [k, ITnum(v)])));
    const columns = Object.keys(records[0] || []);
    return { columns, rows: records, rowCount: records.length, delimiter: best };
  }

  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    const header = lines[i++];
    if (!schemas[header]) continue;
    const { cols } = schemas[header];

    const rows = [];
    while (i < lines.length && !schemas[lines[i]]) rows.push(lines[i++]);

    const parsed = parse(rows.join('\n'), {
      delimiter: '\t',
      relax_column_count: true,
      skip_empty_lines: true
    }).map(arr => {
      if (arr.length > cols.length) {
        if (cols.includes('Ragione_Sociale_Cliente')) {
          arr = [arr[0], arr[1], arr.slice(2, arr.length - 1).join(' '), arr[arr.length - 1]];
        } else if (cols.includes('Categoria')) {
          arr = [arr[0], arr.slice(1, arr.length - 1).join(' '), arr[arr.length - 1]];
        }
      }
      const o = {};
      cols.forEach((k, idx) => { o[k] = ITnum(arr[idx]); });
      return o;
    });
    blocks.push({ cols, rows: parsed });
  }

  const allRows = blocks.flatMap(b => b.rows);
  const columns = [...new Set(blocks.flatMap(b => b.cols))];
  return { columns, rows: allRows, rowCount: allRows.length, delimiter: '\\t' };
}

/* ---------- FTP: usa un vero Writable stream ---------- */
async function downloadFromFtpAsText() {
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Mancano var. ambiente: FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE');
  }
  const client = new ftp.Client(15000);
  client.ftp.verbose = false;

  const chunks = [];
  const sink = new Writable({
    write(chunk, enc, cb) { chunks.push(Buffer.from(chunk)); cb(); }
  });

  try {
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: /^true$/i.test(FTP_SECURE)
    });
    await client.downloadTo(sink, FTP_FILE); // <— ora è uno stream valido
    return Buffer.concat(chunks).toString('utf8');
  } finally {
    client.close();
  }
}

/* ---------- App ---------- */
const app = express();
app.disable('x-powered-by');
app.use(compression());
if (/^true$/i.test(CORS)) {
  const cors = require('cors');
  app.use(cors());
}

// serve statico: prima ./public, poi root repo
const publicDir = path.join(__dirname, 'public');
if (fs.existsSync(publicDir)) app.use(express.static(publicDir));
app.use(express.static(__dirname)); // copre il caso di index.html in root

app.get('/healthz', (req, res) => res.type('text/plain').send('ok'));

app.get('/raw', async (_req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    res.type('text/plain').send(txt);
  } catch (err) {
    res.status(500).json({ error: 'FTP error', detail: String(err?.message || err) });
  }
});

app.get('/data', async (_req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    const parsed = parseSmart(txt);
    res.json({ updatedAt: new Date().toISOString(), ...parsed });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err?.message || err) });
  }
});

// fallback a index.html (public o root)
app.get('*', (req, res) => {
  const idxPublic = path.join(publicDir, 'index.html');
  const idxRoot = path.join(__dirname, 'index.html');
  if (fs.existsSync(idxPublic)) return res.sendFile(idxPublic);
  if (fs.existsSync(idxRoot)) return res.sendFile(idxRoot);
  res.status(404).send('index.html non trovato');
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Server avviato su http://localhost:${PORT}`);
});
