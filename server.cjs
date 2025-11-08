// server.cjs
// Express API che scarica un TSV/CSV da FTP e lo espone come JSON per la dashboard

const express = require('express');
const compression = require('compression');
const ftp = require('basic-ftp');
const { parse } = require('csv-parse/sync');
const path = require('path');

// === CONFIG via ENV ===
// FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE (es: generalb2b.csv)
// FTP_SECURE=false (default) | true  (per FTPS esplicito)
// PORT=10000 (Render imposta PORT automaticamente)
const {
  FTP_HOST,
  FTP_USER,
  FTP_PASS,
  FTP_FILE,
  FTP_SECURE = 'false'
} = process.env;

// --- CORS (attiva solo se frontend e backend sono su origini diverse) ---
const USE_CORS = process.env.CORS === 'true';

// === Parser robusto per TSV/CSV con numeri it-IT ===
const ITnum = (s) => {
  if (s == null) return null;
  const v = String(s).trim();
  if (!v) return null;
  // percentuali: "70,65%" -> 70.65 (se vuoi 0..1 usa n/100)
  if (/%$/.test(v)) {
    const n = ITnum(v.replace('%', ''));
    if (n === 999999) return null; // sentinella nel tuo dataset
    return n;
  }
  // numero it-IT: "1.234,56" => 1234.56
  const t = v.replace(/\./g, '').replace(/,/g, '.');
  if (/^[+-]?\d+(\.\d+)?$/.test(t)) return Number(t);
  return v;
};

function cleanLines(text) {
  return text
    .replace(/^\uFEFF/, '')
    .split(/\r?\n/)
    .filter(line => line.trim() !== '' && !/^#{1,2}\s/.test(line)); // rimuove commenti/titoli (# / ##)
}

// intestazioni -> schema
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

// parser principale (autodetect tab/comma/semicolon/pipe e gestione blocchi TSV)
function parseSmart(rawText) {
  // prima prova: il tuo file è TSV con blocchi; gestiamolo come tale
  const lines = cleanLines(rawText);

  // Se non sembra TSV a blocchi, fallback a autodetect delimitatore unico
  const hasTSVHeaders = lines.some(l => schemas[l]);
  if (!hasTSVHeaders) {
    // autodetect singolo delimitatore su prima riga utile
    const first = (rawText.replace(/^\uFEFF/, '').split(/\r?\n/).find(x => x.trim() && !/^#{1,2}\s/.test(x))) || '';
    const candidates = [',', ';', '\t', '|'];
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

  // modalità blocchi (TSV)
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
      // ripara colonne extra (tab “sporchi” in nomi)
      if (arr.length > cols.length) {
        if (cols.includes('Ragione_Sociale_Cliente')) {
          // [Tipo, Prov, ...nome..., Fatturato]
          arr = [arr[0], arr[1], arr.slice(2, arr.length - 1).join(' '), arr[arr.length - 1]];
        } else if (cols.includes('Categoria')) {
          // [Tipo, ...categoria..., Importo]
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

// === FTP download ===
async function downloadFromFtpAsText() {
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE) {
    throw new Error('Mancano var. ambiente: FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE');
    }
  const client = new ftp.Client(15000); // 15s timeout socket
  client.ftp.verbose = false;
  try {
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: /^true$/i.test(FTP_SECURE)
    });
    const writable = [];
    await client.downloadTo((chunk) => writable.push(Buffer.from(chunk)), FTP_FILE);
    const buf = Buffer.concat(writable);
    return buf.toString('utf8');
  } finally {
    client.close();
  }
}

// === App ===
const app = express();
app.disable('x-powered-by');
app.use(compression());
if (USE_CORS) {
  const cors = require('cors');
  app.use(cors());
}

// static: serve ./public
app.use(express.static(path.join(__dirname, 'public')));

// health
app.get('/healthz', (req, res) => res.type('text/plain').send('ok'));

// raw: per debug (contenuto puro del file)
app.get('/raw', async (req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    res.type('text/plain').send(txt);
  } catch (err) {
    res.status(500).json({ error: 'FTP error', detail: String(err?.message || err) });
  }
});

// data: JSON normalizzato
app.get('/data', async (req, res) => {
  try {
    const txt = await downloadFromFtpAsText();
    const parsed = parseSmart(txt);
    res.json({ updatedAt: new Date().toISOString(), ...parsed });
  } catch (err) {
    console.error('Errore /data:', err);
    res.status(500).json({ error: 'Parse/FTP failed', detail: String(err?.message || err) });
  }
});

// fallback a index.html
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// avvio
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Server avviato su http://localhost:${PORT}`);
});
