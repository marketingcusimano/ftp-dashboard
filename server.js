const express = require('express');
const compression = require('compression');
const ftp = require('basic-ftp');
const { parse } = require('csv-parse/sync');
const path = require('path');
const fs = require('fs');
const { Writable } = require('stream');

const {
  FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
  FTP_SECURE = 'false', CORS = 'false'
} = process.env;

const ITnum = (s) => {
  if (s == null) return null;
  const v = String(s).trim();
  if (!v) return null;
  if (/%$/.test(v)) {
    const n = ITnum(v.replace('%',''));
    if (n === 999999) return null;
    return n;
  }
  const t = v.replace(/\./g,'').replace(/,/g,'.');
  if (/^[+-]?\d+(\.\d+)?$/.test(t)) return Number(t);
  return v;
};

function cleanLines(text) {
  return text.replace(/^\uFEFF/,'').split(/\r?\n/)
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
  const text = rawText.replace(/^\uFEFF/, '');
  const all = parse(text, {
    delimiter: '\t',
    relax_column_count: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
    columns: false
  }).filter(arr => {
    const line = (arr.join('\t') || '').trim();
    return line && !/^#{1,2}\s/.test(line);
  });

  const isTipo = v => ['OBIETTIVI','CLIENTI_PROVINCIA','VENDITE_CLIENTE','CATEGORIA_RIEPILOGO','RIPARTIZIONE','TOTALE'].includes(v);
  const out = [];

  for (let arr of all) {
    const tipo = (arr[0] || '').trim();
    if (!isTipo(tipo)) continue;

    if (tipo === 'OBIETTIVI') {
      out.push({
        Tipo_Dato: 'OBIETTIVI',
        Provincia: arr[1] ?? null,
        Anno_Precedente: ITnum(arr[2]),
        Anno_Corrente: ITnum(arr[3]),
        Obiettivo: ITnum(arr[4]),
        Percentuale_Obiettivo: ITnum(arr[5]),
        Clienti_Anno_Precedente: ITnum(arr[6]),
        Clienti_Anno_Corrente: ITnum(arr[7]),
        Obiettivo_Clienti: ITnum(arr[8]),
        Percentuale_Obiettivo_Clienti: ITnum(arr[9])
      });
      continue;
    }

    if (tipo === 'CLIENTI_PROVINCIA') {
      out.push({
        Tipo_Dato: 'CLIENTI_PROVINCIA',
        Provincia: arr[1] ?? null,
        Clienti_Serviti: ITnum(arr[2]),
        Fatturato: ITnum(arr[3])
      });
      continue;
    }

    if (tipo === 'VENDITE_CLIENTE') {
      let provincia = arr[1] ?? null;
      let fatt = arr[arr.length - 1];
      let nome = arr.slice(2, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'VENDITE_CLIENTE',
        Provincia: provincia,
        Ragione_Sociale_Cliente: nome,
        Fatturato: ITnum(fatt)
      });
      continue;
    }

    if (tipo === 'CATEGORIA_RIEPILOGO') {
      let importo = arr[arr.length - 1];
      let categoria = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'CATEGORIA_RIEPILOGO',
        Categoria: categoria || null,
        Importo_Totale: ITnum(importo)
      });
      continue;
    }

    if (tipo === 'RIPARTIZIONE') {
      const percent = arr[arr.length - 1];
      const importo = arr[arr.length - 2];
      const cat = arr.slice(1, arr.length - 2).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'RIPARTIZIONE',
        Categoria_Prodotto: cat || null,
        Importo: ITnum(importo),
        Percentuale: ITnum(percent)
      });
      continue;
    }

    if (tipo === 'TOTALE') {
      const valore = arr[arr.length - 1];
      const descr = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'TOTALE',
        Descrizione: descr || null,
        Valore: ITnum(valore)
      });
      continue;
    }
  }

  const columns = Array.from(out.reduce((set, r) => {
    Object.keys(r).forEach(k => set.add(k));
    return set;
  }, new Set()));

  return {
    delimiter: '\\t',
    columns,
    rowCount: out.length,
    rows: out
  };
}

function parseSmart(rawText) {
  const text = rawText.replace(/^\uFEFF/, '');
  const all = parse(text, {
    delimiter: '\t',
    relax_column_count: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
    columns: false
  }).filter(arr => {
    const line = (arr.join('\t') || '').trim();
    return line && !/^#{1,2}\s/.test(line);
  });

  const isTipo = v => ['OBIETTIVI','CLIENTI_PROVINCIA','VENDITE_CLIENTE','CATEGORIA_RIEPILOGO','RIPARTIZIONE','TOTALE'].includes(v);
  const out = [];

  for (let arr of all) {
    const tipo = (arr[0] || '').trim();
    if (!isTipo(tipo)) continue;

    if (tipo === 'OBIETTIVI') {
      out.push({
        Tipo_Dato: 'OBIETTIVI',
        Provincia: arr[1] ?? null,
        Anno_Precedente: ITnum(arr[2]),
        Anno_Corrente: ITnum(arr[3]),
        Obiettivo: ITnum(arr[4]),
        Percentuale_Obiettivo: ITnum(arr[5]),
        Clienti_Anno_Precedente: ITnum(arr[6]),
        Clienti_Anno_Corrente: ITnum(arr[7]),
        Obiettivo_Clienti: ITnum(arr[8]),
        Percentuale_Obiettivo_Clienti: ITnum(arr[9])
      });
      continue;
    }

    if (tipo === 'CLIENTI_PROVINCIA') {
      out.push({
        Tipo_Dato: 'CLIENTI_PROVINCIA',
        Provincia: arr[1] ?? null,
        Clienti_Serviti: ITnum(arr[2]),
        Fatturato: ITnum(arr[3])
      });
      continue;
    }

    if (tipo === 'VENDITE_CLIENTE') {
      let provincia = arr[1] ?? null;
      let fatt = arr[arr.length - 1];
      let nome = arr.slice(2, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'VENDITE_CLIENTE',
        Provincia: provincia,
        Ragione_Sociale_Cliente: nome,
        Fatturato: ITnum(fatt)
      });
      continue;
    }

    if (tipo === 'CATEGORIA_RIEPILOGO') {
      let importo = arr[arr.length - 1];
      let categoria = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'CATEGORIA_RIEPILOGO',
        Categoria: categoria || null,
        Importo_Totale: ITnum(importo)
      });
      continue;
    }

    if (tipo === 'RIPARTIZIONE') {
      const percent = arr[arr.length - 1];
      const importo = arr[arr.length - 2];
      const cat = arr.slice(1, arr.length - 2).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'RIPARTIZIONE',
        Categoria_Prodotto: cat || null,
        Importo: ITnum(importo),
        Percentuale: ITnum(percent)
      });
      continue;
    }

    if (tipo === 'TOTALE') {
      const valore = arr[arr.length - 1];
      const descr = arr.slice(1, arr.length - 1).join(' ').replace(/\s+/g, ' ').trim();
      out.push({
        Tipo_Dato: 'TOTALE',
        Descrizione: descr || null,
        Valore: ITnum(valore)
      });
      continue;
    }
  }

  const columns = Array.from(out.reduce((set, r) => {
    Object.keys(r).forEach(k => set.add(k));
    return set;
  }, new Set()));

  return {
    delimiter: '\\t',
    columns,
    rowCount: out.length,
    rows: out
  };
}

async function downloadFromFtpAsText(){
  if (!FTP_HOST || !FTP_USER || !FTP_PASS || !FTP_FILE)
    throw new Error('Mancano var. ambiente: FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE');
  const client = new ftp.Client(15000);
  client.ftp.verbose = false;

  const chunks = [];
  const sink = new Writable({ write(c, _e, cb){ chunks.push(Buffer.from(c)); cb(); }});

  try{
    await client.access({
      host: FTP_HOST, user: FTP_USER, password: FTP_PASS,
      secure: /^true$/i.test(FTP_SECURE)
    });
    await client.downloadTo(sink, FTP_FILE);
    return Buffer.concat(chunks).toString('utf8');
  } finally { client.close(); }
}

const app = express();
app.disable('x-powered-by');
app.use(compression());
if (/^true$/i.test(CORS)) { app.use(require('cors')()); }

const publicDir = path.join(__dirname,'public');
if (fs.existsSync(publicDir)) app.use(express.static(publicDir));
app.use(express.static(__dirname));

app.get('/healthz', (_req,res)=>res.type('text/plain').send('ok'));
app.get('/raw', async (_req,res)=>{
  try{ res.type('text/plain').send(await downloadFromFtpAsText()); }
  catch(err){ res.status(500).json({error:'FTP error', detail:String(err?.message||err)}); }
});
app.get('/data', async (_req,res)=>{
  try{
    const parsed = parseSmart(await downloadFromFtpAsText());
    res.json({ updatedAt: new Date().toISOString(), ...parsed });
  }catch(err){
    console.error('Errore /data:', err);
    res.status(500).json({error:'Parse/FTP failed', detail:String(err?.message||err)});
  }
});

app.get('*', (req,res)=>{
  const idxPublic = path.join(publicDir,'index.html');
  const idxRoot   = path.join(__dirname,'index.html');
  if (fs.existsSync(idxPublic)) return res.sendFile(idxPublic);
  if (fs.existsSync(idxRoot))   return res.sendFile(idxRoot);
  res.status(404).send('index.html non trovato');
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, ()=>console.log(`Server avviato su http://localhost:${PORT}`));
