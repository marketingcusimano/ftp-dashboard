// --- utils nuove (incolla in server.js) ---
const U_NBSP = /\u00A0/g; // non-breaking space

function normalizeLine(s) {
  // rimuove BOM, NBSP e spazi strani
  return String(s ?? '')
    .replace(/^\uFEFF/, '')
    .replace(U_NBSP, ' ')
    .replace(/\t+/g, '\t')        // compattare tab
    .replace(/\s+$/,'')           // trim end
    .trim();
}

function pickDelimiterFromHeader(line) {
  // NO VIRGOLA: i numeri italiani la usano come decimale
  const candidates = [/\t+/, / {2,}/, /;/, /\|/];
  const norm = line.replace(U_NBSP, ' ');
  let best = { rx: null, cols: 1 };
  for (const rx of candidates) {
    const arr = norm.split(rx).map(s => s.trim()).filter(Boolean);
    if (arr.length > best.cols) best = { rx, cols: arr.length };
  }
  // fallback sicuro: tab o 2+ spazi
  return best.rx || /\t+| {2,}/;
}

function parseTextToRows(text) {
  const rawLines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  const lines = rawLines.map(normalizeLine);

  let rows = [];
  let section = null;
  let agenteCodice = null, agenteNome = null, periodo = null;

  // 1) trova tutte le righe header (contengono "Tipo_Dato")
  const headerIdx = [];
  for (let i=0;i<lines.length;i++){
    const L = lines[i];
    if (!L) continue;
    if (/^#\s*Codice\s+Agente:/i.test(L)) { agenteCodice = L.replace(/^#\s*Codice\s+Agente:\s*/i,'').trim(); continue; }
    if (/^#\s*Ragione\s+Sociale:/i.test(L)) { agenteNome   = L.replace(/^#\s*Ragione\s+Sociale:\s*/i,'').trim(); continue; }
    if (/^#\s*Periodo:/i.test(L))          { periodo      = L.replace(/^#\s*Periodo:\s*/i,'').trim(); continue; }

    const sec = L.match(/^##\s*(.+)$/);
    if (sec) { section = sec[1].trim(); continue; }

    if (/^#/.test(L)) continue; // altri commenti

    if (/tipo[_ ]?dato/i.test(L)) {
      headerIdx.push({ i, sectionAt: section ?? null });
    }
  }

  // 2) per ogni header, scegli il separatore e parse le righe fino al prossimo header/sezione
  for (let h = 0; h < headerIdx.length; h++){
    const start = headerIdx[h].i;
    const end   = (h+1 < headerIdx.length ? headerIdx[h+1].i : lines.length);
    const sectionHere = headerIdx[h].sectionAt;

    const headerLine = lines[start];
    const delim = pickDelimiterFromHeader(headerLine);
    const header = headerLine.split(delim).map(s=>s.trim());

    // salta header malformati
    if (!header.length || !/tipo[_ ]?dato/i.test(header[0])) continue;

    for (let r = start+1; r < end; r++){
      let L = lines[r];
      if (!L) continue;
      if (/^#/.test(L)) continue;
      // se parte una nuova sezione, interrompi
      if (/^##\s+/.test(L)) break;

      let cells = L.split(delim).map(s=>s.trim());

      // uniforma numero di colonne
      if (cells.length < header.length) {
        while (cells.length < header.length) cells.push('');
      } else if (cells.length > header.length) {
        cells = cells.slice(0, header.length);
      }

      const obj = {};
      for (let c=0;c<header.length;c++){
        const keyRaw = header[c] || '';
        const key = keyRaw.trim();
        if (!key) continue;
        let val = (cells[c] ?? '').replace(/^["']|["']$/g,'').trim();

        const isPercentKey = key.toLowerCase().includes('percentuale');

        if (/%\s*$/.test(val)) {
          // percentuali 0–100, es "66,51%" -> 66.51
          let n = val.replace('%','').replace(/\./g,'').replace(',', '.').trim();
          let num = Number(n);
          if (!Number.isFinite(num) || num > 1000) num = null;
          obj[key] = num;
        } else if (/^[0-9.\s,]+$/.test(val)) {
          // numeri italiani -> float
          let n = val.replace(/\./g,'').replace(',', '.').replace(/\s/g,'');
          let num = Number(n);
          if (!Number.isFinite(num)) num = null;
          if (num === 999999 || num === 99999900) num = null;
          if (isPercentKey && num > 1000) num = null;
          obj[key] = num;
        } else {
          obj[key] = val || null;
        }
      }

      if (Object.keys(obj).length){
        if (sectionHere) obj['__Sezione'] = sectionHere;
        if (agenteCodice) obj['Agente_Codice'] = agenteCodice;
        if (agenteNome)   obj['Agente_Nome']   = agenteNome;
        if (periodo)      obj['Periodo']       = periodo;
        rows.push(obj);
      }
    }
  }

  return rows;
}
