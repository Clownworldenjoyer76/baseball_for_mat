// pages/api/best-props.js
import fs from 'fs';
import path from 'path';

function splitCSVLine(line) {
  const cols = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; continue; }
    if (ch === '"') { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { cols.push(cur); cur = ''; continue; }
    cur += ch;
  }
  cols.push(cur);
  return cols;
}

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return [];
  const headers = splitCSVLine(lines[0]).map(h => h.trim().replace(/^"|"$/g, ''));
  return lines.slice(1).map(line => {
    const cols = splitCSVLine(line).map(c => c.trim().replace(/^"|"$/g, ''));
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] ?? ''; });
    return obj;
  });
}

export default function handler(req, res) {
  try {
    const file = path.join(process.cwd(), 'data', 'bets', 'player_props_history.csv');
    const csv = fs.readFileSync(file, 'utf8');
    const rows = parseCSV(csv);

    // normalize keys -> lowercase for easy access
    const norm = rows.map(r => {
      const o = {};
      Object.keys(r).forEach(k => { o[k.toLowerCase()] = r[k]; });
      return o;
    });

    // filter to Best Prop
    const filtered = norm.filter(r => (r['bet_type'] || '') === 'Best Prop');

    // optional sort if fields exist; otherwise keep file order
    const sorted = [...filtered].sort((a, b) => {
      const keys = ['probability', 'edge', 'value', 'mega_z', 'z_score'];
      for (const k of keys) {
        const av = parseFloat(a[k]);
        const bv = parseFloat(b[k]);
        if (!Number.isNaN(av) && !Number.isNaN(bv) && av !== bv) return bv - av;
      }
      return 0;
    });

    // map + dedupe by player_id (fallback to name)
    const mapped = sorted.map(r => ({
      playerId: r['player_id'] || r['playerid'] || '',
      name: r['name'] || '',
      team: r['team'] || r['team_code'] || '',
      line: r['line'] || r['prop_type'] || r['proptype'] || '',
      edge: r['edge'] ?? '',
      probability: r['probability'] ?? ''
    }));

    const unique = [];
    const seen = new Set();
    for (const r of mapped) {
      const key = r.playerId || r.name;
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(r);
      }
      if (unique.length >= 3) break;
    }

    res.status(200).json(unique); // top 3
  } catch (e) {
    res.status(500).json({ error: e?.message || 'Failed to load best props' });
  }
}
