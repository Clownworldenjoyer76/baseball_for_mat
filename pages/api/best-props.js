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

export default function handler(req, res) {
  try {
    const file = path.join(process.cwd(), 'data', 'bets', 'player_props_history.csv');
    if (!fs.existsSync(file)) {
      return res.status(404).json({ error: 'CSV not found' });
    }

    const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(l => l.trim().length);
    if (!lines.length) return res.status(200).json([]);

    // Read headers exactly as they are
    const headers = splitCSVLine(lines[0]).map(h => h.trim());
    const betTypeIndex = headers.findIndex(h => h.toLowerCase().replace(/\s+/g, '') === 'bet_type');
    const nameIndex = headers.findIndex(h => h.toLowerCase().includes('player'));
    const teamIndex = headers.findIndex(h => h.toLowerCase().includes('team'));
    const propTypeIndex = headers.findIndex(h => h.toLowerCase().includes('prop_type'));
    const propLineIndex = headers.findIndex(h => h.toLowerCase().includes('prop_line'));

    if (betTypeIndex === -1) {
      return res.status(500).json({ error: 'bet_type column not found', headers });
    }

    const out = [];
    for (let i = 1; i < lines.length; i++) {
      const cells = splitCSVLine(lines[i]).map(c => c.trim());
      if ((cells[betTypeIndex] || '').trim().toLowerCase() !== 'best prop') continue;

      out.push({
        playerId: '',
        name: nameIndex >= 0 ? cells[nameIndex] : '',
        team: teamIndex >= 0 ? cells[teamIndex] : '',
        line: [
          propTypeIndex >= 0 ? cells[propTypeIndex] : '',
          propLineIndex >= 0 ? cells[propLineIndex] : ''
        ].filter(Boolean).join(' ')
      });

      if (out.length >= 3) break;
    }

    return res.status(200).json(out);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
