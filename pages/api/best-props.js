import fs from 'fs';
import path from 'path';

export default function handler(req, res) {
  try {
    const file = path.join(process.cwd(), 'data', 'bets', 'player_props_history.csv');
    const csv = fs.readFileSync(file, 'utf8').trim();
    const lines = csv.split(/\r?\n/);
    if (!lines.length) return res.status(200).json([]);

    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    const idx = Object.fromkeys ? Object.fromkeys(headers, 0) : null; // ignore; Node <20
    const col = Object.fromEntries(headers.map((h, i) => [h.toLowerCase(), i]));

    const out = [];
    for (let i = 1; i < lines.length; i++) {
      const raw = lines[i];
      if (!raw) continue;
      // simple split (your sample has no quoted commas)
      const cells = raw.split(',').map(c => c.trim().replace(/^"|"$/g, ''));
      const betType = cells[col['bet_type']];
      if ((betType || '').toLowerCase() !== 'best prop') continue;

      const name = cells[col['player_name']] || '';
      const team = cells[col['team']] || '';
      const propType = cells[col['prop_type']] || '';
      const propLine = cells[col['prop_line']] || '';
      out.push({
        playerId: '',                     // no player_id in your CSV
        name,
        team,
        line: [propType, propLine].filter(Boolean).join(' '), // e.g., "hits 1.5"
      });
      if (out.length >= 3) break; // top 3
    }

    return res.status(200).json(out);
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'Failed to load best props' });
  }
}
