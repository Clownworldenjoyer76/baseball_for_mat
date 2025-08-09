import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

const readCsv = (p) =>
  fs.existsSync(p)
    ? parse(fs.readFileSync(p, 'utf8'), { columns: true, skip_empty_lines: true })
    : [];

const DATA_DIR = path.join(process.cwd(), 'data');
const PLAYER_HISTORY = path.join(DATA_DIR, 'bets', 'player_props_history.csv');
const BATTER_PROJ = path.join(DATA_DIR, '_projections', 'batter_props_z_expanded.csv');
const PITCHER_PROJ = path.join(DATA_DIR, '_projections', 'pitcher_mega_z.csv');

const norm = (s) => String(s || '').trim().toLowerCase();
const key = (name, team) => `${norm(name)}|${norm(team)}`;

export default function handler(req, res) {
  try {
    const hist = readCsv(PLAYER_HISTORY);
    if (!hist.length) return res.status(200).json([]);

    const dates = [...new Set(hist.map(r => r.date))].filter(Boolean).sort();
    const latest = dates[dates.length - 1];

    const todays = hist.filter(r => r.date === latest && r.bet_type === 'Best Prop');

    const bat = readCsv(BATTER_PROJ);
    const pit = readCsv(PITCHER_PROJ);

    const idMap = new Map();
    bat.forEach(r => {
      if (r.player_id) idMap.set(key(r.name || r.player_name, r.team), String(r.player_id));
    });
    pit.forEach(r => {
      if (r.player_id) idMap.set(key(r.name, r.team), String(r.player_id));
    });

    const out = todays.slice(0, 3).map(r => {
      const player_name = r.player_name || r.name;
      const player_id = idMap.get(key(player_name, r.team)) || '';
      return {
        player_name,
        team: r.team,
        prop_type: r.prop_type,
        prop_line: r.line || r.prop_line || '',
        bet_type: r.bet_type,
        player_id,
        playerId: player_id,
      };
    });

    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json(out);
  } catch (e) {
    console.error(e);
    return res.status(200).json([]);
  }
}
