import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync'; // ✅ FIXED: use sync parser

export const config = {
  api: {
    bodyParser: false,
  },
};

export default function handler(req, res) {
  try {
    const projectionsPath = path.resolve('./data/_projections');

    // ✅ Read file contents
    const batterCsv = fs.readFileSync(path.join(projectionsPath, 'batter_props_projected.csv'), 'utf8');
    const pitcherCsv = fs.readFileSync(path.join(projectionsPath, 'pitcher_props_projected.csv'), 'utf8');
    const weatherCsv = fs.readFileSync(path.resolve('./data/weather_adjustments.csv'), 'utf8');

    // ✅ FIXED: parse CSVs to arrays using csv-parse/sync
    const batters = parse(batterCsv, { columns: true, skip_empty_lines: true });
    const pitchers = parse(pitcherCsv, { columns: true, skip_empty_lines: true });
    const weather = parse(weatherCsv, { columns: true, skip_empty_lines: true });

    const games = {};

    // Build game objects using weather
    weather.forEach((w) => {
      const key = `${w.away_team} @ ${w.home_team}`;
      games[key] = {
        matchup: key,
        temp: Math.round(Number(w.temperature)),
        time: w.game_time || '',
        props: [],
      };
    });

    // Add batter props
    const batterKeys = ['hit', 'home_run', 'total_bases', 'rbi'];
    const zScore = (row, keys) =>
      Math.max(...keys.map(k => Number(row[`${k}_z`]) || -Infinity));

    batters.forEach((b) => {
      const gameKey = `${b.away_team} @ ${b.home_team}`;
      if (!games[gameKey]) return;
      games[gameKey].props.push({
        type: 'batter',
        name: b.name,
        stat: 'hit',
        value: b['hit'],
        z: zScore(b, batterKeys),
        headshot: b.headshot || '',
      });
    });

    // Add pitcher props
    const pitcherKeys = ['strikeouts', 'outs'];
    pitchers.forEach((p) => {
      const gameKey = `${p.away_team} @ ${p.home_team}`;
      if (!games[gameKey]) return;
      games[gameKey].props.push({
        type: 'pitcher',
        name: p.name,
        stat: 'strikeouts',
        value: p['strikeouts'],
        z: zScore(p, pitcherKeys),
        headshot: p.headshot || '',
      });
    });

    // Trim to top 5 props
    const final = Object.values(games).map(game => {
      game.props.sort((a, b) => b.z - a.z);
      game.props = game.props.slice(0, 5);
      return game;
    });

    res.status(200).json(final);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to process game card data' });
  }
}
