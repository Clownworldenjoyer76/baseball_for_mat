import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';

export const config = {
  api: {
    bodyParser: false,
  },
};

export default function handler(req, res) {
  try {
    const projectionsPath = path.resolve('./data/_projections');

    const batterCsv = fs.readFileSync(path.join(projectionsPath, 'batter_props_projected.csv'), 'utf8');
    const pitcherCsv = fs.readFileSync(path.join(projectionsPath, 'pitcher_props_projected.csv'), 'utf8');
    const weatherCsv = fs.readFileSync(path.resolve('./data/weather_adjustments.csv'), 'utf8');

    const batters = parse(batterCsv, { columns: true, skip_empty_lines: true });
    const pitchers = parse(pitcherCsv, { columns: true, skip_empty_lines: true });
    const weather = parse(weatherCsv, { columns: true, skip_empty_lines: true });

    const games = {};

    weather.forEach((w) => {
      const key = `${w.away_team} @ ${w.home_team}`;
      games[key] = {
        matchup: key,
        temp: Math.round(Number(w.temperature)),
        time: w.game_time || '',
        props: [],
      };
    });

    const zScore = (row, keys) =>
      Math.max(...keys.map(k => Number(row[`${k}_z`]) || -Infinity));

    const batterKeys = ['hit', 'home_run', 'total_bases', 'rbi'];
    batters.forEach((b) => {
      const gameKey = `${b.away_team} @ ${b.home_team}`;
      if (!games[gameKey]) return;
      games[gameKey].props.push({
        type: 'batter',
        name: b.name,
        stat: b.top_stat,
        value: b[`b_${b.top_stat}`],
        z: zScore(b, batterKeys),
        headshot: b.headshot || '',
      });
    });

    const pitcherKeys = ['strikeouts', 'outs'];
    pitchers.forEach((p) => {
      const gameKey = `${p.away_team} @ ${p.home_team}`;
      if (!games[gameKey]) return;
      games[gameKey].props.push({
        type: 'pitcher',
        name: p.name,
        stat: p.top_stat,
        value: p[`p_${p.top_stat}`],
        z: zScore(p, pitcherKeys),
        headshot: p.headshot || '',
      });
    });

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
