import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

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

    // 🔍 Log parsed data for debugging
    console.log("✅ WEATHER", weather.length, weather.slice(0, 2));
    console.log("✅ BATTERS", batters.length, batters.slice(0, 2));
    console.log("✅ PITCHERS", pitchers.length, pitchers.slice(0, 2));

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

    const batterKeys = ['hit', 'home_run', 'total_bases', 'rbi'];
    const zScore = (row, keys) =>
      Math.max(...keys.map(k => Number(row[`${k}_z`]) || -Infinity));

    batters.forEach((b) => {
      const gameKey = `${b.away_team} @ ${b.home_team}`;
      if (!games[gameKey]) {
        console.log(`⚠️ No weather entry for batter gameKey: ${gameKey}`);
        return;
      }
      games[gameKey].props.push({
        type: 'batter',
        name: b.name,
        stat: 'hit',
        value: b['hit'],
        z: zScore(b, batterKeys),
        headshot: b.headshot || '',
      });
    });

    const pitcherKeys = ['strikeouts', 'outs'];
    pitchers.forEach((p) => {
      const gameKey = `${p.away_team} @ ${p.home_team}`;
      if (!games[gameKey]) {
        console.log(`⚠️ No weather entry for pitcher gameKey: ${gameKey}`);
        return;
      }
      games[gameKey].props.push({
        type: 'pitcher',
        name: p.name,
        stat: 'strikeouts',
        value: p['strikeouts'],
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
    console.error("❌ API Error:", err);
    res.status(500).json({ error: 'Failed to process game card data' });
  }
}
