
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';

function zScore(series) {
  const valid = series.map(Number).filter(n => !isNaN(n));
  const mean = valid.reduce((a, b) => a + b, 0) / valid.length;
  const std = Math.sqrt(valid.reduce((acc, x) => acc + (x - mean) ** 2, 0) / valid.length);
  return series.map(x => std ? (x - mean) / std : 0);
}

export default async function handler(req, res) {
  try {
    const root = process.cwd();

    const batterCSV = fs.readFileSync(path.join(root, 'data/_projections/batter_props_projected.csv'), 'utf8');
    const pitcherCSV = fs.readFileSync(path.join(root, 'data/_projections/pitcher_props_projected.csv'), 'utf8');
    const weatherCSV = fs.readFileSync(path.join(root, 'data/weather_adjustments.csv'), 'utf8');

    const batter = Papa.parse(batterCSV, { header: true }).data;
    const pitcher = Papa.parse(pitcherCSV, { header: true }).data;
    const weather = Papa.parse(weatherCSV, { header: true }).data;

    const weatherMap = {};
    for (const w of weather) {
      const key = `${w.away_team} @ ${w.home_team}`;
      weatherMap[key] = w;
    }

    const batterGames = [];
    for (const b of batter) {
      const game = Object.keys(weatherMap).find(key => key.includes(b.team));
      if (!game) continue;
      batterGames.push({
        name: b.name,
        game_key: game,
        hit: parseFloat(b.total_hits_projection),
        hr: parseFloat(b.avg_hr)
      });
    }

    const pitcherGames = [];
    for (const p of pitcher) {
      const game = Object.keys(weatherMap).find(key => key.includes(p.team));
      if (!game) continue;
      pitcherGames.push({
        name: p.name,
        game_key: game,
        era: parseFloat(p.era)
      });
    }

    const zHit = zScore(batterGames.map(x => x.hit));
    const zHr = zScore(batterGames.map(x => x.hr));
    const zEra = zScore(pitcherGames.map(x => -x.era)); // lower is better

    const props = [];

    batterGames.forEach((b, i) => {
      props.push({ name: b.name, stat: 'hit', z: zHit[i], game_key: b.game_key });
      props.push({ name: b.name, stat: 'hr', z: zHr[i], game_key: b.game_key });
    });

    pitcherGames.forEach((p, i) => {
      props.push({ name: p.name, stat: 'era', z: zEra[i], game_key: p.game_key });
    });

    const grouped = {};
    for (const p of props) {
      if (!grouped[p.game_key]) grouped[p.game_key] = [];
      grouped[p.game_key].push(p);
    }

    const final = Object.entries(grouped).map(([game_key, props]) => {
      const weatherRow = weatherMap[game_key];
      const sorted = props.sort((a, b) => b.z - a.z).slice(0, 5);
      return {
        game: game_key,
        temperature: Math.round(parseFloat(weatherRow.temperature)),
        top_props: sorted.map(row => ({
          player: row.name,
          stat: row.stat,
          z_score: +row.z.toFixed(2)
        }))
      };
    });

    const date = new Date().toISOString().split("T")[0];
    const historyPath = path.join(root, 'data/history');
    if (!fs.existsSync(historyPath)) {
      fs.mkdirSync(historyPath, { recursive: true });
    }
    const savePath = path.join(historyPath, `${date}.json`);
    fs.writeFileSync(savePath, JSON.stringify(final, null, 2), 'utf8');

    res.status(200).json(final);
  } catch (err) {
    res.status(500).json({ success: false, error: err.toString() });
  }
}
