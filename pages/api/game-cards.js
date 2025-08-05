import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

export default function handler(req, res) {
  try {
    const batterPath = path.join(process.cwd(), 'data/_projections/batter_props_projected.csv');
    const pitcherPath = path.join(process.cwd(), 'data/_projections/pitcher_props_projected.csv');
    const weatherPath = path.join(process.cwd(), 'data/weather_adjustments.csv');

    const batterCSV = fs.readFileSync(batterPath, 'utf8');
    const pitcherCSV = fs.readFileSync(pitcherPath, 'utf8');
    const weatherCSV = fs.readFileSync(weatherPath, 'utf8');

    const batters = parse(batterCSV, { columns: true });
    const pitchers = parse(pitcherCSV, { columns: true });
    const weather = parse(weatherCSV, { columns: true });

    res.status(200).json({ batters, pitchers, weather });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to load data' });
  }
}
