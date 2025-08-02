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
    const weatherCsv = fs.readFileSync(path.join(projectionsPath, 'weather_adjustments.csv'), 'utf8');

    const batters = parse(batterCsv, { columns: true });
    const pitchers = parse(pitcherCsv, { columns: true });
    const weather = parse(weatherCsv, { columns: true });

    const games = {};

    // Combine data by game
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
     
