// pages/api/game-cards.js

import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';

function readCSV(filepath) {
  const file = fs.readFileSync(filepath, 'utf8');
  return Papa.parse(file, { header: true }).data;
}

function zScoreToProbability(z) {
  const erf = x => {
    const sign = x >= 0 ? 1 : -1;
    x = Math.abs(x);
    const t = 1 / (1 + 0.3275911 * x);
    const y = 1 - (((((
      +1.061405429 * t
      - 1.453152027) * t)
      + 1.421413741) * t
      - 0.284496736) * t
      + 0.254829592) * t * Math.exp(-x * x);
    return sign * y;
  };
  return Math.round((0.5 * (1 + erf(z / Math.sqrt(2))) * 100));
}

function formatGameTime(raw) {
  if (!raw) return '';
  const match = raw.trim().match(/^0?(\d+):(\d+)\s*(AM|PM)$/i);
  if (!match) return raw;
  const [, h, m, meridian] = match;
  return `${h}:${m} ${meridian.toUpperCase().replace('AM', 'A.M.').replace('PM', 'P.M.')}`;
}

export default function handler(req, res) {
  const batterPath = path.resolve('data/_projections/batter_props_projected.csv');
  const pitcherPath = path.resolve('data/_projections/pitcher_props_projected.csv');
  const weatherPath = path.resolve('data/weather_adjustments.csv');
  const scoresPath = path.resolve('data/_projections/final_scores_projected.csv');

  const batters = readCSV(batterPath);
  const pitchers = readCSV(pitcherPath);
  const weather = readCSV(weatherPath);
  const scores = readCSV(scoresPath);

  const games = scores.map(row => {
    const game = `${row.away_team} @ ${row.home_team}`;
    const weatherEntry = weather.find(w =>
      w.home_team === row.home_team && w.away_team === row.away_team
    );

    const temp = weatherEntry?.temperature || '';
    const rawTime = weatherEntry?.game_time || '';
    const formattedTime = formatGameTime(rawTime);

    const batterProps = batters.filter(p =>
      p.home_team === row.home_team && p.away_team === row.away_team
    ).map(p => ({
      player: p.name,
      stat: p.stat_type,
      z_score: parseFloat(p.z_score || 0)
    }));

    const pitcherProps = pitchers.filter(p =>
      p.home_team === row.home_team && p.away_team === row.away_team
    ).map(p => ({
      player: p.name,
      stat: p.stat_type,
      z_score: parseFloat(p.z_score || 0)
    }));

    const allProps = [...batterProps, ...pitcherProps]
      .filter(p => !isNaN(p.z_score))
      .sort((a, b) => b.z_score - a.z_score)
      .slice(0, 5);

    return {
      game,
      home_team: row.home_team,
      away_team: row.away_team,
      temperature: temp,
      game_time: formattedTime,
      top_props: allProps
    };
  });

  res.status(200).json(games);
}
