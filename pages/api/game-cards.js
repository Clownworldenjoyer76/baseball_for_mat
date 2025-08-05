import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";

export const config = {
  api: {
    bodyParser: false,
  },
};

export default async function handler(req, res) {
  try {
    const weatherPath = path.join(process.cwd(), "data", "weather_adjustments.csv");
    const batterPath = path.join(process.cwd(), "data", "_projections", "batter_props_projected.csv");
    const pitcherPath = path.join(process.cwd(), "data", "_projections", "pitcher_props_projected.csv");

    const rawWeather = fs.readFileSync(weatherPath, "utf8");
    const rawBatters = fs.readFileSync(batterPath, "utf8");
    const rawPitchers = fs.readFileSync(pitcherPath, "utf8");

    const weather = parse(rawWeather, { columns: true, skip_empty_lines: true });
    const batters = parse(rawBatters, { columns: true, skip_empty_lines: true });
    const pitchers = parse(rawPitchers, { columns: true, skip_empty_lines: true });

    // Create games array using away_team @ home_team
    const games = weather.map((row) => ({
      matchup: `${row.away_team} @ ${row.home_team}`,
      temp: parseFloat(row.temperature).toFixed(0),
      time: row.game_time,
      props: [],
    }));

    const zscore = (val, mean, std) => {
      if (!val || !mean || !std || std == 0) return 0;
      return (val - mean) / std;
    };

    for (const b of batters) {
      const gameKey = `${b.away_team} @ ${b.home_team}`;
      const game = games.find((g) => g.matchup === gameKey);
      const z = zscore(Number(b.hit), Number(b.hit_mean), Number(b.hit_std));
      if (game) {
        game.props.push({
          name: b.name,
          stat: "H",
          value: b.hit,
          z,
          headshot: b.headshot_url || "/fallback_headshot.png",
        });
      }
    }

    for (const p of pitchers) {
      const gameKey = `${p.away_team} @ ${p.home_team}`;
      const game = games.find((g) => g.matchup === gameKey);
      const z = zscore(Number(p.strikeouts), Number(p.k_mean), Number(p.k_std));
      if (game) {
        game.props.push({
          name: p.name,
          stat: "K",
          value: p.strikeouts,
          z,
          headshot: p.headshot_url || "/fallback_headshot.png",
        });
      }
    }

    for (const g of games) {
      g.props = g.props.sort((a, b) => b.z - a.z).slice(0, 5);
    }

    res.status(200).json(games);
  } catch (err) {
    console.error("❌ Error in /api/game-cards:", err);
    res.status(500).json({ error: "Internal server error" });
  }
}
