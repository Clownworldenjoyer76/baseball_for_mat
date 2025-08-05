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
    // Use absolute paths for Vercel compatibility
    const weatherPath = path.join(process.cwd(), "data", "weather_adjustments.csv");
    const batterPath = path.join(process.cwd(), "data", "_projections", "batter_props_projected.csv");
    const pitcherPath = path.join(process.cwd(), "data", "_projections", "pitcher_props_projected.csv");

    const rawWeather = fs.readFileSync(weatherPath, "utf8");
    const rawBatters = fs.readFileSync(batterPath, "utf8");
    const rawPitchers = fs.readFileSync(pitcherPath, "utf8");

    const weather = parse(rawWeather, {
      columns: true,
      skip_empty_lines: true,
    });

    const batters = parse(rawBatters, {
      columns: true,
      skip_empty_lines: true,
    });

    const pitchers = parse(rawPitchers, {
      columns: true,
      skip_empty_lines: true,
    });

    const games = weather.map((row) => ({
      matchup: row.matchup,
      temp: parseFloat(row.temp).toFixed(0),
      time: row.time,
      props: [],
    }));

    const zscore = (val, mean, std) => {
      if (!val || !mean || !std || std == 0) return 0;
      return (val - mean) / std;
    };

    for (const b of batters) {
      const z = zscore(Number(b.hit), Number(b.hit_mean), Number(b.hit_std));
      const gameKey = games.find((g) => g.matchup === b.matchup);
      if (gameKey) {
        gameKey.props.push({
          name: b.name,
          stat: "H",
          value: b.hit,
          z,
          headshot: b.headshot_url || "/fallback_headshot.png",
        });
      }
    }

    for (const p of pitchers) {
      const z = zscore(Number(p.strikeouts), Number(p.k_mean), Number(p.k_std));
      const gameKey = games.find((g) => g.matchup === p.matchup);
      if (gameKey) {
        gameKey.props.push({
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
