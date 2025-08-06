import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Papa from 'papaparse';

const weatherCSV = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/weather_adjustments.csv';
const battersCSV = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/_projections/batter_props_projected.csv';
const pitchersCSV = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/_projections/pitcher_props_projected.csv';

function GameDetailPage() {
  const { query } = useRouter();
  const slug = query.slug;

  const [game, setGame] = useState(null);
  const [pitchers, setPitchers] = useState([]);
  const [batters, setBatters] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!slug) return;

    const fetchData = async () => {
      try {
        const [weatherText, battersText, pitchersText] = await Promise.all([
          fetch(weatherCSV).then(r => r.text()),
          fetch(battersCSV).then(r => r.text()),
          fetch(pitchersCSV).then(r => r.text())
        ]);

        Papa.parse(weatherText, {
          header: true,
          skipEmptyLines: true,
          complete: ({ data: weatherData }) => {
            const match = weatherData.find(row => {
              const testSlug = `${row.away_team}-vs-${row.home_team}`.toLowerCase().replace(/\s+/g, '-');
              return testSlug === slug;
            });
            if (!match) return setError('Game not found');
            setGame(match);
          }
        });

        Papa.parse(battersText, {
          header: true,
          skipEmptyLines: true,
          complete: ({ data }) => setBatters(data)
        });

        Papa.parse(pitchersText, {
          header: true,
          skipEmptyLines: true,
          complete: ({ data }) => setPitchers(data)
        });

      } catch (err) {
        setError('Error loading data');
      }
    };

    fetchData();
  }, [slug]);

  if (error) return <div style={{ color: 'red', padding: '20px' }}>{error}</div>;
  if (!game) return <div style={{ padding: '20px', color: '#fff' }}>Loading...</div>;

  const title = `${game.away_team} @ ${game.home_team}`;
  const venue = game.venue;
  const conditions = [
    `${Math.round(game.temperature)}°`,
    game.wind_direction,
    `${game.wind_speed} mph`,
    `${game.humidity}%`,
    game.precipitation,
    game.condition
  ].filter(Boolean).join(', ');
  const gameTime = game.game_time;

  const normalized = str => str?.toLowerCase().replace(/\s+/g, '').trim();

  const starters = pitchers.filter(p => {
    return p.team && p.name &&
      [normalized(game.away_team), normalized(game.home_team)].includes(normalized(p.team));
  });

  const startersLine = starters.length === 2
    ? `${starters[0].name} ${starters[0].team} vs ${starters[1].name} ${starters[1].team}`
    : 'Starting pitchers TBD';

  const filteredPicks = batters
    .filter(b =>
      b.team &&
      [normalized(game.away_team), normalized(game.home_team)].includes(normalized(b.team)) &&
      ['total_bases', 'hits', 'home_runs'].includes(b.prop_type) &&
      b.z_score && b.line && b.name
    )
    .sort((a, b) => parseFloat(b.z_score) - parseFloat(a.z_score));

  const topPicks = [
    ...filteredPicks.filter(p => p.prop_type === 'total_bases').slice(0, 2),
    ...filteredPicks.filter(p => p.prop_type === 'hits').slice(0, 2),
    ...filteredPicks.filter(p => p.prop_type === 'home_runs').slice(0, 1)
  ];

  return (
    <div style={{ backgroundColor: '#121212', color: '#fff', padding: '20px', fontFamily: 'sans-serif', minHeight: '100vh' }}>
      <h1 style={{ fontSize: '1.5em', marginBottom: '10px' }}>{title}</h1>

      <p style={{ color: '#B0B0B0', marginBottom: '4px' }}>{venue}</p>
      <p style={{ color: '#B0B0B0', marginBottom: '4px' }}>{conditions}</p>
      <p style={{ color: '#B0B0B0', marginBottom: '10px' }}>{gameTime}</p>

      <p style={{ fontWeight: 'bold', color: '#F59E0B', marginBottom: '10px' }}>{startersLine}</p>

      <h3 style={{ color: '#D4AF37', marginTop: '20px', marginBottom: '10px' }}>Top 5 Picks</h3>
      {topPicks.length === 0 ? (
        <p style={{ color: '#888' }}>No props available for this game.</p>
      ) : (
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {topPicks.map((pick, i) => (
            <li key={i} style={{ marginBottom: '12px' }}>
              <strong>{pick.name}</strong> – {pick.prop_type.replace('_', ' ')} over {pick.line} (<span style={{ color: '#9CA3AF' }}>z: {pick.z_score}</span>)
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default GameDetailPage;
