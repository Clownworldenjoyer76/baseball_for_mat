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

  const formatERA = (era) => {
    const n = parseFloat(era);
    return isNaN(n) ? '—' : `${n.toFixed(2)} ERA`;
  };

  const gameTime = game.game_time;

  const normalized = str => str?.toLowerCase().replace(/\s+/g, '').trim();

  const starters = pitchers.filter(p =>
    p.team && p.name &&
    [normalized(game.away_team), normalized(game.home_team)].includes(normalized(p.team))
  );

  const formatName = (fullName) => {
    const [last, first] = fullName.split(', ');
    return `${first} ${last}`;
  };

  const startersLine = starters.length === 2
    ? `${formatName(starters[0].name)}, ${starters[0].team}, ${formatERA(starters[0].era)}
@
${formatName(starters[1].name)}, ${starters[1].team}, ${formatERA(starters[1].era)}`
    : 'Starting pitchers TBD';

  const formatLabel = (type) => {
    if (type === 'total_bases') return 'Total Bases';
    if (type === 'hits') return 'Hits';
    if (type === 'home_runs') return 'Home Runs';
    return type;
  };

  const getHeadshotUrl = (playerId) =>
    `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;

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

      <div style={{ color: '#B0B0B0', marginBottom: '10px', lineHeight: '1.6' }}>
        <div>{Math.round(game.temperature)}°</div>
        <div>Wind {game.wind_speed} MPH Blowing {game.wind_direction}</div>
        <div>{game.humidity}% Humidity</div>
        <div>{parseFloat(game.precipitation || 0).toFixed(2)}% Chance of Rain</div>
        <div>{game.condition}</div>
      </div>

      <p style={{ color: '#B0B0B0', marginBottom: '10px' }}>{gameTime}</p>

      <p style={{ fontWeight: 'bold', color: '#F59E0B', marginBottom: '10px', whiteSpace: 'pre-line' }}>
        {startersLine}
      </p>

      <h3 style={{ color: '#D4AF37', marginTop: '20px', marginBottom: '15px', textAlign: 'center' }}>Top Props</h3>
      {topPicks.length === 0 ? (
        <p style={{ color: '#888' }}>No props available for this game.</p>
      ) : (
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#E0E0E0' }}>
          {topPicks.map((prop, index) => (
            <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
              <img
                src={getHeadshotUrl(prop.playerId)}
                alt={prop.name}
                style={{
                  height: '50px',
                  width: '50px',
                  borderRadius: '50%',
                  marginRight: '15px',
                  backgroundColor: '#2F2F30',
                  objectFit: 'cover'
                }}
              />
              <div>
                <div style={{ fontSize: '1em' }}>{prop.name}</div>
                <div style={{ fontSize: '0.9em', color: '#B0B0B0', marginLeft: '10px' }}>
                  Over {prop.line} {formatLabel(prop.prop_type)}
                </div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default GameDetailPage;
