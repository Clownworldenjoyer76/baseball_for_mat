import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Papa from 'papaparse';

const CSV_URL = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/weather_adjustments.csv';

function GameDetailPage() {
  const router = useRouter();
  const { slug } = router.query;

  const [game, setGame] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!slug) return;

    fetch(CSV_URL)
      .then(res => res.text())
      .then(csvText => {
        Papa.parse(csvText, {
          header: true,
          skipEmptyLines: true,
          complete: (results) => {
            const match = results.data.find(row => {
              const generatedSlug = `${row.away_team}-vs-${row.home_team}`.toLowerCase().replace(/\s+/g, '-');
              return generatedSlug === slug;
            });
            if (match) {
              setGame(match);
            } else {
              setError('Game not found');
            }
          },
          error: (err) => setError('Error parsing CSV: ' + err.message),
        });
      })
      .catch(err => setError('Error fetching data: ' + err.message));
  }, [slug]);

  if (error) {
    return <div style={{ color: 'red', padding: '20px' }}>{error}</div>;
  }

  if (!game) {
    return <div style={{ color: '#fff', padding: '20px' }}>Loading game data...</div>;
  }

  return (
    <div style={{ padding: '20px', backgroundColor: '#121212', minHeight: '100vh', color: '#fff', fontFamily: 'sans-serif' }}>
      <h1 style={{ fontSize: '1.5em', marginBottom: '10px' }}>
        {game.away_team} @ {game.home_team}
      </h1>
      <p style={{ color: '#B0B0B0' }}>Venue: {game.venue}</p>
      <p style={{ color: '#B0B0B0' }}>Temperature: {Math.round(game.temperature)}°</p>
      {/* More data sections can go here later */}
    </div>
  );
}

export default GameDetailPage;
