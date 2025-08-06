import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import Link from 'next/link';

const CSV_URL = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/weather_adjustments.csv';

function GamesPage() {
  const [games, setGames] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    fetch(CSV_URL)
      .then(response => {
        if (!response.ok) throw new Error('Network response was not ok');
        return response.text();
      })
      .then(csvText => {
        Papa.parse(csvText, {
          header: true,
          skipEmptyLines: true,
          complete: (results) => setGames(results.data),
          error: (err) => setError('Error parsing CSV: ' + err.message)
        });
      })
      .catch(err => setError('Error fetching CSV: ' + err.message));
  }, []);

  const generateSlug = (away, home) => {
    return `${away}-vs-${home}`.toLowerCase().replace(/\s+/g, '-');
  };

  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif', color: '#fff', backgroundColor: '#121212', minHeight: '100vh' }}>
      <h1 style={{ fontSize: '1.5em', marginBottom: '20px' }}>All Games</h1>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      {games.map((game, index) => {
        const slug = generateSlug(game.away_team, game.home_team);
        return (
          <Link href={`/game/${slug}`} passHref key={index}>
            <a style={{
              display: 'block',
              textDecoration: 'none',
              backgroundColor: '#1F1F1F',
              padding: '12px 16px',
              marginBottom: '10px',
              borderRadius: '8px',
              border: '1px solid #2F2F30',
              color: '#E0E0E0',
              fontSize: '1em',
              WebkitTapHighlightColor: 'transparent'
            }}>
              <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center'
              }}>
                <span>{game.away_team} @ {game.home_team}</span>
                <span style={{ fontSize: '0.8em', color: '#B0B0B0' }}>{Math.round(game.temperature)}°</span>
              </div>
            </a>
          </Link>
        );
      })}
    </div>
  );
}

export default GamesPage;
