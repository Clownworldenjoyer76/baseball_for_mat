import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';

// The full URL to the raw CSV file on your GitHub.
const CSV_URL = 'https://raw.githubusercontent.com/Clownworldenjoyer76/sports-betting-site/main/data/weather_adjustments.csv';

function GamesPage() {
  const [games, setGames] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    fetch(CSV_URL)
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.text();
      })
      .then(csvText => {
        Papa.parse(csvText, {
          header: true,
          skipEmptyLines: true,
          complete: (results) => {
            setGames(results.data);
          },
          error: (err) => {
            setError('Error parsing CSV file: ' + err.message);
          }
        });
      })
      .catch(err => {
        setError('Error fetching the CSV file: ' + err.message);
      });
  }, []);

  return (
    <div style={{ padding: '15px', fontFamily: 'sans-serif', color: '#fff', backgroundColor: '#121212' }}>
      <h1>Game Conditions</h1>
      
      {error && <p style={{ color: 'red' }}>{error}</p>}
      
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '600px' }}>
          <thead>
            <tr style={{ borderBottom: '2px solid #444' }}>
              <th style={{ padding: '10px', textAlign: 'left' }}>Away</th>
              <th style={{ padding: '10px', textAlign: 'left' }}>Home</th>
              <th style={{ padding: '10px', textAlign: 'left' }}>Venue</th>
              <th style={{ padding: '10px', textAlign: 'left' }}>Temp</th>
            </tr>
          </thead>
          <tbody>
            {games.map((game, index) => (
              <tr key={index} style={{ borderBottom: '1px solid #333' }}>
                <td style={{ padding: '10px' }}>{game.away_team}</td>
                <td style={{ padding: '10px' }}>{game.home_team}</td>
                <td style={{ padding: '10px' }}>{game.venue}</td>
                <td style={{ padding: '10px' }}>{game.temperature}°</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default GamesPage;
