import React from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';

function BetHistoryPage({ playerProps, gameProps }) {
  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif', color: '#fff', backgroundColor: '#121212', minHeight: '100vh' }}>
      <h1 style={{ fontSize: '1.5em', marginBottom: '20px' }}>Bet History</h1>

      {/* Player Props Section */}
      <h2 style={{ fontSize: '1.2em', marginBottom: '10px', color: '#D4AF37' }}>Player Props</h2>
      {playerProps.length === 0 ? (
        <p>No player props have been recorded yet.</p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
          {playerProps.map((prop, index) => (
            <div 
              key={index} 
              style={{
                backgroundColor: '#1F1F1F',
                padding: '16px',
                borderRadius: '8px',
                border: '1px solid #2F2F30'
              }}
            >
              <h3 style={{ fontSize: '1em', margin: '0 0 4px 0', color: '#E0E0E0' }}>
                {prop.player_name} ({prop.team})
              </h3>
              <p style={{ fontSize: '0.9em', margin: '0 0 4px 0', color: '#B0B0B0' }}>
                Prop: {prop.prop_line}
              </p>
              <p style={{ fontSize: '0.9em', margin: '0 0 4px 0', color: '#B0B0B0' }}>
                Type: {prop.prop_type}
              </p>
              <p style={{ fontSize: '0.8em', margin: '8px 0 0 0', color: '#6B7280' }}>
                Date: {prop.date}
              </p>
            </div>
          ))}
        </div>
      )}

      {/* Game Props Section */}
      <h2 style={{ fontSize: '1.2em', marginTop: '30px', marginBottom: '10px', color: '#D4AF37' }}>Game Props</h2>
      {gameProps.length === 0 ? (
        <p>No game props have been recorded yet.</p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
          {gameProps.map((prop, index) => (
            <div 
              key={index} 
              style={{
                backgroundColor: '#1F1F1F',
                padding: '16px',
                borderRadius: '8px',
                border: '1px solid #2F2F30'
              }}
            >
              <h3 style={{ fontSize: '1em', margin: '0 0 4px 0', color: '#E0E0E0' }}>
                {prop.away_team} at {prop.home_team}
              </h3>
              <p style={{ fontSize: '0.9em', margin: '0 0 4px 0', color: '#B0B0B0' }}>
                Moneyline: {prop.moneyline}
              </p>
              <p style={{ fontSize: '0.9em', margin: '0 0 4px 0', color: '#B0B0B0' }}>
                Real Run Total: {prop.real_run_total}
              </p>
              <p style={{ fontSize: '0.8em', margin: '8px 0 0 0', color: '#6B7280' }}>
                Date: {prop.date}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export async function getStaticProps() {
  const parseCsv = (filePath) => {
    try {
      const csvFile = fs.readFileSync(filePath, 'utf-8');
      return Papa.parse(csvFile, { header: true, skipEmptyLines: true }).data;
    } catch (error) {
      console.error(`Error reading file: ${filePath}`, error);
      return [];
    }
  };

  const dataDir = path.join(process.cwd(), 'data');
  const playerPropsList = parseCsv(path.join(dataDir, 'player_props_history.csv'));
  const gamePropsList = parseCsv(path.join(dataDir, 'game_props_history.csv'));

  return {
    props: { 
      playerProps: playerPropsList,
      gameProps: gamePropsList
    },
    revalidate: 1
  };
}

export default BetHistoryPage;
