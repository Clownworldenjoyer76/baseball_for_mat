import React from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import GameCard from '../components/GameCard';

function HomePage({ games }) {
  return (
    <div style={{ padding: '15px', fontFamily: 'sans-serif', color: '#000', backgroundColor: '#fff', minHeight: '100vh' }}>
      <h1 style={{ textAlign: 'center' }}>Today's Top MLB Picks and Props</h1>
      {games.map((gameData, index) => (
        <GameCard 
          key={index}
          game={gameData.gameInfo}
          topProps={gameData.topProps}
          projectedScore={gameData.projectedScore}
        />
      ))}
    </div>
  );
}

export async function getStaticProps() {
  // Helper function to read and parse a CSV file
  const parseCsv = (filePath) => {
    const csvFile = fs.readFileSync(filePath, 'utf-8');
    return Papa.parse(csvFile, { header: true, skipEmptyLines: true }).data;
  };

  // --- 1. Load all data ---
  const dataDir = path.join(process.cwd(), 'data');
  const gamesList = parseCsv(path.join(dataDir, 'weather_adjustments.csv'));
  const scoresList = parseCsv(path.join(dataDir, '_projections', 'final_scores_projected.csv'));
  const batterProps = parseCsv(path.join(dataDir, '_projections', 'batter_props_projected.csv'));

  // --- 2. Process and combine data for each game ---
  const gamesWithData = gamesList.map(game => {
    // --- Corrected Projected Score Logic ---
    const homeTeamUpper = game.home_team.toUpperCase();
    const awayTeamUpper = game.away_team.toUpperCase();
    
    const homeScoreEntry = scoresList.find(s => s.team === homeTeamUpper);
    const awayScoreEntry = scoresList.find(s => s.team === awayTeamUpper);
    
    const projectedScore = homeScoreEntry && awayScoreEntry ? {
      home: Math.round(parseFloat(homeScoreEntry.projected_team_score)),
      away: Math.round(parseFloat(awayScoreEntry.projected_team_score)),
    } : null;

    // --- Corrected Top 3 Props Logic ---
    const gameBatters = batterProps.filter(prop => 
      prop.team === game.home_team || prop.team === game.away_team
    );
    
    // Helper to find the best prop of a certain type
    const findTopProp = (props, key, typeName) => {
      if (props.length === 0) return null;
      const topPlayer = props.reduce((prev, current) => 
        (parseFloat(prev[key]) > parseFloat(current[key])) ? prev : current
      );
      return { name: topPlayer.name, type: typeName };
    };

    const topProps = [
      findTopProp(gameBatters, 'total_bases_projection', 'Total Bases'),
      findTopProp(gameBatters, 'total_hits_projection', 'Total Hits'),
      findTopProp(gameBatters, 'avg_hr', 'Home Runs')
    ].filter(p => p); // Filter out nulls if no players found

    return {
      gameInfo: game,
      topProps: topProps,
      projectedScore: projectedScore,
    };
  });

  return {
    props: {
      games: gamesWithData,
    },
    revalidate: 3600, // Rebuild the page every hour
  };
}

export default HomePage;
