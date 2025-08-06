import React from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import GameCard from '../components/GameCard';

function HomePage({ games }) {
  return (
    <div style={{ 
      padding: '15px', 
      fontFamily: "'Inter', sans-serif",
      fontWeight: '400',
      color: '#F5F5F5', 
      backgroundColor: '#000000', 
      minHeight: '100vh' 
    }}>
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

// getStaticProps has been updated with the new prop logic
export async function getStaticProps() {
  const parseCsv = (filePath) => {
    const csvFile = fs.readFileSync(filePath, 'utf-8');
    return Papa.parse(csvFile, { header: true, skipEmptyLines: true }).data;
  };

  const dataDir = path.join(process.cwd(), 'data');
  const gamesList = parseCsv(path.join(dataDir, 'weather_adjustments.csv'));
  const scoresList = parseCsv(path.join(dataDir, '_projections', 'final_scores_projected.csv'));
  const batterProps = parseCsv(path.join(dataDir, '_projections', 'batter_props_projected.csv'));

  // Helper to reformat "Last, First" to "First Last"
  const formatPlayerName = (name) => {
    if (!name || !name.includes(',')) return name;
    const parts = name.split(',');
    return `${parts[1].trim()} ${parts[0].trim()}`;
  };
  
  const gamesWithData = gamesList.map(game => {
    const homeTeamUpper = game.home_team.toUpperCase();
    const awayTeamUpper = game.away_team.toUpperCase();
    
    const homeScoreEntry = scoresList.find(s => s.team === homeTeamUpper);
    const awayScoreEntry = scoresList.find(s => s.team === awayTeamUpper);
    
    const projectedScore = homeScoreEntry && awayScoreEntry ? {
      home: Math.round(parseFloat(homeScoreEntry.projected_team_score)),
      away: Math.round(parseFloat(awayScoreEntry.projected_team_score)),
    } : null;

    const gameBatters = batterProps.filter(prop => 
      prop.team === game.home_team || prop.team === game.away_team
    );
    
    // Updated function to find top prop and format the output
    const findTopProp = (props, key, line) => {
      if (props.length === 0) return null;
      const topPlayer = props.reduce((prev, current) => 
        (parseFloat(prev[key]) > parseFloat(current[key])) ? prev : current
      );
      // Create the final prop string here
      return `${formatPlayerName(topPlayer.name)} ${line}`;
    };

    const topProps = [
      findTopProp(gameBatters, 'total_bases_projection', 'Total Bases Over 1.5'),
      findTopProp(gameBatters, 'total_hits_projection', 'Total Hits Over 1.5'),
      findTopProp(gameBatters, 'avg_hr', 'Home Runs Over 0.5')
    ].filter(p => p); // Filter out nulls

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
    revalidate: 3600,
  };
}

export default HomePage;
