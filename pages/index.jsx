import React, { useState, useEffect } from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import GameCard from '../components/GameCard';

// This is a new component for the gray placeholder cards
const SkeletonCard = () => (
  <div style={{ 
    backgroundColor: '#1F2937', 
    margin: '20px 0', 
    borderRadius: '12px', 
    padding: '20px',
    height: '350px' // Approximate height of a real card
  }}></div>
);


function HomePage({ games }) {
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // This simulates loading so you can see the skeleton and animations.
    const timer = setTimeout(() => {
      setLoading(false);
    }, 1500); // 1.5 seconds
    return () => clearTimeout(timer);
  }, []);

  return (
    <div style={{ 
      padding: '15px', 
      fontFamily: "'Inter', sans-serif",
      fontWeight: '400',
      color: '#D1D5DB', // Primary Text
      backgroundColor: '#111827', // Main Background
      minHeight: '100vh' 
    }}>
      <h1 style={{ textAlign: 'center' }}>Today's Top MLB Picks and Props</h1>

      {loading ? (
        // If loading, show 5 skeleton cards
        <>
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
        </>
      ) : (
        // If not loading, show the real game cards
        games.map((gameData, index) => (
          <GameCard 
            key={index}
            game={gameData.gameInfo}
            topProps={gameData.topProps}
            projectedScore={gameData.projectedScore}
            animationDelay={`${index * 100}ms`} // Stagger animation
          />
        ))
      )}
    </div>
  );
}

// getStaticProps function remains the same
export async function getStaticProps() {
  const parseCsv = (filePath) => {
    const csvFile = fs.readFileSync(filePath, 'utf-8');
    return Papa.parse(csvFile, { header: true, skipEmptyLines: true }).data;
  };
  const dataDir = path.join(process.cwd(), 'data');
  const gamesList = parseCsv(path.join(dataDir, 'weather_adjustments.csv'));
  const scoresList = parseCsv(path.join(dataDir, '_projections', 'final_scores_projected.csv'));
  const batterProps = parseCsv(path.join(dataDir, '_projections', 'batter_props_projected.csv'));
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
    let projectedScore = null;
    if (homeScoreEntry && awayScoreEntry) {
      const rawHome = parseFloat(homeScoreEntry.projected_team_score);
      const rawAway = parseFloat(awayScoreEntry.projected_team_score);
      let roundedHome = Math.round(rawHome);
      let roundedAway = Math.round(rawAway);
      if (roundedHome === roundedAway) {
        if (rawHome >= rawAway) { roundedHome++; } else { roundedAway++; }
      }
      projectedScore = { home: roundedHome, away: roundedAway, total: (rawHome + rawAway).toFixed(2) };
    }
    const gameBatters = batterProps.filter(prop => prop.team === game.home_team || prop.team === game.away_team);
    const findTopProp = (props, key, line) => {
      if (props.length === 0) return null;
      const topPlayer = props.reduce((prev, current) => (parseFloat(prev[key]) > parseFloat(current[key])) ? prev : current);
      return { name: formatPlayerName(topPlayer.name), line: line, playerId: topPlayer.player_id };
    };
    const topProps = [
      findTopProp(gameBatters, 'total_bases_projection', 'Total Bases Over 1.5'),
      findTopProp(gameBatters, 'total_hits_projection', 'Total Hits Over 1.5'),
      findTopProp(gameBatters, 'avg_hr', 'Home Runs Over 0.5')
    ].filter(p => p);
    return { gameInfo: game, topProps: topProps, projectedScore: projectedScore };
  });
  return { props: { games: gamesWithData }, revalidate: 3600 };
}

export default HomePage;

  
