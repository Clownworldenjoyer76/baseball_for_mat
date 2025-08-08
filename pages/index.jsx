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

// CORRECTED getStaticProps function
export async function getStaticProps() {
  // Helper function to read and parse a CSV file
  const parseCsv = (filePath) => {
    try {
      const csvFile = fs.readFileSync(filePath, 'utf-8');
      return Papa.parse(csvFile, { header: true, skipEmptyLines: true }).data;
    } catch (error) {
      console.error(`Error reading file: ${filePath}`, error);
      return []; // Return empty array on error
    }
  };

  // Define file paths
  const dataDir = path.join(process.cwd(), 'data');
  const gamesList = parseCsv(path.join(dataDir, 'weather_adjustments.csv'));
  const scoresList = parseCsv(path.join(dataDir, '_projections', 'final_scores_projected.csv'));
  const batterProps = parseCsv(path.join(dataDir, '_projections', 'batter_props_projected.csv'));

  const gamesWithData = gamesList.map(game => {
    // 1. Find the projected score for this specific game
    let projectedScore = null;
    const scoreEntry = scoresList.find(s =>
      s.home_team.trim() === game.home_team.trim() &&
      s.away_team.trim() === game.away_team.trim()
    );

    if (scoreEntry) {
      const home = parseFloat(scoreEntry.home_score);
      const away = parseFloat(scoreEntry.away_score);
      projectedScore = {
        home: home,
        away: away,
        total: (home + away).toFixed(2)
      };
    }

    // 2. Find the top props for this game
    const gameBatters = batterProps.filter(prop => 
      prop.team.trim() === game.home_team.trim() || 
      prop.team.trim() === game.away_team.trim()
    );

    const findTopProp = (props, key) => {
      if (!props || props.length === 0) return null;
      // Filter out players with non-numeric or missing values for the key
      const validProps = props.filter(p => p && p[key] && !isNaN(parseFloat(p[key])));
      if (validProps.length === 0) return null;
      
      const topPlayer = validProps.reduce((prev, current) => 
        (parseFloat(prev[key]) > parseFloat(current[key])) ? prev : current
      );
      
      // Determine the line text based on the key
      let lineText = '';
      if (key.includes('bases')) lineText = `Total Bases Over ${topPlayer.total_bases_line || '1.5'}`;
      else if (key.includes('hits')) lineText = `Total Hits Over ${topPlayer.total_hits_line || '1.5'}`;
      else if (key.includes('hr')) lineText = `Home Runs Over ${topPlayer.hr_line || '0.5'}`;

      return { 
        name: topPlayer.name, 
        line: lineText, 
        playerId: topPlayer.player_id 
      };
    };
    
    // Create a list of top props for the card
    const topProps = [
      findTopProp(gameBatters, 'total_bases_projection'),
      findTopProp(gameBatters, 'total_hits_projection'),
      findTopProp(gameBatters, 'hr_projection')
    ].filter(p => p); // Filter out any null results

    // Return the combined data object for this game
    return {
      gameInfo: game,
      topProps: topProps,
      projectedScore: projectedScore
    };
  });

  return {
    props: {
      games: gamesWithData
    },
    // Re-generate the page at most once per hour
    revalidate: 3600
  };
}


export default HomePage;
