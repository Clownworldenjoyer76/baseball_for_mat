import React, { useState, useEffect } from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import GameCard from '../components/GameCard';
import TopPropsCard from '../components/TopPropsCard';

const SkeletonCard = () => (
  <div style={{ 
    backgroundColor: '#1F2937', 
    margin: '20px 0', 
    borderRadius: '12px', 
    padding: '20px',
    height: '450px'
  }}></div>
);

function HomePage({ games, bestProps }) {
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const timer = setTimeout(() => {
      setLoading(false);
    }, 1500);
    return () => clearTimeout(timer);
  }, []);

  return (
    <div style={{ 
      padding: '15px', 
      fontFamily: "'Inter', sans-serif",
      fontWeight: '400',
      color: '#D1D5DB',
      backgroundColor: '#111827',
      minHeight: '100vh' 
    }}>
      <h1 style={{ textAlign: 'center' }}>Today's Top MLB Picks and Props</h1>
      {loading ? (
        <>
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
        </>
      ) : (
        <>
          <TopPropsCard bestProps={bestProps} />
          {games.map((gameData, index) => (
            <GameCard 
              key={index}
              game={gameData.gameInfo}
              topProps={gameData.topProps}
              projectedScore={gameData.projectedScore}
              animationDelay={`${index * 100}ms`}
            />
          ))}
        </>
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
  
  const formatPlayerName = (name) => {
    if (!name || !name.includes(',')) return name;
    const parts = name.split(',');
    return `${parts[1].trim()} ${parts[0].trim()}`;
  };

  const dataDir = path.join(process.cwd(), 'data');
  const gamesList = parseCsv(path.join(dataDir, 'weather_adjustments.csv'));
  const scoresList = parseCsv(path.join(dataDir, '_projections', 'final_scores_projected.csv'));
  const batterProps = parseCsv(path.join(dataDir, '_projections', 'batter_props_z_expanded.csv'));
  const pitcherProps = parseCsv(path.join(dataDir, '_projections', 'pitcher_mega_z.csv'));

  const allProps = [];

  const gamesWithData = gamesList.map(game => {
    let projectedScore = null;
    const scoreEntry = scoresList.find(s =>
      s.home_team.trim() === game.home_team.trim() &&
      s.away_team.trim() === game.away_team.trim()
    );

    if (scoreEntry) {
      const homeRaw = parseFloat(scoreEntry.home_score);
      const awayRaw = parseFloat(scoreEntry.away_score);
      let homeRounded = Math.round(homeRaw);
      let awayRounded = Math.round(awayRaw);

      if (homeRounded === awayRounded) {
        if (homeRaw > awayRaw) { homeRounded++; } 
        else if (awayRaw > homeRaw) { awayRounded++; } 
        else { homeRounded++; }
      }
      projectedScore = { home: homeRounded, away: awayRounded, total: (homeRaw + awayRaw).toFixed(2) };
    }

    const gameBatters = batterProps.filter(prop => 
      prop.team?.trim() === game.home_team.trim() || 
      prop.team?.trim() === game.away_team.trim()
    );
    const gamePitchers = pitcherProps.filter(prop => 
      prop.team?.trim() === game.home_team.trim() || 
      prop.team?.trim() === game.away_team.trim()
    );

    const formatProp = (prop, probability, type) => {
        const propTypeClean = prop.prop_type.replace(/_/g, ' ');
        const propTypeCapitalized = propTypeClean.charAt(0).toUpperCase() + propTypeClean.slice(1);
        return {
            name: formatPlayerName(prop.name),
            team: prop.team,
            line: `${propTypeCapitalized} Over ${prop.line}`,
            playerId: prop.player_id,
            probability: probability,
            type: type
        };
    };

    const topBatterProps = gameBatters
      .sort((a, b) => (parseFloat(b.over_probability) || 0) - (parseFloat(a.over_probability) || 0))
      .slice(0, 2)
      .map(prop => formatProp(prop, (parseFloat(prop.over_probability) * 100).toFixed(0), 'batter'));

    const topPitcherProps = gamePitchers
      .sort((a, b) => (parseFloat(b.z_score) || 0) - (parseFloat(a.z_score) || 0))
      .slice(0, 1)
      .map(prop => formatProp(prop, (parseFloat(prop.over_score_probability) * 100).toFixed(0), 'pitcher'));

    const topProps = [...topBatterProps, ...topPitcherProps];
    allProps.push(...topProps);

    return {
      gameInfo: game,
      topProps: topProps,
      projectedScore: projectedScore
    };
  });

  const bestProps = allProps
    .sort((a, b) => {
        const aValue = a.type === 'batter' ? parseFloat(a.probability) : parseFloat(a.z_score);
        const bValue = b.type === 'batter' ? parseFloat(b.probability) : parseFloat(b.z_score);
        return (bValue || 0) - (aValue || 0);
    })
    .slice(0, 5); // Changed from 3 to 5 to give a larger pool to choose from
    
  return {
    props: { 
        games: gamesWithData,
        bestProps: bestProps
    },
    revalidate: 3600
  };
}

export default HomePage;
