import React, { useState, useEffect } from 'react';
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import GameCard from '../components/GameCard';

const SkeletonCard = () => (
  <div style={{
    backgroundColor: '#1F2937',
    margin: '20px 0',
    borderRadius: '12px',
    padding: '20px',
    height: '350px'
  }}></div>
);

function HomePage({ games }) {
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
        games.map((gameData, index) => (
          <GameCard
            key={index}
            game={gameData.gameInfo}
            topProps={gameData.topProps}
            projectedScore={gameData.projectedScore}
            animationDelay={`${index * 100}ms`}
          />
        ))
      )}
    </div>
  );
}

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

  const formatLabel = (type) => {
    if (type === 'total_bases') return 'Total Bases';
    if (type === 'hits') return 'Hits';
    if (type === 'home_runs') return 'Home Runs';
    return type;
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
      projectedScore = {
        home: roundedHome,
        away: roundedAway,
        total: (rawHome + rawAway).toFixed(2)
      };
    }

    const normalized = (str) => str?.toLowerCase().replace(/\s+/g, '').trim();

    const gameBatters = batterProps.filter(b =>
      b.team &&
      [normalized(game.home_team), normalized(game.away_team)].includes(normalized(b.team)) &&
      b.prop_type &&
      b.z_score && b.line && b.name && b.player_id &&
      parseFloat(b.line) >= 0.5
    );

    const topProps = gameBatters
      .sort((a, b) => parseFloat(b.z_score) - parseFloat(a.z_score))
      .slice(0, 5)
      .map(prop => ({
        name: formatPlayerName(prop.name),
        line: `Over ${prop.line} ${formatLabel(prop.prop_type)}`,
        playerId: prop.player_id
      }));

    return {
      gameInfo: game,
      topProps,
      projectedScore
    };
  });

  return { props: { games: gamesWithData }, revalidate: 3600 };
}

export default HomePage;
