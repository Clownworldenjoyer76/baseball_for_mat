import React from 'react';
import Image from 'next/image';

function GameCard({ game, topProps, projectedScore }) {
  // Converts "Team Name" to "/logos/teamname.png"
  const getLogoUrl = (teamName) => {
    const imageName = teamName.toLowerCase().replace(/\s+/g, '');
    return `/logos/${imageName}.png`;
  };

  return (
    <div style={{ border: '1px solid #e0e0e0', padding: '16px', margin: '16px 0', borderRadius: '8px' }}>
      {/* Team matchup section */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '15px' }}>
        <Image src={getLogoUrl(game.away_team)} alt={`${game.away_team} logo`} width={40} height={40} unoptimized />
        <h2 style={{ margin: 0, fontSize: '1.2em', textAlign: 'center' }}>{game.away_team} vs {game.home_team}</h2>
        <Image src={getLogoUrl(game.home_team)} alt={`${game.home_team} logo`} width={40} height={40} unoptimized />
      </div>

      {/* Game info section */}
      <div style={{ display: 'flex', justifyContent: 'space-around', color: '#555', padding: '15px 0', borderBottom: '1px solid #eee' }}>
        <span>{game.game_time}</span>
        <span>{Math.round(game.temperature)}°</span>
        <span>{game.venue}</span>
      </div>

      {/* Top props section */}
      <div style={{ padding: '15px 0', borderBottom: '1px solid #eee' }}>
        <h4 style={{ margin: '0 0 10px 0', textAlign: 'center' }}>Top Props</h4>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, textAlign: 'center' }}>
          {topProps.map((prop, index) => (
            <li key={index} style={{ padding: '5px 0' }}>
              {prop.name} ({prop.type})
            </li>
          ))}
        </ul>
      </div>

      {/* Projected score section */}
      <div style={{ padding: '15px 0 0', textAlign: 'center' }}>
        <h4 style={{ margin: '0 0 10px 0' }}>Projected Score</h4>
        <p style={{ margin: 0, fontSize: '1.2em' }}>
          {projectedScore ? `${game.away_team} ${projectedScore.away} - ${projectedScore.home} ${game.home_team}` : 'N/A'}
        </p>
      </div>
    </div>
  );
}

export default GameCard;
