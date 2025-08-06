import React from 'react';

function GameCard({ game, topProps, projectedScore, animationDelay }) {
  const getLogoUrl = (teamName) => {
    const imageName = teamName.toLowerCase().replace(/\s+/g, '');
    return `/logos/${imageName}.png`;
  };

  const getHeadshotUrl = (playerId) => {
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  return (
    <div 
      className="fade-in-card card-interactive" 
      style={{ 
        backgroundColor: '#1F2937',
        margin: '20px 0', 
        borderRadius: '12px', 
        overflow: 'hidden',
        animationDelay: animationDelay
      }}
    >
      <div style={{ padding: '20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <img src={getLogoUrl(game.away_team)} alt={`${game.away_team} logo`} style={{ height: '40px', width: 'auto' }} />
          <div style={{ textAlign: 'center' }}>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#D1D5DB' }}>{game.away_team}</h2>
            <p style={{ margin: '4px 0', fontSize: '0.8em', color: '#9CA3AF' }}>at</p>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#D1D5DB' }}>{game.home_team}</h2>
          </div>
          <img src={getLogoUrl(game.home_team)} alt={`${game.home_team} logo`} style={{ height: '40px', width: 'auto' }} />
        </div>

        <div className="text-xs text-gray-400 flex items-center gap-4" style={{ justifyContent: 'center', paddingTop: '15px' }}>
          <span>🕒 {game.game_time}</span>
          <span>🌡️ {Math.round(game.temperature)}°</span>
          <span>📍 {game.venue}</span>
        </div>
      </div>

      <div style={{ padding: '20px', borderTop: '1px solid #374151' }}>
        <div style={{ padding: '0 0 15px 0', borderBottom: '1px solid #374151' }}>
          <h4 style={{ margin: '0 0 15px 0', textAlign: 'center', color: '#F59E0B' }}>Top Props</h4>
          <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#D1D5DB' }}>
            {topProps.map((prop, index) => (
              <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
                <img src={getHeadshotUrl(prop.playerId)} alt={prop.name} style={{ height: '50px', width: '50px', borderRadius: '50%', marginRight: '15px', backgroundColor: '#374151', objectFit: 'cover' }} />
                <div>
                  <div style={{ fontSize: '1em' }}>{prop.name}</div>
                  <div style={{ fontSize: '0.9em', color: '#9CA3AF', marginLeft: '10px' }}>{prop.line}</div>
                </div>
              </li>
            ))}
          </ul>
        </div>

        <div style={{ padding: '15px 0 0', textAlign: 'center' }}>
          <h4 style={{ margin: '0 0 10px 0', textAlign: 'center', color: '#F59E0B' }}>Projected Score</h4>
          <div style={{ fontSize: '1.2em', color: '#D1D5DB', lineHeight: '1.5' }}>
            {projectedScore ? (
              <>
                <div>{game.away_team} {projectedScore.away}</div>
                <div>{game.home_team} {projectedScore.home}</div>
                <div style={{ fontSize: '0.9em', color: '#9CA3AF', marginTop: '10px' }}>
                  Total Run Projection: {projectedScore.total}
                </div>
              </>
            ) : <p>N/A</p>}
          </div>
        </div>
      </div>
    </div>
  );
}

export default GameCard;
