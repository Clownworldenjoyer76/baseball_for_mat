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
        backgroundColor: '#1C1C1E',
        margin: '20px 0', 
        borderRadius: '12px', 
        overflow: 'hidden',
        animationDelay: animationDelay,
        border: '1px solid #2F2F30'
      }}
    >
      <div style={{ padding: '20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <img src={getLogoUrl(game.away_team)} alt={`${game.away_team} logo`} style={{ height: '40px', width: 'auto' }} />
          <div style={{ textAlign: 'center' }}>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#E0E0E0' }}>{game.away_team}</h2>
            <p style={{ margin: '4px 0', fontSize: '0.8em', color: '#B0B0B0' }}>at</p>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#E0E0E0' }}>{game.home_team}</h2>
          </div>
          <img src={getLogoUrl(game.home_team)} alt={`${game.home_team} logo`} style={{ height: '40px', width: 'auto' }} />
        </div>

        <div
          style={{
            fontSize: '12px',
            lineHeight: '1',
            color: '#B0B0B0',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            textAlign: 'center',
            paddingTop: '12px'
          }}
        >
          🕒 {game.game_time}&nbsp;&nbsp;&nbsp;🌡️ {Math.round(game.temperature)}°&nbsp;&nbsp;&nbsp;📍 {game.venue}
        </div>
      </div>

      <div style={{ padding: '20px', borderTop: '1px solid #2F2F30' }}>
        <div style={{ padding: '0 0 15px 0', borderBottom: '1px solid #2F2F30' }}>
          <h4 style={{ margin: '0 0 15px 0', textAlign: 'center', color: '#D4AF37' }}>Top Props</h4>
          <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#E0E0E0' }}>
            {topProps.map((prop, index) => (
              <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
                <img
                  src={getHeadshotUrl(prop.playerId)}
                  alt={prop.name}
                  style={{
                    height: '50px',
                    width: '50px',
                    borderRadius: '50%',
                    marginRight: '15px',
                    backgroundColor: '#2F2F30',
                    objectFit: 'cover'
                  }}
                />
                <div>
                  <div style={{ fontSize: '1em' }}>{prop.name}</div>
                  <div style={{ fontSize: '0.9em', color: '#B0B0B0', marginLeft: '10px' }}>{prop.line}</div>
                </div>
              </li>
            ))}
          </ul>
        </div>

        <div style={{ padding: '15px 0 0', textAlign: 'center' }}>
          <h4 style={{ margin: '0 0 10px 0', textAlign: 'center', color: '#D4AF37' }}>Projected Score</h4>
          <div style={{ fontSize: '1.2em', color: '#E0E0E0', lineHeight: '1.5' }}>
            {projectedScore ? (
              <>
                <div>{game.away_team} {projectedScore.away}</div>
                <div>{game.home_team} {projectedScore.home}</div>
                <div style={{ fontSize: '0.9em', color: '#B0B0B0', marginTop: '10px' }}>
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
