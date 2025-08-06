import React from 'react';

function GameCard({ game, topProps, projectedScore }) {
  const getLogoUrl = (teamName) => {
    const imageName = teamName.toLowerCase().replace(/\s+/g, '');
    return `/logos/${imageName}.png`;
  };

  return (
    <div style={{ backgroundColor: '#2C2C2C', padding: '0', margin: '20px 0', borderRadius: '12px', overflow: 'hidden' }}>
      
      <div style={{ backgroundColor: '#F9F9F9', color: '#212121', padding: '20px' }}>
        {/* Team matchup section */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <img src={getLogoUrl(game.away_team)} alt={`${game.away_team} logo`} style={{ height: '40px', width: 'auto' }} />
          
          <div style={{ textAlign: 'center' }}>
            <h2 style={{ margin: 0, fontSize: '1.2em' }}>{game.away_team}</h2>
            <p style={{ margin: '4px 0', fontSize: '0.8em', color: '#888888' }}>at</p>
            <h2 style={{ margin: 0, fontSize: '1.2em' }}>{game.home_team}</h2>
          </div>
          
          <img src={getLogoUrl(game.home_team)} alt={`${game.home_team} logo`} style={{ height: '40px', width: 'auto' }} />
        </div>

        {/* Game info section */}
        <div style={{ display: 'flex', justifyContent: 'space-around', color: '#555', paddingTop: '15px' }}>
          <span>{game.game_time}</span>
          <span>{Math.round(game.temperature)}°</span>
          <span>{game.venue}</span>
        </div>
      </div>

      <div style={{ padding: '20px' }}>
        {/* Top props section */}
        <div style={{ padding: '0 0 15px 0', borderBottom: '1px solid #444' }}>
          <h4 style={{ margin: '0 0 10px 0', textAlign: 'center', color: '#3B82F6' }}>Top Props</h4>
          <ul style={{ listStyle: 'none', padding: 0, margin: 0, textAlign: 'center', color: '#F5F5F5' }}>
            {topProps.map((prop, index) => (
              <li key={index} style={{ padding: '5px 0' }}>
                {prop.name} ({prop.type})
              </li>
            ))}
          </ul>
        </div>

        {/* Projected score section */}
        <div style={{ padding: '15px 0 0', textAlign: 'center' }}>
          <h4 style={{ margin: '0 0 10px 0', textAlign: 'center', color: '#3B82F6' }}>Projected Score</h4>
          <p style={{ margin: 0, fontSize: '1.2em', color: '#F5F5F5' }}>
            {projectedScore ? `${game.away_team} ${projectedScore.away} - ${projectedScore.home} ${game.home_team}` : 'N/A'}
          </p>
        </div>
      </div>
    </div>
  );
}

export default GameCard;
