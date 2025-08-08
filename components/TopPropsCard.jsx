import React from 'react';

function TopPropsCard({ bestProps }) {
  const getHeadshotUrl = (playerId) => {
    if (!playerId) return '/images/default_player.png';
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  if (!bestProps || bestProps.length === 0) {
    return null;
  }

  // Logic to only show each player once
  const uniquePlayers = [];
  const playerIds = new Set();
  bestProps.forEach(prop => {
    if (!playerIds.has(prop.playerId)) {
      playerIds.add(prop.playerId);
      uniquePlayers.push(prop);
    }
  });

  return (
    <div
      className="fade-in-card card-interactive"
      style={{
        backgroundColor: '#1C1C1E',
        margin: '20px 0',
        borderRadius: '12px',
        overflow: 'hidden',
        border: '1px solid #2F2F30'
      }}
    >
      <div style={{ padding: '20px' }}>
        <h4 style={{ margin: '0 0 15px 0', textAlign: 'center', color: '#D4AF37' }}>Today’s Best Props</h4>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#E0E0E0' }}>
          {uniquePlayers.slice(0, 3).map((prop, index) => (
            <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
              <img
                alt={prop.name}
                src={getHeadshotUrl(prop.playerId)}
                onError={(e) => { e.target.onerror = null; e.target.src = '/images/default_player.png'; }}
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
                <div style={{ fontSize: '1em', display: 'flex', alignItems: 'center' }}>
                  {prop.name} <span role="img" aria-label="fire" style={{ marginLeft: '8px', fontSize: '1.2em' }}>🔥</span>
                </div>
                <div style={{ fontSize: '0.8em', color: '#B0B0B0', marginTop: '4px' }}>
                  {prop.team}
                </div>
                <div style={{ fontSize: '0.9em', color: '#E0E0E0', marginTop: '4px' }}>
                  {prop.line}
                </div>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default TopPropsCard;
