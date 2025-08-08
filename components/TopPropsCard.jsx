import React from 'react';

// This component displays the top props of the day.
function TopPropsCard({ bestProps }) {
  const getHeadshotUrl = (playerId) => {
    if (!playerId) return '/images/default_player.png';
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  // Add a check to ensure there are props to display
  if (!bestProps || bestProps.length === 0) {
    return null;
  }

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
          {bestProps.map((prop, index) => (
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
                <div style={{ fontSize: '1em' }}>{prop.name}</div>
                <div style={{ fontSize: '0.9em', color: '#B0B0B0' }}>{prop.line} - {prop.probability}%</div>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default TopPropsCard;
