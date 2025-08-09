import React, { useEffect, useState } from 'react';

function GameCard({ game, topProps, projectedScore, animationDelay }) {
  const [autoProps, setAutoProps] = useState([]);

  const getLogoUrl = (teamName) => {
    if (!teamName) return '/images/default_logo.png';
    const imageName = teamName.toLowerCase().replace(/\s+/g, '');
    return `/logos/${imageName}.png`;
  };
  const getHeadshotUrl = (playerId) => {
    if (!playerId) return '/images/default_player.png';
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  // fetch top props for this game if not provided
  useEffect(() => {
    let cancel = false;
    if (!game || (Array.isArray(topProps) && topProps.length)) return;
    (async () => {
      try {
        const params = new URLSearchParams({ home: game.home_team, away: game.away_team });
        const r = await fetch(`/api/game-top-props?${params.toString()}`, { cache: 'no-store' });
        const data = r.ok ? await r.json() : [];
        if (!cancel) setAutoProps(Array.isArray(data) ? data : []);
      } catch {
        if (!cancel) setAutoProps([]);
      }
    })();
    return () => { cancel = true; };
  }, [game, topProps]);

  if (!game) return null;

  const propsToShow = (Array.isArray(topProps) && topProps.length) ? topProps : autoProps;

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
          <img 
            src={getLogoUrl(game.away_team)} 
            alt={`${game.away_team} logo`} 
            style={{ height: '40px', width: 'auto' }}
            onError={(e) => { e.target.onerror = null; e.target.src='/images/default_logo.png'; }}
          />
          <div style={{ textAlign: 'center' }}>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#E0E0E0' }}>{game.away_team}</h2>
            <p style={{ margin: '4px 0', fontSize: '0.8em', color: '#B0B0B0' }}>at</p>
            <h2 style={{ margin: 0, fontSize: '1.2em', color: '#E0E0E0' }}>{game.home_team}</h2>
          </div>
          <img 
            src={getLogoUrl(game.home_team)} 
            alt={`${game.home_team} logo`} 
            style={{ height: '40px', width: 'auto' }}
            onError={(e) => { e.target.onerror = null; e.target.src='/images/default_logo.png'; }}
          />
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
          🕰️  {game.game_time}&nbsp;&nbsp;&nbsp;🌡️  {game.temperature ? `${Math.round(game.temperature)}°` : 'N/A'}&nbsp;&nbsp;&nbsp;📍 {game.venue}
        </div>
      </div>

      <div style={{ padding: '20px', borderTop: '1px solid #2F2F30' }}>
        {/* Top Props */}
        {Array.isArray(propsToShow) && propsToShow.length > 0 && (
          <div style={{ padding: '0 0 15px 0', borderBottom: '1px solid #2F2F30' }}>
            <h4 style={{ margin: '0 0 15px 0', textAlign: 'center', color: '#D4AF37' }}>Top Props</h4>
            <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#E0E0E0' }}>
              {propsToShow.map((prop, index) => (
                <li key={prop?.playerId || prop?.name || index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
                  <img
                    alt={prop?.name}
                    src={getHeadshotUrl(prop?.playerId)}
                    onError={(e) => { e.target.onerror = null; e.target.src = '/images/default_player.png'; }}
                    style={{ height: '50px', width: '50px', borderRadius: '50%', marginRight: '15px', backgroundColor: '#2F2F30', objectFit: 'cover' }}
                  />
                  <div>
                    <div style={{ fontSize: '1em' }}>{prop?.name}</div>
                    <div style={{ fontSize: '0.9em', color: '#B0B0B0' }}>{prop?.line}</div>
                  </div>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Projected Score (unchanged) */}
        {projectedScore && typeof projectedScore === 'object' && (
          <div style={{ padding: '15px 0 0', textAlign: 'center' }}>
            <h4 style={{ margin: '0 0 10px 0', textAlign: 'center', color: '#D4AF37' }}>Projected Score</h4>
            <div style={{ fontSize: '1.2em', color: '#E0E0E0', lineHeight: '1.5' }}>
              <div>{game.away_team} {projectedScore.away}</div>
              <div>{game.home_team} {projectedScore.home}</div>
              <div style={{ fontSize: '0.9em', color: '#B0B0B0', marginTop: '10px' }}>
                Real Run Total Projection: {projectedScore.total}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default GameCard;
