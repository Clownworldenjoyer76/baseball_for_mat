import React from 'react';
import './GameCard.css';

function GameCard({ game, topProps, projectedScore, animationDelay }) {
  // ... your helper functions
  const handleImageError = (e, fallbackSrc) => {
    e.target.onerror = null;
    e.target.src = fallbackSrc;
  };

  return (
    <div 
      className="game-card fade-in-card card-interactive" 
      style={{ '--animation-delay': animationDelay }}
    >
      <div className="card-header">
        <div className="team-info">
          <img 
            src={getLogoUrl(game.away_team)} 
            alt={`${game.away_team} logo`} 
            className="team-logo"
            onError={(e) => handleImageError(e, '/images/default_logo.png')}
          />
          {/* ... rest of the component */}
        </div>
      </div>
      {/* ... rest of the component */}
    </div>
  );
}

export default GameCard;
