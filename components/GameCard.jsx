// components/GameCard.jsx
import React from 'react';

const getTeamLogo = (team) => {
  const fileName = team.toLowerCase().replace(/ /g, '-') + '.png';
  return `/logos/${fileName}`;
};

const zScoreToPercentage = (z) => {
  const p = 100 / (1 + Math.exp(-z));
  return Math.min(99, Math.max(1, Math.round(p)));
};

const GameCard = ({ game }) => {
  const { matchup, temperature, topProps } = game;

  return (
    <div className="card">
      <div className="header">
        <div className="teams">
          <img src={getTeamLogo(game.away_team)} alt={game.away_team} className="logo" />
          <span>{game.away_team} @ {game.home_team}</span>
          <img src={getTeamLogo(game.home_team)} alt={game.home_team} className="logo" />
        </div>
        <div className="temp">{Math.round(temperature)}°F</div>
      </div>
      <div className="props">
        {topProps.map((prop, idx) => (
          <div className="prop" key={idx}>
            <div className="label">{prop.player} – {prop.stat}</div>
            <div className="bar-container">
              <div className="bar" style={{ width: `${zScoreToPercentage(prop.z)}%` }} />
            </div>
            <div className="value">{zScoreToPercentage(prop.z)}%</div>
          </div>
        ))}
      </div>
      <style jsx>{`
        .card {
          background: #1e1e1e;
          border-radius: 12px;
          padding: 16px;
          margin: 12px 0;
          box-shadow: 0 0 8px rgba(0,0,0,0.4);
        }
        .header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 12px;
        }
        .teams {
          display: flex;
          align-items: center;
          gap: 8px;
          font-weight: 600;
        }
        .logo {
          width: 24px;
          height: 24px;
        }
        .temp {
          font-weight: bold;
        }
        .props {
          display: flex;
          flex-direction: column;
          gap: 10px;
        }
        .prop {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
        }
        .label {
          flex: 1;
          font-size: 14px;
        }
        .bar-container {
          flex: 2;
          background: #333;
          height: 8px;
          border-radius: 4px;
          overflow: hidden;
        }
        .bar {
          height: 100%;
          background: linear-gradient(to right, #00ffa3, #007bff);
        }
        .value {
          width: 36px;
          text-align: right;
          font-size: 13px;
          font-weight: 600;
        }
      `}</style>
    </div>
  );
};

export default GameCard;
