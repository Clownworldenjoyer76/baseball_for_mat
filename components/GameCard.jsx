// components/GameCard.jsx
import React from 'react';

function zToPercent(z) {
  const cdf = 0.5 * (1 + Math.tanh(z / Math.sqrt(2)));
  return Math.round(cdf * 100);
}

export default function GameCard({ game }) {
  return (
    <div className="game-card">
      <div className="header">
        <div className="matchup">{game.game}</div>
        <div className="temperature">{game.temperature}&deg;F</div>
      </div>
      <div className="props">
        {game.top_props.map((prop, index) => (
          <div className="prop-bar" key={index}>
            <span className="label">{prop.stat.toUpperCase()}</span>
            <div className="bar-wrapper">
              <div
                className="bar-fill"
                style={{ width: `${zToPercent(prop.z_score)}%` }}
              />
            </div>
            <span className="value">{zToPercent(prop.z_score)}%</span>
          </div>
        ))}
      </div>
      <style jsx>{`
        .game-card {
          background: #1e1e1e;
          border-radius: 12px;
          padding: 16px;
          margin-bottom: 16px;
          box-shadow: 0 0 8px rgba(0, 0, 0, 0.3);
        }
        .header {
          display: flex;
          justify-content: space-between;
          font-weight: 600;
          margin-bottom: 12px;
        }
        .props {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        .prop-bar {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .label {
          width: 30px;
        }
        .bar-wrapper {
          flex-grow: 1;
          height: 8px;
          background: #444;
          border-radius: 4px;
          overflow: hidden;
        }
        .bar-fill {
          height: 100%;
          background: linear-gradient(to right, #00ff7f, #0077ff);
        }
        .value {
          width: 40px;
          text-align: right;
        }
      `}</style>
    </div>
  );
}