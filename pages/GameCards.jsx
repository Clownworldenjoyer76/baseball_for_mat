
import React, { useEffect, useState } from 'react';

export default function GameCards() {
  const [cards, setCards] = useState([]);

  useEffect(() => {
    fetch('/game_cards_data.json')
      .then(res => res.json())
      .then(data => setCards(data));
  }, []);

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <h1 className="text-3xl font-bold mb-6">Today's Matchups</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {cards.map((card, i) => (
          <div key={i} className="bg-white p-4 rounded shadow">
            <h2 className="text-xl font-semibold">{card.matchup}</h2>
            <p className="text-sm text-gray-600 mb-2">
              Temp: {card.temperature} | Precip: {card.precipitation}
            </p>
            <h3 className="font-semibold mb-1">Top Props</h3>
            {card.props.map((p, j) => (
              <div
                key={j}
                className="flex justify-between items-center px-2 py-1 my-1 rounded bg-blue-50"
              >
                <span className="text-sm">{p.player} ({p.type})</span>
                <span className="text-xs font-mono text-blue-800">
                  Z: {p.zscore}
                </span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}
