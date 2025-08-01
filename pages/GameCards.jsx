
import React, { useEffect, useState } from 'react';

const STAT_ICONS = {
  "Strikeouts": "🧢 K",
  "ERA": "📉 ERA",
  "xFIP": "📊 xFIP",
  "Expected wOBA": "🎯 xwOBA",
  "Hits": "⚾ H",
  "Home Runs": "💣 HR",
  "Total Bases": "📦 TB"
};

export default function GameCards() {
  const [cards, setCards] = useState([]);

  useEffect(() => {
    fetch('/game_cards_data.json')
      .then(res => res.json())
      .then(setCards);
  }, []);

  return (
    <div className="bg-gray-900 text-white min-h-screen p-4">
      <h1 className="text-3xl font-bold mb-6">Today's Matchups</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {cards.map((card, i) => (
          <div key={i} className="bg-gray-800 text-white p-4 rounded shadow">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <img src={card.away_logo} className="w-6 h-6" alt="away logo" />
                <span className="text-lg font-semibold">{card.away_team}</span>
                <span>@</span>
                <span className="text-lg font-semibold">{card.home_team}</span>
                <img src={card.home_logo} className="w-6 h-6" alt="home logo" />
              </div>
            </div>
            <p className="text-sm mb-1">Game Time: {card.game_time}</p>
            <p className="text-sm mb-3">Temp: {card.temperature}</p>

            <h3 className="font-semibold mb-1">Top Props</h3>
            {card.props.map((p, j) => {
              const [left, stat] = (p.display || "").split(" – ");
              const icon = STAT_ICONS[stat] || "📈";
              const val = p.stat_value ? parseFloat(p.stat_value).toFixed(2) : "";
              return (
                <div key={j} className="text-sm py-2 flex items-center border-b border-gray-700">
                  <img
                    src={p.img_url}
                    alt="headshot"
                    className="w-8 h-8 rounded-full mr-3 bg-white border"
                    onError={(e) => { e.target.style.display = 'none'; }}
                  />
                  <div className="flex justify-between items-center w-full">
                    <span>{left}</span>
                    <span className="ml-2 font-mono">{icon} {val}</span>
                  </div>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}
