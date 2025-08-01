
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
    <div style={{ backgroundColor: '#000', color: '#fff', minHeight: '100vh', padding: '1rem' }}>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 'bold', marginBottom: '1.5rem' }}>Today's Matchups</h1>

      <div style={{ display: 'grid', gap: '1.5rem' }}>
        {cards.map((card, i) => (
          <div key={i} style={{
            backgroundColor: '#1f2937', // Tailwind gray-800
            color: '#fff',
            borderRadius: '0.5rem',
            padding: '1rem',
            boxShadow: '0 0 10px rgba(0,0,0,0.3)'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
              <img src={card.away_logo} style={{ maxHeight: '24px', width: 'auto' }} alt="away logo" />
              <span style={{ fontWeight: '600' }}>{card.away_team}</span>
              <span style={{ fontWeight: '600' }}>@</span>
              <span style={{ fontWeight: '600' }}>{card.home_team}</span>
              <img src={card.home_logo} style={{ maxHeight: '24px', width: 'auto' }} alt="home logo" />
            </div>
            <p style={{ fontSize: '0.875rem', marginBottom: '0.25rem' }}>Game Time: {card.game_time}</p>
            <p style={{ fontSize: '0.875rem', marginBottom: '0.75rem' }}>Temp: {card.temperature}</p>

            <h3 style={{ fontWeight: '600', marginBottom: '0.5rem' }}>Top Props</h3>
            {card.props.map((p, j) => {
              const [left, stat] = (p.display || "").split(" – ");
              const icon = STAT_ICONS[stat] || "📈";
              const val = p.stat_value ? parseFloat(p.stat_value).toFixed(2) : "";
              return (
                <div key={j} style={{
                  fontSize: '0.875rem',
                  padding: '0.5rem 0',
                  display: 'flex',
                  alignItems: 'center',
                  borderBottom: '1px solid #374151' // Tailwind gray-700
                }}>
                  <img
                    src={p.img_url}
                    alt="headshot"
                    style={{ height: '32px', width: '32px', borderRadius: '9999px', marginRight: '0.75rem', backgroundColor: '#fff' }}
                    onError={(e) => { e.target.style.display = 'none'; }}
                  />
                  <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%' }}>
                    <span>{left}</span>
                    <span style={{ fontFamily: 'monospace' }}>{icon} {val}</span>
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
