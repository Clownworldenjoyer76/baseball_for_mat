import { useEffect, useState } from 'react';
import GameCard from '../components/GameCard';

export default function Home() {
  const [cards, setCards] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch('/api/game-cards')
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch');
        return res.json();
      })
      .then(data => {
        if (Array.isArray(data)) setCards(data);
        else throw new Error('Invalid data format');
      })
      .catch(err => setError(err.message));
  }, []);

  return (
    <div style={{
      background: 'linear-gradient(180deg, #0f0f0f, #1a1a1a)',
      padding: '24px',
      minHeight: '100vh',
      fontFamily: '"SF Pro Display", -apple-system, BlinkMacSystemFont, system-ui, sans-serif'
    }}>
      <h1 style={{ color: 'white', fontSize: '20px', marginBottom: '24px' }}>
        📊 Most Likely Props – Today’s Games
      </h1>

      {error && (
        <p style={{ color: 'red', fontSize: '14px' }}>⚠️ {error}</p>
      )}

      {cards.length === 0 && !error && (
        <p style={{ color: '#888', fontSize: '14px' }}>No games available.</p>
      )}

      {cards.map((card, idx) => {
        const isValid =
          card &&
          typeof card.game === 'string' &&
          card.game.includes(' @ ') &&
          Array.isArray(card.top_props);

        return isValid ? (
          <GameCard
            key={idx}
            game={card.game}
            temperature={card.temperature}
            top_props={card.top_props}
          />
        ) : null;
      })}
    </div>
  );
}
