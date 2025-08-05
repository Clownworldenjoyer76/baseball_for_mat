
import { useEffect, useState } from 'react';
import GameCard from '../components/GameCard';

export default function Home() {
  const [cards, setCards] = useState([]);

  useEffect(() => {
    fetch('/api/game-cards')
      .then(res => res.json())
      .then(data => setCards(data));
  }, []);

  return (
    <div style={{
      background: 'linear-gradient(180deg, #0f0f0f, #1a1a1a)',
      padding: '24px',
      minHeight: '100vh',
      fontFamily: 'system-ui, sans-serif'
    }}>
      <h1 style={{ color: 'white', fontSize: '22px', marginBottom: '24px' }}>📊 Most Likely Props – Today’s Games</h1>
      {cards.map((card, idx) => (
        <GameCard key={idx} {...card} />
      ))}
    </div>
  );
}
