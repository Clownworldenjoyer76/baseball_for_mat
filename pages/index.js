
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
      backgroundColor: '#111',
      padding: '16px',
      minHeight: '100vh',
      fontFamily: 'system-ui, sans-serif'
    }}>
      <h1 style={{ color: 'white', fontSize: '20px', marginBottom: '20px' }}>🔥 Top Props by Game</h1>
      {cards.map((card, idx) => (
        <GameCard key={idx} {...card} />
      ))}
    </div>
  );
}
