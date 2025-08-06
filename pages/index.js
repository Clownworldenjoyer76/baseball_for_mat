// pages/index.js

import { useEffect, useState } from 'react';
import GameCard from '../components/GameCard';

export default function Home() {
  const [games, setGames] = useState([]);

  useEffect(() => {
    fetch('/api/game-cards')
      .then(res => res.json())
      .then(data => setGames(data))
      .catch(err => console.error('Failed to load game cards:', err));
  }, []);

  return (
    <div style={{ padding: '20px' }}>
      {games.map((gameData, index) => (
        <GameCard
          key={index}
          game={gameData.game}
          temperature={gameData.temperature}
          game_time={gameData.game_time}
          top_props={gameData.top_props}
        />
      ))}
    </div>
  );
}
