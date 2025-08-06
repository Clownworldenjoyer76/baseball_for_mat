// pages/index.js
import { useEffect, useState } from 'react';
import GameCard from '../components/GameCard';

export default function Home() {
  const [games, setGames] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/game-cards')
      .then(res => res.json())
      .then(data => {
        setGames(data);
        setLoading(false);
      });
  }, []);

  return (
    <div className="container">
      <h1>📊 Most Likely Props – Today’s Games</h1>
      {loading ? (
        <p>Loading...</p>
      ) : (
        games.map((game, i) => <GameCard key={i} game={game} />)
      )}
      <style jsx>{`
        .container {
          padding: 20px;
          color: white;
          font-family: 'SF Pro Display', sans-serif;
          background: #121212;
          min-height: 100vh;
        }
        h1 {
          font-size: 20px;
          margin-bottom: 20px;
        }
      `}</style>
    </div>
  );
}