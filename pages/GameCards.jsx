import { useEffect, useState } from 'react';

export default function GameCards() {
  const [data, setData] = useState(null);

  useEffect(() => {
    fetch('/api/game-cards')
      .then((res) => res.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="loading-spinner">Loading...</div>;

  return (
    <div className="game-cards">
      {data.map((game) => (
        <div key={game.matchup} className="card">
          <h2>{game.matchup}</h2>
          <p>{game.time} • {game.temp}°F</p>
          <ul>
            {game.props.map((prop) => (
              <li key={prop.name + prop.stat}>
                <img src={prop.headshot} alt={prop.name} className="headshot" />
                {prop.name} – {prop.stat}: {prop.value}
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}
