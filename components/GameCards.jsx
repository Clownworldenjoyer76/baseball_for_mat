// components/GameCards.jsx
export default function GameCards({ cards }) {
  if (!cards?.length) return <p className="text-red-500">⚠️ No props loaded.</p>;

  return (
    <div className="game-cards">
      {cards.map((game) => (
        <div key={game.matchup} className="card">
          <h2>{game.matchup}</h2>
          <p>{game.time} • {game.temp}°F</p>
          <ul>
            {game.props.map((prop) => (
              <li key={prop.name + prop.stat}>
                <img
                  src={prop.headshot}
                  alt={prop.name}
                  className="headshot"
                  onError={(e) => {
                    e.currentTarget.onerror = null;
                    e.currentTarget.src = '/fallback_headshot.png';
                  }}
                />
                {prop.name} – {prop.stat}: {prop.value}
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}
