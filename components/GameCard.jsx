export default function GameCard({ game, temperature, top_props }) {
  return (
    <div style={{
      backgroundColor: '#2a2a2a',
      borderRadius: '12px',
      padding: '16px',
      marginBottom: '16px',
      color: 'white'
    }}>
      <h2>{game}</h2>
      <p>Temp: {temperature}°F</p>
      <ul>
        {top_props.map((prop, idx) => (
          <li key={idx}>
            {prop.player} – {prop.stat.toUpperCase()} (z={prop.z_score})
          </li>
        ))}
      </ul>
    </div>
  );
}