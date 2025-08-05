
export default function GameCard({ game, temperature, top_props }) {
  const statIcons = {
    hr: '🔥',
    hit: '🎯',
    era: '🧊',
    whip: '🌬️'
  };

  return (
    <div style={{
      background: 'linear-gradient(135deg, #1c1c1e, #2c2c2e)',
      borderRadius: '16px',
      padding: '18px',
      marginBottom: '20px',
      color: '#f9f9f9',
      boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
      fontFamily: 'system-ui, sans-serif'
    }}>
      <h2 style={{ fontSize: '18px', marginBottom: '4px' }}>{game}</h2>
      <p style={{ fontSize: '14px', color: '#aaa', marginBottom: '12px' }}>🌡 {temperature}°F</p>
      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {top_props.map((prop, idx) => {
          const icon = statIcons[prop.stat] || '📊';
          return (
            <li key={idx} style={{
              fontSize: '14px',
              padding: '6px 0',
              borderBottom: idx < top_props.length - 1 ? '1px solid #333' : 'none'
            }}>
              {icon} <strong>{prop.player}</strong> — {prop.stat.toUpperCase()} <span style={{ color: '#aaa' }}>(z={prop.z_score})</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
