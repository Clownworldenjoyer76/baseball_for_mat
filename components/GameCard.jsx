
export default function GameCard({ game, temperature, top_props }) {
  const statIcons = {
    hr: '🔥',
    hit: '🎯',
    era: '🧊',
    whip: '🌬️'
  };

  return (
    <div style={{
      background: 'rgba(255, 255, 255, 0.05)',
      border: '1px solid rgba(255,255,255,0.1)',
      borderRadius: '16px',
      padding: '20px',
      marginBottom: '20px',
      color: '#ffffff',
      backdropFilter: 'blur(12px)',
      boxShadow: '0 4px 30px rgba(0,0,0,0.2)',
      fontFamily: 'system-ui, sans-serif'
    }}>
      <h2 style={{ fontSize: '20px', marginBottom: '6px' }}>{game}</h2>
      <p style={{ fontSize: '14px', marginBottom: '12px', color: '#ccc' }}>🌡 {temperature}°F</p>
      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {top_props.map((prop, idx) => {
          const icon = statIcons[prop.stat] || '📊';
          return (
            <li key={idx} style={{
              fontSize: '14px',
              padding: '6px 0',
              borderBottom: idx < top_props.length - 1 ? '1px solid #333' : 'none'
            }}>
              {icon} <strong>{prop.player}</strong> — <span style={{ color: '#bbb' }}>{prop.stat.toUpperCase()}</span> <span style={{ color: '#777' }}>(z={prop.z_score})</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
