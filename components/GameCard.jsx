const teamLogos = {
  "braves": "/logos/braves.png",
  "orioles": "/logos/orioles.png",
  "mets": "/logos/mets.png",
  "redsox": "/logos/redsox.png",
  "cubs": "/logos/cubs.png",
  "reds": "/logos/reds.png",
  "guardians": "/logos/guardians.png",
  "rangers": "/logos/rangers.png",
  "rockies": "/logos/rockies.png",
  "tigers": "/logos/tigers.png",
  "astros": "/logos/astros.png",
  "whitesox": "/logos/whitesox.png",
  "royals": "/logos/royals.png",
  "angels": "/logos/angels.png",
  "dodgers": "/logos/dodgers.png",
  "marlins": "/logos/marlins.png",
  "brewers": "/logos/brewers.png",
  "twins": "/logos/twins.png",
  "yankees": "/logos/yankees.png",
  "athletics": "/logos/athletics.png",
  "phillies": "/logos/phillies.png",
  "diamondbacks": "/logos/diamondbacks.png",
  "pirates": "/logos/pirates.png",
  "padres": "/logos/padres.png",
  "mariners": "/logos/mariners.png",
  "giants": "/logos/giants.png",
  "cardinals": "/logos/cardinals.png",
  "bluejays": "/logos/bluejays.png",
  "rays": "/logos/rays.png",
  "nationals": "/logos/nationals.png"
};

export default function GameCard({ game, temperature, top_props }) {
  const [awayTeam, homeTeam] = game.toLowerCase().split(" @ ");
  const awayLogo = teamLogos[awayTeam];
  const homeLogo = teamLogos[homeTeam];

  return (
    <div style={{
      background: 'rgba(255, 255, 255, 0.03)',
      border: '1px solid rgba(255,255,255,0.08)',
      borderImage: 'linear-gradient(135deg, #3a3a3a, #2e2e2e) 1',
      borderRadius: '16px',
      padding: '20px',
      marginBottom: '20px',
      color: '#ffffff',
      backdropFilter: 'blur(10px)',
      boxShadow: '0 4px 20px rgba(0,0,0,0.4)',
      fontFamily: '"SF Pro Display", -apple-system, BlinkMacSystemFont, system-ui, sans-serif'
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        marginBottom: '10px'
      }}>
        {awayLogo && <img src={awayLogo} alt={awayTeam} style={{ width: 24, height: 24, marginRight: 8 }} />}
        <h2 style={{ fontSize: '16px', margin: 0 }}>{game}</h2>
        {homeLogo && <img src={homeLogo} alt={homeTeam} style={{ width: 24, height: 24, marginLeft: 8 }} />}
      </div>

      <p style={{ fontSize: '13px', color: '#bbb', marginBottom: '12px' }}>🌡 {temperature}°F</p>

      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {top_props.map((prop, idx) => (
          <li key={idx} style={{
            fontSize: '13px',
            padding: '5px 0',
            borderBottom: idx < top_props.length - 1 ? '1px solid #333' : 'none'
          }}>
            <strong>{prop.player}</strong> — {prop.stat.toUpperCase()} <span style={{ color: '#888' }}>(z={prop.z_score})</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
