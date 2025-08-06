// components/GameCard.jsx

const teamLogos = {
  braves: "/logos/braves.png",
  orioles: "/logos/orioles.png",
  mets: "/logos/mets.png",
  redsox: "/logos/redsox.png",
  cubs: "/logos/cubs.png",
  reds: "/logos/reds.png",
  guardians: "/logos/guardians.png",
  rangers: "/logos/rangers.png",
  rockies: "/logos/rockies.png",
  tigers: "/logos/tigers.png",
  astros: "/logos/astros.png",
  whitesox: "/logos/whitesox.png",
  royals: "/logos/royals.png",
  angels: "/logos/angels.png",
  dodgers: "/logos/dodgers.png",
  marlins: "/logos/marlins.png",
  brewers: "/logos/brewers.png",
  twins: "/logos/twins.png",
  yankees: "/logos/yankees.png",
  athletics: "/logos/athletics.png",
  phillies: "/logos/phillies.png",
  diamondbacks: "/logos/diamondbacks.png",
  pirates: "/logos/pirates.png",
  padres: "/logos/padres.png",
  mariners: "/logos/mariners.png",
  giants: "/logos/giants.png",
  cardinals: "/logos/cardinals.png",
  bluejays: "/logos/bluejays.png",
  rays: "/logos/rays.png",
  nationals: "/logos/nationals.png",
};

function zScoreToProbability(z) {
  const erf = x => {
    const sign = x >= 0 ? 1 : -1;
    x = Math.abs(x);
    const t = 1 / (1 + 0.3275911 * x);
    const y = 1 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x));
    return sign * y;
  };
  return Math.round((0.5 * (1 + erf(z / Math.sqrt(2))) * 100));
}

export default function GameCard({ game, temperature, game_time, top_props }) {
  const [awayTeam, homeTeam] = game.toLowerCase().split(" @ ");
  const awayLogo = teamLogos[awayTeam];
  const homeLogo = teamLogos[homeTeam];

  return (
    <div style={{
      background: 'rgba(255, 255, 255, 0.03)',
      border: '1px solid rgba(255,255,255,0.08)',
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
        marginBottom: '10px',
        justifyContent: 'space-between'
      }}>
        <div style={{ display: 'flex', alignItems: 'center' }}>
          {awayLogo && <img src={awayLogo} alt={awayTeam} style={{ width: 20, height: 20, marginRight: 8 }} />}
          <h2 style={{ fontSize: '15px', margin: 0 }}>{game}</h2>
          {homeLogo && <img src={homeLogo} alt={homeTeam} style={{ width: 20, height: 20, marginLeft: 8 }} />}
        </div>
        <span style={{ fontSize: '13px', color: '#ccc' }}>
          {temperature}°F · {game_time}
        </span>
      </div>

      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {top_props.map((prop, idx) => {
          const prob = zScoreToProbability(prop.z_score);
          const label = `${prop.stat.toUpperCase()}${prop.player ? ` — ${prop.player}` : ''}`;
          return (
            <li key={idx} style={{ marginBottom: '6px' }}>
              <div style={{ fontSize: '13px', marginBottom: '4px' }}>{label}</div>
              <div style={{
                height: '8px',
                background: '#333',
                borderRadius: '4px',
                overflow: 'hidden',
                position: 'relative'
              }}>
                <div style={{
                  width: `${prob}%`,
                  background: 'linear-gradient(90deg, red, orange, limegreen)',
                  height: '100%'
                }}></div>
              </div>
              <div style={{ fontSize: '12px', color: '#aaa', marginTop: '2px' }}>{prob}%</div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
