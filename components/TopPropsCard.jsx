import React, { useEffect, useState } from 'react';

export default function TopPropsCard() {
  const [rows, setRows] = useState([]);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    let cancel = false;
    (async () => {
      try {
        const res = await fetch('/api/best-props', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (!cancel) setRows(Array.isArray(data) ? data : []);
      } catch {
        if (!cancel) setRows([]);
      } finally {
        if (!cancel) setReady(true);
      }
    })();
    return () => { cancel = true; };
  }, []);

  const getHeadshotUrl = (playerId) => {
    if (!playerId) return '/images/default_player.png';
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  if (!ready || rows.length === 0) return null;

  return (
    <div
      className="fade-in-card card-interactive"
      style={{
        backgroundColor: '#1C1C1E',
        margin: '20px 0',
        borderRadius: '12px',
        overflow: 'hidden',
        border: '1px solid #2F2F30'
      }}
    >
      <div style={{ padding: '20px' }}>
        <h4 style={{ margin: '0 0 15px 0', textAlign: 'center', color: '#D4AF37' }}>Today’s Best Props</h4>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, color: '#E0E0E0' }}>
          {rows.map((prop, index) => (
            <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
              <img
                alt={prop.name}
                src={getHeadshotUrl(prop.playerId)}
                onError={(e) => { e.currentTarget.onerror = null; e.currentTarget.src = '/images/default_player.png'; }}
                style={{
                  height: '50px',
                  width: '50px',
                  borderRadius: '50%',
                  marginRight: '15px',
                  backgroundColor: '#2F2F30',
                  objectFit: 'cover'
                }}
              />
              <div>
                <div style={{ fontSize: '1em', display: 'flex', alignItems: 'center' }}>
                  {prop.name} <span role="img" aria-label="fire" style={{ marginLeft: '8px', fontSize: '1.2em' }}>🔥</span>
                </div>
                <div style={{ fontSize: '0.8em', color: '#B0B0B0', marginTop: '4px' }}>
                  {prop.team}
                </div>
                <div style={{ fontSize: '0.9em', color: '#E0E0E0', marginTop: '4px' }}>
                  {prop.line}
                </div>
                {prop.edge ? (
                  <div style={{ fontSize: '0.8em', color: '#9ad27f', marginTop: '4px' }}>
                    Edge: {Number(prop.edge).toFixed ? Number(prop.edge).toFixed(2) : prop.edge}
                  </div>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
