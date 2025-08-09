// components/TopPropsCard.jsx
import React, { useEffect, useState } from 'react';

const toFirstLast = (lastFirst) => {
  if (!lastFirst) return '';
  const s = String(lastFirst).trim();
  if (!s.includes(',')) return s; // already First Last
  const [last, rest] = [s.split(',')[0], s.split(',').slice(1).join(',')];
  const first = (rest || '').trim().split(/\s+/)[0] || '';
  return [first, last].filter(Boolean).join(' ');
};

const prettifyPropType = (t) => {
  if (!t) return '';
  return String(t).replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
};

export default function TopPropsCard() {
  const [rows, setRows] = useState([]);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancel = false;
    (async () => {
      try {
        const r = await fetch('/api/best-props', { cache: 'no-store' });
        const data = r.ok ? await r.json() : [];
        if (!cancel) setRows(Array.isArray(data) ? data.slice(0, 3) : []);
      } catch {
        if (!cancel) setRows([]);
      } finally {
        if (!cancel) setLoaded(true);
      }
    })();
    return () => { cancel = true; };
  }, []);

  const headshot = (id) =>
    id
      ? `https://securea.mlb.com/mlb/images/players/head_shot/${id}.jpg`
      : '/images/default_player.png';

  if (!loaded) return null;

  return (
    <div
      className="fade-in-card card-interactive"
      style={{ backgroundColor:'#1C1C1E', margin:'20px 0', borderRadius:12, overflow:'hidden', border:'1px solid #2F2F30' }}
    >
      <div style={{ padding:20 }}>
        <h4 style={{ margin:'0 0 15px', textAlign:'center', color:'#D4AF37' }}>Today’s Best Props</h4>

        {rows.length === 0 ? (
          <div style={{ color:'#B0B0B0', textAlign:'center' }}>No Best Props found.</div>
        ) : (
          <ul style={{ listStyle:'none', padding:0, margin:0, color:'#E0E0E0' }}>
            {rows.map((r, i) => {
              const name = toFirstLast(r.player_name);
              const line = r.prop_line
                ? `${prettifyPropType(r.prop_type)} Over ${r.prop_line}`
                : prettifyPropType(r.prop_type);
              return (
                <li key={i} style={{ display:'flex', alignItems:'center', marginBottom:15 }}>
                  <img
                    alt={name}
                    src={headshot(r.playerId)}
                    onError={(e) => { e.currentTarget.onerror = null; e.currentTarget.src = '/images/default_player.png'; }}
                    style={{ height:50, width:50, borderRadius:'50%', marginRight:15, backgroundColor:'#2F2F30', objectFit:'cover' }}
                  />
                  <div>
                    <div style={{ fontSize:'1em', display:'flex', alignItems:'center' }}>
                      {name} <span style={{ marginLeft:8, fontSize:'1.2em' }}>🔥</span>
                    </div>
                    <div style={{ fontSize:12, color:'#B0B0B0', marginTop:4 }}>{r.team}</div>
                    <div style={{ fontSize:14, color:'#E0E0E0', marginTop:4 }}>{line}</div>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}
