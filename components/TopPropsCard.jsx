import React, { useEffect, useState } from 'react';

function TopPropsCard({ bestProps }) {
  const [rows, setRows] = useState(bestProps || []);
  const [ready, setReady] = useState(!!bestProps);

  // Tiny CSV parser (handles quotes)
  const parseCSV = (text) => {
    const lines = text.trim().split(/\r?\n/);
    if (!lines.length) return [];
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).map(line => {
      const cols = [];
      let cur = '', inQ = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; continue; }
        if (ch === '"') { inQ = !inQ; continue; }
        if (ch === ',' && !inQ) { cols.push(cur); cur = ''; continue; }
        cur += ch;
      }
      cols.push(cur);
      const obj = {};
      headers.forEach((h, i) => { obj[h] = (cols[i] ?? '').trim().replace(/^"|"$/g, ''); });
      return obj;
    });
  };

  useEffect(() => {
    if (bestProps) return; // use provided data
    let cancel = false;
    (async () => {
      try {
        const res = await fetch('/data/bets/player_props_history.csv', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const text = await res.text();
        const all = parseCSV(text);

        // Filter where bet_type === "Best Prop"
        const filtered = all.filter(r => (r.bet_type || r.betType) === 'Best Prop');

        // Sort if helpful columns exist (else keep file order)
        const sorted = [...filtered].sort((a, b) => {
          const keys = ['probability', 'edge', 'value', 'mega_z', 'z_score'];
          for (const k of keys) {
            const av = parseFloat(a[k]);
            const bv = parseFloat(b[k]);
            if (!Number.isNaN(av) && !Number.isNaN(bv) && av !== bv) return bv - av;
          }
          return 0;
        });

        // Map to the fields this card expects
        const mapped = sorted.map(r => ({
          playerId: r.player_id || r.playerId || '',
          name: r.name || '',
          team: r.team || r.team_code || '',
          line: r.line ?? r.prop_type ?? r.propType ?? '',
        }));

        // Dedupe by playerId (fallback to name)
        const unique = [];
        const seen = new Set();
        for (const r of mapped) {
          const key = r.playerId || r.name;
          if (!seen.has(key)) {
            seen.add(key);
            unique.push(r);
          }
          if (unique.length >= 3) break;
        }

        if (!cancel) setRows(unique);
      } catch {
        if (!cancel) setRows([]); // fail silent per current UX
      } finally {
        if (!cancel) setReady(true);
      }
    })();
    return () => { cancel = true; };
  }, [bestProps]);

  const getHeadshotUrl = (playerId) => {
    if (!playerId) return '/images/default_player.png';
    return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
  };

  if (!ready || !rows || rows.length === 0) return null;

  // Keep your exact styling/markup, just swap data source to `rows`
  const uniquePlayers = [];
  const playerIds = new Set();
  rows.forEach(prop => {
    const pid = prop.playerId || prop.name;
    if (!playerIds.has(pid)) {
      playerIds.add(pid);
      uniquePlayers.push(prop);
    }
  });

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
          {uniquePlayers.slice(0, 3).map((prop, index) => (
            <li key={index} style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
              <img
                alt={prop.name}
                src={getHeadshotUrl(prop.playerId)}
                onError={(e) => { e.target.onerror = null; e.target.src = '/images/default_player.png'; }}
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
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default TopPropsCard;
