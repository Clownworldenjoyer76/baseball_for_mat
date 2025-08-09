// components/TopPropsCard.jsx
import React, { useEffect, useState } from 'react';

export default function TopPropsCard() {
  const [rows, setRows] = useState([]);
  const [ready, setReady] = useState(false);

  // Minimal CSV parser with quoted-field support
  const parseCSV = (text) => {
    const lines = text.trim().split(/\r?\n/);
    if (!lines.length) return [];
    const headers = splitCSVLine(lines[0]).map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).map((line) => {
      const cols = splitCSVLine(line).map(c => c.trim().replace(/^"|"$/g, ''));
      const obj = {};
      headers.forEach((h, i) => (obj[h] = cols[i] ?? ''));
      return obj;
    });
  };

  const splitCSVLine = (line) => {
    const cols = [];
    let cur = '';
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; continue; }
      if (ch === '"') { inQ = !inQ; continue; }
      if (ch === ',' && !inQ) { cols.push(cur); cur = ''; continue; }
      cur += ch;
    }
    cols.push(cur);
    return cols;
  };

  useEffect(() => {
    let cancel = false;
    (async () => {
      try {
        const res = await fetch('/data/bets/player_props_history.csv', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const text = await res.text();
        const all = parseCSV(text);

        // Normalize keys to lowercase for robust access
        const rowsNorm = all.map(r => {
          const obj = {};
          Object.keys(r).forEach(k => obj[k.toLowerCase()] = r[k]);
          return obj;
        });

        // Filter to Best Prop (keep file order)
        const filtered = rowsNorm.filter(r => (r['bet_type'] || '') === 'Best Prop');

        // Map to fields used by the card
        const mapped = filtered.map(r => ({
          playerId: r['player_id'] || r['playerid'] || '',
          name: r['name'] || '',
          team: r['team'] || r['team_code'] || '',
          line: r['line'] || r['prop_type'] || r['proptype'] || '',
        }));

        // Dedupe by playerId (fallback to name) and keep first 3
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

  // Keep your original markup/styles
  const uniquePlayers = rows;

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
          {uniquePlayers.map((prop, index) => (
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
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
