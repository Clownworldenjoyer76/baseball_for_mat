// pages/api/game-projection.js
import fs from 'fs';
import path from 'path';

// CSV utils (quoted fields supported)
function split(line){const out=[];let cur='',q=false;for(let i=0;i<line.length;i++){const ch=line[i];
if(ch==='"'&&line[i+1]==='"'){cur+='"';i++;continue}
if(ch==='"'){q=!q;continue}
if(ch===','&&!q){out.push(cur);cur='';continue}
cur+=ch}out.push(cur);return out}
function parseCSV(abs){
  if(!fs.existsSync(abs)) return {headers:[],rows:[]};
  const txt = fs.readFileSync(abs,'utf8');
  const lines = txt.split(/\r?\n/).filter(Boolean);
  if(!lines.length) return {headers:[],rows:[]};
  const headers = split(lines[0]).map(h=>h.trim());
  const rows = lines.slice(1).map(l=>{
    const c = split(l).map(x=>x.trim().replace(/^"|"$/g,''));
    return Object.fromEntries(headers.map((h,i)=>[h,c[i]??'']));
  });
  return {headers,rows};
}
const li = (headers, candidates) => {
  const L = headers.map(h=>h.toLowerCase());
  for(const want of candidates){
    const idx = L.indexOf(want);
    if(idx !== -1) return idx;
  }
  return -1;
};
const normTeam = (s) => String(s||'').trim().toLowerCase();

// Handler
export default function handler(req, res){
  // cache‑busting headers
  res.setHeader('Cache-Control', 'no-store, max-age=0, must-revalidate');
  res.setHeader('CDN-Cache-Control', 'no-store');
  res.setHeader('Vercel-CDN-Cache-Control', 'no-store');

  try{
    const homeQ = normTeam(req.query.home);
    const awayQ = normTeam(req.query.away);
    if(!homeQ || !awayQ) return res.status(400).json({ error: 'Missing ?home= and ?away=' });

    const csvPath = path.join(process.cwd(), 'data', 'bets', 'game_props_history.csv');
    const { headers, rows } = parseCSV(csvPath);
    if(!rows.length) return res.status(200).json({ home: null, away: null, total: null });

    // Find columns (flexible, case-insensitive)
    const iHomeTeam = li(headers, ['home_team','hometeam','home']);
    const iAwayTeam = li(headers, ['away_team','awayteam','away','visitor']);
    // Score/total candidates
    const iHomeScore = li(headers, ['home_score','home_prediction','homeprojected','homeproj','home_runs','home_runs_proj']);
    const iAwayScore = li(headers, ['away_score','away_prediction','awayprojected','awayproj','away_runs','away_runs_proj']);
    const iTotal     = li(headers, ['total','game_total','projected_total','run_total','real_run_total_projection']);

    if(iHomeTeam === -1 || iAwayTeam === -1){
      return res.status(500).json({ error: 'home/away team columns not found', headers });
    }

    // Pick the first matching row in file order (your file is ordered by probability)
    const match = rows.find(r => {
      const home = normTeam(r[headers[iHomeTeam]]);
      const away = normTeam(r[headers[iAwayTeam]]);
      return home === homeQ && away === awayQ;
    }) || rows.find(r => {
      // fallback: some sources might swap home/away in data; handle that just in case
      const home = normTeam(r[headers[iHomeTeam]]);
      const away = normTeam(r[headers[iAwayTeam]]);
      return home === awayQ && away === homeQ;
    });

    if(!match){
      return res.status(200).json({ home: null, away: null, total: null });
    }

    // Numbers
    const homeVal = iHomeScore !== -1 ? Number(match[headers[iHomeScore]]) : NaN;
    const awayVal = iAwayScore !== -1 ? Number(match[headers[iAwayScore]]) : NaN;
    let totalVal  = iTotal     !== -1 ? Number(match[headers[iTotal]])     : NaN;

    const home = Number.isFinite(homeVal) ? homeVal : null;
    const away = Number.isFinite(awayVal) ? awayVal : null;
    const total = Number.isFinite(totalVal)
      ? totalVal
      : (Number.isFinite(homeVal) && Number.isFinite(awayVal) ? +(homeVal + awayVal).toFixed(2) : null);

    return res.status(200).json({
      home: Number.isFinite(home) ? +(+home).toFixed(2) : null,
      away: Number.isFinite(away) ? +(+away).toFixed(2) : null,
      total: Number.isFinite(total) ? +(+total).toFixed(2) : null
    });
  }catch(e){
    return res.status(500).json({ error: e.message || 'Failed' });
  }
}
