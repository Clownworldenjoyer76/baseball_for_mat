// pages/api/game-top-props.js
import fs from 'fs';
import path from 'path';

// Split a CSV line with quoted fields
function split(line){const out=[];let cur='',q=false;for(let i=0;i<line.length;i++){const ch=line[i];
if(ch==='"'&&line[i+1]==='"'){cur+='"';i++;continue}
if(ch==='"'){q=!q;continue}
if(ch===','&&!q){out.push(cur);cur='';continue}
cur+=ch}out.push(cur);return out}
function parseCSV(abs){
  if(!fs.existsSync(abs)) return {headers:[],rows:[]};
  const lines = fs.readFileSync(abs,'utf8').split(/\r?\n/).filter(Boolean);
  if(!lines.length) return {headers:[],rows:[]};
  const headers = split(lines[0]).map(h=>h.trim());
  const rows = lines.slice(1).map(l=>{
    const c = split(l).map(x=>x.trim().replace(/^"|"$/g,''));
    return Object.fromEntries(headers.map((h,i)=>[h,c[i]??'']));
  });
  return {headers,rows};
}

// Format helpers
const toFirstLast = (lastFirst)=>{
  if(!lastFirst) return '';
  const s = String(lastFirst).trim();
  if(!s.includes(',')) return s;
  const [last, rest] = [s.split(',')[0], s.split(',').slice(1).join(',')];
  const first = (rest||'').trim().split(/\s+/)[0]||'';
  return [first,last].filter(Boolean).join(' ');
};
const prettify = (t)=> String(t||'').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());

export default function handler(req,res){
  // ✅ Cache‑busting headers (Vercel + browser)
  res.setHeader('Cache-Control', 'no-store, max-age=0, must-revalidate');
  res.setHeader('CDN-Cache-Control', 'no-store');
  res.setHeader('Vercel-CDN-Cache-Control', 'no-store');

  try{
    const home = (req.query.home||'').trim().toLowerCase();
    const away = (req.query.away||'').trim().toLowerCase();
    if(!home || !away) return res.status(400).json({error:'Missing ?home= and ?away='});

    const root = process.cwd();
    const playersCsv = path.join(root,'data','bets','player_props_history.csv');
    const { headers, rows } = parseCSV(playersCsv);
    if(!rows.length) return res.status(200).json([]);

    // Build lowercase header map (supports your known columns)
    const H = headers.map(h=>h.toLowerCase());
    const iName = H.findIndex(h=>['player_name','name','last_name, first_name'].includes(h));
    const iTeam = H.findIndex(h=>['team','team_code','team_name'].includes(h));
    const iType = H.findIndex(h=>['prop_type','market','bet'].includes(h));
    const iLine = H.findIndex(h=>['prop_line','line','threshold','total','number'].includes(h));
    if(iName===-1 || iTeam===-1) {
      return res.status(500).json({error:'Expected columns not found in player_props_history.csv', headers});
    }

    // ✅ Filter ONLY by team; take first 3 in file order (highest probability)
    const filtered = rows.filter(r=>{
      const team = (r[headers[iTeam]]||'').trim().toLowerCase();
      return team === home || team === away;
    });

    // Map to card shape
    const mapped = filtered.map(r=>{
      const rawName = r[headers[iName]] || '';
      const team    = r[headers[iTeam]] || '';
      const type    = iType>-1 ? r[headers[iType]] : '';
      const line    = iLine>-1 ? r[headers[iLine]] : '';
      return {
        playerId: '', // (optional) add mapping later
        name: toFirstLast(rawName),
        team,
        line: line ? `${prettify(type)} Over ${line}` : prettify(type),
      };
    });

    // Dedupe by name; take first 3
    const out=[], seen=new Set();
    for(const m of mapped){
      const k = m.name || m.team+m.line;
      if(!seen.has(k)){ seen.add(k); out.push(m); }
      if(out.length===3) break;
    }

    return res.status(200).json(out);
  }catch(e){
    return res.status(500).json({error:e.message||'Failed'});
  }
}
