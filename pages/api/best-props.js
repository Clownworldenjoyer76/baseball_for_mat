import fs from 'fs';
import path from 'path';

// --- CSV helpers (quoted fields ok) ---
function splitCSVLine(line){const out=[];let cur='',q=false;for(let i=0;i<line.length;i++){const ch=line[i];
if(ch==='"'&&line[i+1]==='"'){cur+='"';i++;continue}
if(ch==='"'){q=!q;continue}
if(ch===','&&!q){out.push(cur);cur='';continue}
cur+=ch}out.push(cur);return out}
function parseCSV(abs){if(!fs.existsSync(abs))return{headers:[],rows:[]};
const lines=fs.readFileSync(abs,'utf8').split(/\r?\n/).filter(Boolean);
if(!lines.length)return{headers:[],rows:[]};
const headers=splitCSVLine(lines[0]).map(h=>h.trim());
const rows=lines.slice(1).map(l=>{const c=splitCSVLine(l).map(x=>x.trim());const o={};headers.forEach((h,i)=>o[h]=c[i]??'');return o});
return{headers,rows}}

// --- name helpers ---
function toLastFirstLower(s){
  if(!s) return '';
  const t=String(s).trim();
  if(t.includes(',')){
    const [last, rest] = [t.split(',')[0], t.split(',').slice(1).join(',')];
    const first = (rest||'').trim().split(/\s+/)[0]||'';
    return `${last.trim().toLowerCase()}, ${first.toLowerCase()}`;
  }
  const toks=t.split(/\s+/).filter(Boolean);
  if(toks.length>=2){return `${toks[toks.length-1].toLowerCase()}, ${toks[0].toLowerCase()}`;}
  return t.toLowerCase();
}

export default function handler(req,res){
  try{
    const root = process.cwd();

    // 1) Read props history
    const hist = parseCSV(path.join(root,'data','bets','player_props_history.csv'));
    if(!hist.rows.length) return res.status(200).json([]);

    // 2) Read batter projections for IDs
    const bat  = parseCSV(path.join(root,'data','_projections','batter_props_z_expanded.csv'));

    // Build name -> id map (best‑effort)
    const bLower = bat.headers.map(h=>h.toLowerCase());
    const idxName = bLower.findIndex(h => ['name','player_name','last_name, first_name','full_name'].includes(h));
    const idxId   = bLower.findIndex(h => ['player_id','playerid','mlb_id','id'].includes(h));
    const nameToId = new Map();
    if(idxName !== -1 && idxId !== -1){
      for(const r of bat.rows){
        const key = toLastFirstLower(r[bat.headers[idxName]]);
        const id  = String(r[bat.headers[idxId]]||'').trim();
        if(key && id) nameToId.set(key, id);
      }
    }

    // 3) Filter + map top 3 Best Prop
    const h = hist.headers.map(h=>h.toLowerCase());
    const iBet   = h.indexOf('bet_type');
    const iName  = h.findIndex(x=>['player_name','name','last_name, first_name'].includes(x));
    const iTeam  = h.findIndex(x=>['team','team_code','team_name'].includes(x));
    const iType  = h.findIndex(x=>['prop_type','market','bet'].includes(x));
    const iLine  = h.findIndex(x=>['prop_line','line','threshold','total','number'].includes(x));
    if(iBet===-1) return res.status(500).json({error:'bet_type column not found'});

    const out=[];
    for(const r of hist.rows){
      const betType = (r[hist.headers[iBet]]||'').trim().toLowerCase();
      if(betType !== 'best prop') continue;

      const rawName = iName>-1 ? r[hist.headers[iName]] : '';
      const key     = toLastFirstLower(rawName);
      const playerId= nameToId.get(key) || '';

      out.push({
        // pass raw fields; component will format
        playerId,
        player_name: rawName || '',
        team: iTeam>-1 ? r[hist.headers[iTeam]] : '',
        prop_type: iType>-1 ? r[hist.headers[iType]] : '',
        prop_line: iLine>-1 ? r[hist.headers[iLine]] : ''
      });
      if(out.length>=3) break;
    }

    return res.status(200).json(out);
  }catch(e){
    return res.status(500).json({error:e.message||'Failed'});
  }
}
