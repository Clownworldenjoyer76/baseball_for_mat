
const PLAYER_PROPS_COLS={gameId:'game_id',date:'date',team:'team',player:'name',market:'prop',line:'line',odds:'odds',edge:'over_probability',type:'bet_type'};
async function loadPlayerProps(){const rows=await loadCSV('data/bets/player_props_history.csv');return rows.map(r=>({game_id:String(r[PLAYER_PROPS_COLS.gameId]??'').trim(),date:(r[PLAYER_PROPS_COLS.date]??'').toString().slice(0,10),team:String(r[PLAYER_PROPS_COLS.team]??'').trim(),player:String(r[PLAYER_PROPS_COLS.player]??'').trim(),market:String(r[PLAYER_PROPS_COLS.market]??'').trim(),line:r[PLAYER_PROPS_COLS.line],odds:r[PLAYER_PROPS_COLS.odds],edge:r[PLAYER_PROPS_COLS.edge],type:String(r[PLAYER_PROPS_COLS.type]??'').trim()}));}
function pct(x){const n=Number(x);return Number.isFinite(n)?Math.round(n*100):null;}
function renderRows(props){
  return props.map(p=>{const e=pct(p.edge);return `<tr>
    <td>${p.player}</td><td>${p.market.toUpperCase?.()||p.market}</td><td>${p.line??''}</td><td>${p.odds??''}</td>
    <td>${e??''}%<div class="edge" style="margin-top:6px"><i style="width:${e??0}%"></i></div></td><td>${p.type||''}</td>
  </tr>`;}).join('');
}
async function renderGameTopProps(){
  const p=new URLSearchParams(location.search); const gameId=p.get('game_id'), date=(p.get('date')||'').slice(0,10); const home=p.get('home'), away=p.get('away');
  const title=document.getElementById('matchup-title'); if(title){title.innerHTML=`<div class="logo">${(home||'').slice(0,2)}</div><div><div class="title">${away} @ ${home}</div><div class="meta">${date}</div></div>`;}
  const body=document.getElementById('props-body');
  try{
    const players=await loadPlayerProps();
    const filtered=players.filter(x=>(!gameId||String(x.game_id)===String(gameId))&&(!date||x.date===date)).sort((a,b)=>(b.edge??0)-(a.edge??0));
    body.innerHTML=filtered.length?renderRows(filtered):`<tr><td colspan="6">No props found.</td></tr>`;
  }catch(e){console.error(e);body.innerHTML=`<tr><td colspan="6">Failed to load props.</td></tr>`;}
}
document.addEventListener('DOMContentLoaded', renderGameTopProps);
