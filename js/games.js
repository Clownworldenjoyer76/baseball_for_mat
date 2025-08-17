
const SCHED={gameId:'game_id',date:'date',home:'home_team',away:'away_team'};
async function loadSched(){const rows=await loadCSV('data/bets/mlb_sched.csv');return rows.map(r=>({game_id:String(r[SCHED.gameId]??'').trim(),date:(r[SCHED.date]??'').toString().slice(0,10),home:String(r[SCHED.home]??'').trim(),away:String(r[SCHED.away]??'').trim()}));}
async function renderGamesList(){
  const listEl=document.getElementById('games-list'); if(!listEl) return;
  try{
    const games=(await loadSched()).filter(g=>!!g.game_id);
    listEl.innerHTML=games.map(g=>{
      const q=new URLSearchParams({game_id:g.game_id,date:g.date,home:g.home,away:g.away}).toString();
      return `<a class="card" href="props.html?${q}" style="text-decoration:none;color:inherit"><div class="body">
        <div class="media"><div class="logo">${(g.home||'').slice(0,2)}</div>
        <div><div class="title">${g.away} @ ${g.home}</div><div class="meta">${g.date}</div></div></div>
      </div></a>`;
    }).join('');
  }catch(e){listEl.innerHTML=`<div class="card"><div class="body">Failed to load schedule.</div></div>`;console.error(e);}
}
document.addEventListener('DOMContentLoaded', renderGamesList);
