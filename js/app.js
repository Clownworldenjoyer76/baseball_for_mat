
async function loadCSV(path){return new Promise((resolve,reject)=>{Papa.parse(path,{download:true,header:true,dynamicTyping:true,skipEmptyLines:true,complete:r=>resolve(r.data),error:reject});});}
function pct(x){const n=Number(x);return Number.isFinite(n)?Math.round(n*100):null;}
function initials(name=''){const p=String(name).split(/\s+/).filter(Boolean);return (p[0]?.[0]||'')+(p[1]?.[0]||'');}
async function renderBestProps(){
  const wrap=document.getElementById('best-props'); if(!wrap) return;
  try{
    const data=await loadCSV('data/bets/player_props_history.csv');
    const ranked=[...data].sort((a,b)=>(b.value??0)-(a.value??0)).slice(0,12);
    wrap.innerHTML=ranked.map(row=>{
      const edgePct=pct(row.over_probability);
      return `<div class="card"><div class="body"><div class="media">
        <div class="logo">${initials(row.team||'')}</div>
        <div><div class="title">${row.name||''}</div>
        <div class="meta">${(row.team||'')} · ${(row.prop||'').toString().toUpperCase()} · Line ${row.line??''}</div></div>
      </div>
      <div style="margin-top:10px"><div class="meta">Over probability ${edgePct??''}%</div>
        <div class="edge"><i style="width:${edgePct??0}%"></i></div></div></div></div>`;
    }).join('');
  }catch(e){wrap.innerHTML=`<div class="card"><div class="body">Failed to load props.</div></div>`;console.error(e);}
}
document.addEventListener('DOMContentLoaded', renderBestProps);
