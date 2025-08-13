async function loadCSV(path) {
  return new Promise((resolve, reject) => {
    Papa.parse(path, {
      download: true,
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      complete: (res) => resolve(res.data),
      error: reject
    });
  });
}

function mlbHeadshot(playerId) {
  if (!playerId) return 'assets/img/default_player.png';
  return `https://securea.mlb.com/mlb/images/players/head_shot/${playerId}.jpg`;
}

// ----- Best Props (Top card) -----
async function renderBestProps() {
  try {
    const data = await loadCSV('data/bets/player_props_history.csv');
    const best = data.filter(r => String(r.bet_type || '').toLowerCase() === 'best prop');

    // unique by playerId
    const seen = new Set();
    const unique = [];
    for (const r of best) {
      const id = String(r.playerId || r.player_id || '').trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      unique.push(r);
    }

    const container = document.querySelector('#best-props');
    if (!container) return;
    container.innerHTML = unique.slice(0, 10).map(p => `
      <div class="card card-interactive fade-in-card">
        <div class="card-body">
          <div class="media">
            <img src="${mlbHeadshot(p.playerId || p.player_id)}" alt="" class="avatar"/>
            <div class="media-content">
              <div class="title">${p.player_name || p.player || ''}</div>
              <div class="subtitle">${p.market || p.prop_type || ''} · ${p.team || ''}</div>
              <div class="meta">Edge: ${p.edge ?? ''} · Odds: ${p.odds ?? ''}</div>
            </div>
          </div>
        </div>
      </div>
    `).join('');
  } catch (e) {
    console.error('renderBestProps failed', e);
  }
}

document.addEventListener('DOMContentLoaded', renderBestProps);
