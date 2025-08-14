/* ===== Config: map your schedule column names here if they differ ===== */
const SCHED = {
  gameId:   'game_id',         // e.g., 12345 (string/number)
  date:     'date',            // e.g., 2025-08-14 (YYYY-MM-DD)
  home:     'home_team',       // e.g., NYY
  away:     'away_team',       // e.g., BOS
  start:    'start_time'       // optional; e.g., 19:05
};
/* ===================================================================== */

async function loadSched() {
  const rows = await loadCSV('data/bets/mlb_sched.csv');
  // Basic normalization
  return rows.map(r => ({
    game_id: String(r[SCHED.gameId] ?? '').trim(),
    date: (r[SCHED.date] ?? '').toString().slice(0,10),
    home: String(r[SCHED.home] ?? '').trim(),
    away: String(r[SCHED.away] ?? '').trim(),
    start: String(r[SCHED.start] ?? '').trim(),
  }));
}

function todayYMD() {
  // Use user's local date; adjust if your CSV uses a different tz.
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,'0');
  const day = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}

async function renderGamesList() {
  const listEl = document.getElementById('games-list');
  if (!listEl) return;

  const sched = await loadSched();
  const today = todayYMD();

  const todays = sched.filter(g => g.date === today);
  const games = todays.length ? todays : sched.slice(0, 15); // fallback: first 15

  listEl.innerHTML = games.map(g => {
    const query = new URLSearchParams({ game_id: g.game_id, date: g.date, home: g.home, away: g.away }).toString();
    return `
      <a class="card card-interactive" href="props.html?${query}">
        <div class="card-body">
          <div class="title">${g.away} @ ${g.home}</div>
          <div class="meta">${g.date}${g.start ? ' · ' + g.start : ''}</div>
        </div>
      </a>
    `;
  }).join('') || `<div class="card"><div class="card-body">No games found.</div></div>`;
}

document.addEventListener('DOMContentLoaded', renderGamesList);
