/* ===== Config: map your column names here if they differ ===== */
const PLAYER_PROPS_COLS = {
  gameId:   'game_id',
  date:     'date',
  team:     'team',
  player:   'player_name',   // e.g., "Mookie Betts"
  market:   'market',        // e.g., "Hits", "HR"
  line:     'line',
  odds:     'odds',
  edge:     'edge',
  betType:  'bet_type'       // optional, e.g., "Best Prop"
};

const GAME_PROPS_COLS = {
  gameId:   'game_id',
  date:     'date',
  home:     'home_team',
  away:     'away_team',
  market:   'market',        // game market name, if present
  line:     'line',
  odds:     'odds',
  edge:     'edge',
  betType:  'bet_type'
};
/* ===================================================================== */

async function loadPlayerProps() {
  const rows = await loadCSV('data/bets/player_props_history.csv');
  return rows.map(r => ({
    type: 'player',
    game_id: String(r[PLAYER_PROPS_COLS.gameId] ?? '').trim(),
    date: (r[PLAYER_PROPS_COLS.date] ?? '').toString().slice(0,10),
    team: String(r[PLAYER_PROPS_COLS.team] ?? '').trim(),
    player_name: String(r[PLAYER_PROPS_COLS.player] ?? '').trim(),
    market: String(r[PLAYER_PROPS_COLS.market] ?? '').trim(),
    line: r[PLAYER_PROPS_COLS.line],
    odds: r[PLAYER_PROPS_COLS.odds],
    edge: r[PLAYER_PROPS_COLS.edge],
    betType: String(r[PLAYER_PROPS_COLS.betType] ?? '').trim()
  }));
}

async function loadGameProps() {
  const rows = await loadCSV('data/bets/game_props_history.csv');
  return rows.map(r => ({
    type: 'game',
    game_id: String(r[GAME_PROPS_COLS.gameId] ?? '').trim(),
    date: (r[GAME_PROPS_COLS.date] ?? '').toString().slice(0,10),
    home_team: String(r[GAME_PROPS_COLS.home] ?? '').trim(),
    away_team: String(r[GAME_PROPS_COLS.away] ?? '').trim(),
    market: String(r[GAME_PROPS_COLS.market] ?? '').trim(),
    line: r[GAME_PROPS_COLS.line],
    odds: r[GAME_PROPS_COLS.odds],
    edge: r[GAME_PROPS_COLS.edge],
    betType: String(r[GAME_PROPS_COLS.betType] ?? '').trim()
  }));
}

function renderTableRows(props) {
  return props.map(p => `
    <tr>
      <td>${p.player_name || ''}</td>
      <td>${p.market || ''}</td>
      <td>${p.line ?? ''}</td>
      <td>${p.odds ?? ''}</td>
      <td>${p.edge ?? ''}</td>
      <td>${p.betType || ''}</td>
    </tr>
  `).join('');
}

async function renderGameTopProps({ gameId, homeTeam, awayTeam, date }) {
  try {
    const [players, games] = await Promise.all([loadPlayerProps(), loadGameProps()]);

    // Filter by game/date (if present)
    const targetDate = (date || '').slice(0,10);
    const byGame = x =>
      (!gameId || String(x.game_id) === String(gameId)) &&
      (!targetDate || x.date === targetDate);

    const playerProps = players.filter(byGame);
    const gameProps   = games.filter(byGame);

    const titleEl = document.getElementById('matchup-title');
    if (titleEl) {
      const title = (homeTeam && awayTeam)
        ? `${awayTeam} @ ${homeTeam} — ${targetDate || ''}`
        : (targetDate || 'Props');
      titleEl.textContent = title;
    }

    // Merge & sort by edge desc
    const merged = [...playerProps, ...gameProps].sort((a, b) => (b.edge ?? 0) - (a.edge ?? 0));

    // Render
    const body = document.getElementById('props-body');
    body.innerHTML = merged.length ? renderTableRows(merged) : `<tr><td colspan="6">No props found for this game.</td></tr>`;
  } catch (e) {
    console.error('renderGameTopProps failed', e);
  }
}

// Wire up on load using query params from props.html
document.addEventListener('DOMContentLoaded', () => {
  const p = new URLSearchParams(location.search);
  renderGameTopProps({
    gameId: p.get('game_id'),
    homeTeam: p.get('home'),
    awayTeam: p.get('away'),
    date: p.get('date')
  });
});
