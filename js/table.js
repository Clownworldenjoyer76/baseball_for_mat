async function renderGameTopProps({ gameId, homeTeam, awayTeam, date }) {
  try {
    const all = await loadCSV('data/bets/player_props_history.csv');
    let rows = all;

    if (gameId) {
      rows = rows.filter(r => String(r.game_id || '').trim() === String(gameId));
    } else if (homeTeam && awayTeam && date) {
      const d = String(date).slice(0, 10);
      rows = rows.filter(r =>
        String(r.date || '').slice(0,10) === d &&
        [r.home_team, r.away_team, r.team].map(x => (x||'').toLowerCase()).some(v =>
          v.includes((homeTeam||'').toLowerCase()) || v.includes((awayTeam||'').toLowerCase())
        )
      );
    }

    // show "Best Prop" first if desired
    rows.sort((a,b) => (b.bet_type === 'Best Prop') - (a.bet_type === 'Best Prop'));

    const tbody = document.querySelector('#game-props tbody');
    if (!tbody) return;
    tbody.innerHTML = rows.slice(0, 50).map(r => `
      <tr>
        <td>${r.player_name || ''}</td>
        <td>${r.team || ''}</td>
        <td>${r.market || r.prop_type || ''}</td>
        <td>${r.line ?? ''}</td>
        <td>${r.odds ?? ''}</td>
        <td>${r.edge ?? ''}</td>
      </tr>
    `).join('');
  } catch (e) {
    console.error('renderGameTopProps failed', e);
  }
}
