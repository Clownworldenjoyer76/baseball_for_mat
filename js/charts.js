async function renderGameProjectionChart(gameId) {
  try {
    const rows = await loadCSV('data/_projections/game_projections.csv');
    let g = null;
    if (gameId) {
      g = rows.find(r => String(r.game_id || '').trim() === String(gameId));
    }
    if (!g && rows.length) g = rows[0]; // fallback

    if (!g) return;
    const el = document.getElementById('game-projection-chart');
    if (!el) return;

    const data = {
      labels: ['Home', 'Away'],
      datasets: [{
        label: 'Projected Runs',
        data: [Number(g.home_score || 0), Number(g.away_score || 0)]
      }]
    };

    new Chart(el, {
      type: 'bar',
      data,
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  } catch (e) {
    console.error('renderGameProjectionChart failed', e);
  }
}
