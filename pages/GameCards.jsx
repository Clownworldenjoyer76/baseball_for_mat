
import React, { useEffect, useState } from 'react';

const STAT_ICONS = {
  "Strikeouts": "🧢 K",
  "ERA": "📉 ERA",
  "xFIP": "📊 xFIP",
  "Expected wOBA": "🎯 xwOBA",
  "Hits": "⚾ H",
  "Home Runs": "💣 HR",
  "Total Bases": "📦 TB"
};

export default function GameCards() {
  const [cards, setCards] = useState([]);
  const [filtered, setFiltered] = useState([]);
  const [teamFilter, setTeamFilter] = useState("All");
  const [typeFilter, setTypeFilter] = useState("All");
  const [darkMode, setDarkMode] = useState(false);

  useEffect(() => {
    fetch('/game_cards_data.json')
      .then(res => res.json())
      .then(data => {
        setCards(data);
        setFiltered(data);
      });
  }, []);

  const allTeams = Array.from(new Set(cards.flatMap(c =>
    c.props.map(p => p.split(", ")[1]?.split(" –")[0])
  ).filter(Boolean))).sort();

  const handleFilter = () => {
    let result = [...cards];
    if (teamFilter !== "All") {
      result = result.map(card => ({
        ...card,
        props: card.props.filter(p => p.includes(`, ${teamFilter} –`))
      })).filter(card => card.props.length > 0);
    }
    if (typeFilter !== "All") {
      result = result.map(card => ({
        ...card,
        props: card.props.filter(p => {
          const stat = p.split(" – ")[1] || "";
          const isPitcher = ["Strikeouts", "ERA", "xFIP", "Expected wOBA"].includes(stat);
          return (typeFilter === "Pitcher" && isPitcher) || (typeFilter === "Batter" && !isPitcher);
        })
      })).filter(card => card.props.length > 0);
    }
    setFiltered(result);
  };

  useEffect(() => {
    handleFilter();
  }, [teamFilter, typeFilter]);

  const bg = darkMode ? "bg-gray-900 text-white" : "bg-gray-100 text-gray-900";
  const cardBg = darkMode ? "bg-gray-800 text-white" : "bg-white text-gray-800";

  return (
    <div className={`${bg} min-h-screen p-4`}>
      <div className="flex flex-wrap items-center gap-4 mb-6">
        <h1 className="text-3xl font-bold mr-auto">Today's Matchups</h1>
        <label className="text-sm">
          Team:
          <select value={teamFilter} onChange={e => setTeamFilter(e.target.value)} className="ml-2 p-1 text-sm rounded text-black">
            <option>All</option>
            {allTeams.map(team => <option key={team}>{team}</option>)}
          </select>
        </label>
        <label className="text-sm">
          Type:
          <select value={typeFilter} onChange={e => setTypeFilter(e.target.value)} className="ml-2 p-1 text-sm rounded text-black">
            <option>All</option>
            <option>Batter</option>
            <option>Pitcher</option>
          </select>
        </label>
        <button
          onClick={() => setDarkMode(!darkMode)}
          className="text-sm bg-indigo-500 text-white px-3 py-1 rounded"
        >
          {darkMode ? "Light Mode" : "Dark Mode"}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {filtered.map((card, i) => (
          <div key={i} className={`${cardBg} p-4 rounded shadow`}>
            <h2 className="text-xl font-semibold mb-1">{card.matchup}</h2>
            <p className="text-sm mb-2">
              Temp: {card.temperature} | Precip: {card.precipitation}
            </p>
            <h3 className="font-semibold mb-1">Top Props</h3>
            {card.props.map((text, j) => {
              const [left, stat] = text.split(" – ");
              const icon = STAT_ICONS[stat] || "📈";
              return (
                <div key={j} className="text-sm py-1 flex justify-between items-center border-b border-gray-300">
                  <span>{left}</span>
                  <span className="ml-2 font-mono">{icon}</span>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}
