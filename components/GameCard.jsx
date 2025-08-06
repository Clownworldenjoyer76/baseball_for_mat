import Image from "next/image";

export default function GameCard({ game }) {
  return (
    <div className="bg-white/5 backdrop-blur-lg rounded-2xl border border-white/10 shadow-md p-4 mb-6 transition-all duration-150 active:scale-[0.97] hover:brightness-110">
      <h2 className="text-2xl font-semibold text-white mb-2 text-center">
        {game.away_team} <span className="text-gray-400">at</span> {game.home_team}
      </h2>

      <div className="flex justify-center items-center space-x-4 mb-4">
        <Image
          src={`/logos/${game.away_team.toLowerCase()}.png`}
          alt={game.away_team}
          width={24}
          height={24}
        />
        <Image
          src={`/logos/${game.home_team.toLowerCase()}.png`}
          alt={game.home_team}
          width={24}
          height={24}
        />
      </div>

      <div className="text-xs text-gray-400 flex justify-center gap-2 mb-4">
        <span>🕒 {game.game_time}</span>
        <span>🌡️ {game.temperature}°</span>
        <span>📍 {game.venue}</span>
      </div>

      <h3 className="text-sm text-gray-300 uppercase tracking-wide mb-2">Top Props</h3>
      <div className="space-y-2 mb-4">
        {game.top_props.map((prop, idx) => (
          <div key={idx} className="flex items-center space-x-3">
            <Image
              src={prop.headshot_url || "/fallback_headshot.png"}
              alt={prop.name}
              width={32}
              height={32}
              className="rounded-full border border-white/20"
            />
            <div className="text-sm text-white">
              <div className="font-medium">{prop.name}</div>
              <div className="text-gray-400 text-xs">{prop.prop_description}</div>
            </div>
          </div>
        ))}
      </div>

      <h3 className="text-sm text-gray-300 uppercase tracking-wide mb-1">Projected Score</h3>
      <div className="text-center text-white text-sm">
        {game.away_team} {game.away_score} <span className="text-gray-400">vs</span> {game.home_team} {game.home_score}
      </div>
    </div>
  );
}
