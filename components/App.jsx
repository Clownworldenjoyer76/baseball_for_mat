import React, { useEffect, useState } from 'react';
import GameCard from './GameCard'; // Make sure this path is correct
import TopPropsCard from './TopPropsCard'; // Make sure this path is correct

function App() {
  const [gamesData, setGamesData] = useState([]);
  const [bestProps, setBestProps] = useState([]);

  useEffect(() => {
    // This is where your app gets the data. This example uses fake data.
    // In your real app, this would be a call to an API.
    const fetchedData = [
      {
        game_id: 1,
        away_team: "Yankees",
        home_team: "Red Sox",
        game_time: "7:05 PM",
        temperature: 75,
        venue: "Fenway Park",
        topProps: [
          { playerId: 1, name: "Aaron Judge", line: "Over 1.5 Hits", probability: 85 },
          { playerId: 2, name: "Gleyber Torres", line: "Over 0.5 HR", probability: 60 }
        ],
        projectedScore: { away: 5, home: 4, total: 9 }
      },
      {
        game_id: 2,
        away_team: "Dodgers",
        home_team: "Giants",
        game_time: "10:10 PM",
        temperature: 68,
        venue: "Oracle Park",
        topProps: [
          { playerId: 3, name: "Shohei Ohtani", line: "Over 2.5 Total Bases", probability: 92 },
          { playerId: 4, name: "Mookie Betts", line: "Over 1.5 Hits", probability: 88 }
        ],
        projectedScore: { away: 6, home: 3, total: 9 }
      }
    ];

    setGamesData(fetchedData);

    // This is the code that finds the top 3 props from all games.
    const allProps = fetchedData.flatMap(game => game.topProps);
    const sortedProps = allProps.sort((a, b) => b.probability - a.probability);
    const top3Props = sortedProps.slice(0, 3);
    setBestProps(top3Props);

  }, []);

  return (
    <div className="app-container">
      {/* This line displays the new card at the very top. */}
      <TopPropsCard bestProps={bestProps} />

      {/* These lines loop through all your games and display a GameCard for each one. */}
      {gamesData.map((game, index) => (
        <GameCard
          key={game.game_id}
          game={game}
          topProps={game.topProps}
          projectedScore={game.projectedScore}
          animationDelay={`${index * 0.1}s`}
        />
      ))}
    </div>
  );
}

export default App;
