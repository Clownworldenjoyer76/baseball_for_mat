import React from 'react';

export async function getStaticProps() {
  try {
    const res = await fetch('https://projects-6252969a.vercel.app/api/game-cards');
    const json = await res.json();

    if (!json || !json.batters || !json.pitchers) {
      return { props: { propsLoaded: false } };
    }

    return {
      props: {
        propsLoaded: true,
        batters: json.batters,
        pitchers: json.pitchers,
        weather: json.weather
      }
    };
  } catch (err) {
    return { props: { propsLoaded: false } };
  }
}

export default function Home({ propsLoaded, batters = [], pitchers = [], weather = [] }) {
  if (!propsLoaded) {
    return (
      <div className="text-white p-6">
        <p className="text-xl font-semibold">⚠️ No props loaded.</p>
      </div>
    );
  }

  return (
    <div className="text-white p-6">
      <h1 className="text-2xl font-bold mb-4">Today's Game Props</h1>
      <div>
        <h2 className="text-lg font-semibold">Batters:</h2>
        <ul>{batters.slice(0, 5).map((b, i) => <li key={i}>{b['last_name, first_name']}</li>)}</ul>
      </div>
      <div className="mt-4">
        <h2 className="text-lg font-semibold">Pitchers:</h2>
        <ul>{pitchers.slice(0, 5).map((p, i) => <li key={i}>{p['last_name, first_name']}</li>)}</ul>
      </div>
    </div>
  );
}
