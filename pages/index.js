// frontend/pages/index.js

import Head from 'next/head'
import GameCard from '../components/GameCard'

export default function Home({ cards }) {
  return (
    <>
      <Head>
        <title>Game Props</title>
      </Head>
      <main className="bg-black min-h-screen p-4 text-white">
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
          {cards.map((card, index) => (
            <GameCard key={index} data={card} />
          ))}
        </div>
      </main>
    </>
  )
}

export async function getServerSideProps(context) {
  const protocol = context.req.headers["x-forwarded-proto"] || "http";
  const host = context.req.headers.host;
  const baseUrl = `${protocol}://${host}`;

  try {
    const res = await fetch(`${baseUrl}/api/game_cards_data`);
    const data = await res.json();
    return { props: { cards: data || [] } };
  } catch {
    return { props: { cards: [] } };
  }
}
