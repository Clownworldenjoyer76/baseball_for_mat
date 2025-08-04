import Head from 'next/head'
import GameCard from '../components/GameCards.jsx'  // ✅ THIS IS THE FIXED LINE

export default function Home({ cards }) {
  return (
    <>
      <Head>
        <title>Game Cards</title>
      </Head>
      <main className="bg-black min-h-screen p-4 text-white">
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
          {cards?.length ? (
            cards.map((card, index) => (
              <GameCard key={index} data={card} />
            ))
          ) : (
            <p className="text-red-500">⚠️ No props loaded.</p>
          )}
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



Trigger  deploy
