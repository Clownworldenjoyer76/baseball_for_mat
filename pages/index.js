// pages/index.js
import Head from 'next/head';
import GameCards from '../components/GameCards.jsx';

export default function Home({ cards }) {
  return (
    <>
      <Head>
        <title>Game Cards</title>
      </Head>
      <main className="bg-black min-h-screen p-4 text-white">
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
          <GameCards cards={cards} />
        </div>
      </main>
    </>
  );
}

export async function getServerSideProps({ req }) {
  const protocol = req.headers['x-forwarded-proto'] || 'http';
  const host = req.headers.host;
  const baseUrl = `${protocol}://${host}`;

  try {
    const res = await fetch(`${baseUrl}/api/game-cards`);
    const data = await res.json();
    return { props: { cards: data || [] } };
  } catch {
    return { props: { cards: [] } };
  }
}
