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
