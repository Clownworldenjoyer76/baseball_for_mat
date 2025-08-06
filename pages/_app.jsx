import { useState } from 'react';
import '../styles/globals.css'; // Optional if used

function MyApp({ Component, pageProps }) {
  const [menuOpen, setMenuOpen] = useState(false);

  const toggleMenu = () => setMenuOpen(prev => !prev);
  const closeMenu = () => setMenuOpen(false);

  return (
    <div style={{ position: 'relative', minHeight: '100vh', backgroundColor: '#121212', color: '#fff' }}>
      {/* ☰ Hamburger Button */}
      <button
        onClick={toggleMenu}
        style={{
          position: 'fixed',
          top: 16,
          left: 16,
          zIndex: 1000,
          fontSize: '1.5em',
          background: 'none',
          border: 'none',
          color: '#D4AF37',
          cursor: 'pointer'
        }}
      >
        ☰
      </button>

      {/* Slide-Out Menu */}
      {menuOpen && (
        <>
          <div
            onClick={closeMenu}
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              width: '100vw',
              height: '100vh',
              backgroundColor: 'rgba(0,0,0,0.6)',
              zIndex: 999
            }}
          />
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              height: '100vh',
              width: '75%',
              maxWidth: '300px',
              backgroundColor: '#1C1C1E',
              zIndex: 1001,
              padding: '20px',
              boxShadow: '2px 0 10px rgba(0,0,0,0.5)'
            }}
          >
            <h3 style={{ marginBottom: '20px', color: '#D4AF37' }}>Menu</h3>
            <nav style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
              <a href="/" onClick={closeMenu} style={linkStyle}>⚾ Home</a>
              <a href="/games" onClick={closeMenu} style={linkStyle}>📅 Games</a>
              <a href="/bets" onClick={closeMenu} style={linkStyle}>💸 Bets</a>
            </nav>
          </div>
        </>
      )}

      {/* Page Content */}
      <div style={{ paddingTop: '60px' }}>
        <Component {...pageProps} />
      </div>
    </div>
  );
}

const linkStyle = {
  color: '#E0E0E0',
  fontSize: '1em',
  textDecoration: 'none'
};

export default MyApp;
