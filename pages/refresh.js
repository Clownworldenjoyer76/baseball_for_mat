import { useState } from 'react';

export default function Refresh() {
  const [status, setStatus] = useState('');
  const [unlocked, setUnlocked] = useState(false);
  const [input, setInput] = useState('');

  const PASSWORD = '!refre$h';

  const handlePassword = () => {
    if (input === PASSWORD) {
      setUnlocked(true);
      setStatus('');
    } else {
      setStatus('❌ Incorrect password.');
    }
  };

  const handleRefresh = async () => {
    setStatus('Refreshing...');
    const res = await fetch('/api/refresh');
    const data = await res.json();
    if (data.success) {
      setStatus('✅ Updated!');
    } else {
      setStatus('❌ Failed: ' + data.error);
    }
  };

  return (
    <div style={{
      background: 'linear-gradient(180deg, #0f0f0f, #1a1a1a)',
      color: 'white',
      padding: '40px 20px',
      fontFamily: 'system-ui, sans-serif',
      minHeight: '100vh'
    }}>
      <h1 style={{ fontSize: '20px', marginBottom: '20px' }}>🔐 Refresh Game Props</h1>

      {!unlocked ? (
        <div>
          <input
            type="password"
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder="Enter password"
            style={{
              padding: '12px',
              fontSize: '14px',
              borderRadius: '6px',
              border: '1px solid #333',
              marginRight: '10px',
              backgroundColor: '#222',
              color: 'white'
            }}
          />
          <button onClick={handlePassword} style={{
            padding: '12px 20px',
            fontSize: '14px',
            background: '#444',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer'
          }}>
            Unlock
          </button>
          <p style={{ marginTop: '12px', fontSize: '13px', color: '#bbb' }}>{status}</p>
        </div>
      ) : (
        <div>
          <button onClick={handleRefresh} style={{
            padding: '14px 24px',
            fontSize: '16px',
            background: 'linear-gradient(90deg, #4ade80, #22c55e)',
            color: '#000',
            border: 'none',
            borderRadius: '10px',
            cursor: 'pointer',
            fontWeight: 'bold'
          }}>
            🔁 Regenerate Props
          </button>
          <p style={{ marginTop: '20px', fontSize: '14px', color: '#ccc' }}>{status}</p>
        </div>
      )}
    </div>
  );
}
