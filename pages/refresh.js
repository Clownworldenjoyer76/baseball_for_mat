
import { useState } from 'react';

export default function Refresh() {
  const [status, setStatus] = useState('');

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
      <h1 style={{ fontSize: '20px', marginBottom: '20px' }}>🔁 Refresh Game Props</h1>
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
        Regenerate Props
      </button>
      <p style={{ marginTop: '20px', fontSize: '14px', color: '#ccc' }}>{status}</p>
    </div>
  );
}
