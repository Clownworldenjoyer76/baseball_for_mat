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
      backgroundColor: '#111',
      color: 'white',
      padding: '40px',
      fontFamily: 'system-ui, sans-serif',
      minHeight: '100vh'
    }}>
      <h1>Refresh Game Cards</h1>
      <button onClick={handleRefresh} style={{
        padding: '12px 24px',
        fontSize: '16px',
        backgroundColor: '#28a745',
        color: 'white',
        border: 'none',
        borderRadius: '6px',
        cursor: 'pointer'
      }}>
        🔁 Regenerate Props
      </button>
      <p style={{ marginTop: '20px' }}>{status}</p>
    </div>
  );
}