import React, { useState, useEffect } from 'react';
import './index.css';
import getNetworkGrid from '../../api/network_grid';

function GridActivity({ initialGridId = 1 }) {
  const [gridId, setGridId] = useState(initialGridId);
  const [asOf, setAsOf] = useState('');
  const [date, setDate] = useState('');
  const [hour, setHour] = useState('');
  
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Fetch data whenever key filters change
  useEffect(() => {
    async function fetchData() {
      if (!gridId) return;
      setLoading(true);
      setError(null);

      const response = await getNetworkGrid(
        gridId,
        date || null,
        hour !== '' ? parseInt(hour, 10) : null,
        asOf || null
      );

      if (response?.error) {
        setError(response.message);
        setData(null);
      } else {
        setData(response);
      }
      setLoading(false);
    }

    fetchData();
  }, [gridId, date, hour, asOf]);

  // Aggregate metrics calculated from the time-series points
  const points = data?.data || [];
  const latestPoint = points[points.length - 1] || {};

  const totalActivity = points.reduce((acc, curr) => acc + (curr.total_activity || 0), 0);
  const totalCalls = points.reduce((acc, curr) => acc + (curr.total_calls || 0), 0);
  const totalSMS = points.reduce((acc, curr) => acc + (curr.total_sms || 0), 0);
  const totalInternet = points.reduce((acc, curr) => acc + (curr.internet_activity || 0), 0);

  return (
    <div className="network-summary">
      {/* Header & Controls */}
      <div className="summary-header">
        <div>
          <h1>Grid Activity</h1>
          <p>Detailed telecom metrics for Grid #{data?.grid_id ?? gridId}</p>
        </div>

        {/* Dynamic Filters */}
        <div className="as-of-section" style={{ flexWrap: 'wrap', gap: '10px' }}>
          <div>
            <label>Grid ID</label>
            <input
              type="number"
              value={gridId}
              onChange={(e) => setGridId(e.target.value)}
              style={{ width: '80px' }}
            />
          </div>

          <div>
            <label>Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
            />
          </div>

          <div>
            <label>Hour (0-23)</label>
            <input
              type="number"
              min="0"
              max="23"
              value={hour}
              placeholder="All"
              onChange={(e) => setHour(e.target.value)}
              style={{ width: '70px' }}
            />
          </div>

          <div>
            <label>As of</label>
            <input
              type="datetime-local"
              value={asOf}
              onChange={(e) => setAsOf(e.target.value)}
            />
          </div>
        </div>
      </div>

      {loading && <div className="loading-state">Loading grid activity...</div>}
      {error && <div className="error-message" style={{ color: 'red' }}>{error}</div>}

      {!loading && !error && data && (
        <>
          {/* Main Summary Metric Cards */}
          <div className="summary-grid">
            <div className="summary-card">
              <div className="card-title">Total Volume</div>
              <div className="card-value">{totalActivity.toLocaleString()}</div>
              <div className="card-description">Aggregated activity for selected period</div>
            </div>

            <div className="summary-card">
              <div className="card-title">Total Calls</div>
              <div className="card-value">{totalCalls.toLocaleString()}</div>
              <div className="card-description">Inbound + Outbound voice traffic</div>
            </div>

            <div className="summary-card">
              <div className="card-title">Total SMS</div>
              <div className="card-value">{totalSMS.toLocaleString()}</div>
              <div className="card-description">Inbound + Outbound messaging traffic</div>
            </div>

            <div className="summary-card">
              <div className="card-title">Internet Share</div>
              <div className="card-value">
                {latestPoint.internet_share 
                  ? `${(latestPoint.internet_share * 100).toFixed(1)}%` 
                  : '0%'}
              </div>
              <div className="card-description">Share in latest timeframe point</div>
            </div>
          </div>

          {/* Activity Timeseries Breakdown Table */}
          <div className="activity-table-container" style={{ marginTop: '20px' }}>
            <table className="activity-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '2px solid #ccc', textAlign: 'left' }}>
                  <th>Timestamp</th>
                  <th>Calls (In/Out)</th>
                  <th>SMS (In/Out)</th>
                  <th>Internet</th>
                  <th>Total Activity</th>
                  <th>Internet Share</th>
                </tr>
              </thead>
              <tbody>
                {points.length > 0 ? (
                  points.map((point, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid #eee' }}>
                      <td>{new Date(point.timestamp).toLocaleString()}</td>
                      <td>{point.call_in} / {point.call_out}</td>
                      <td>{point.sms_in} / {point.sms_out}</td>
                      <td>{point.internet_activity}</td>
                      <td><strong>{point.total_activity}</strong></td>
                      <td>{(point.internet_share * 100).toFixed(1)}%</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan="6" style={{ textAlign: 'center', padding: '12px' }}>
                      No activity data found for the selected criteria.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

export default GridActivity;