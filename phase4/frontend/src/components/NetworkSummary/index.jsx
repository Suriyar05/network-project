import './index.css';
import { useEffect, useState } from 'react';
import {
  CircularProgressbar,
  buildStyles
} from 'react-circular-progressbar';
import 'react-circular-progressbar/dist/styles.css';

import getNetworkSummary from '../../api/network_summary';

function NetworkSummary() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [asOfParam, setAsOf] = useState(null);
  const [error, setError] = useState(null);

  const fetchSummary = async (asOf = null) => {
    setLoading(true);
    setError(null);

    try {
      const data = await getNetworkSummary(asOf);

      if (data?.error) {
        setError(data.message || 'Failed to load network summary.');
        setSummary(null);
      } else {
        setSummary(data);
      }
    } catch (err) {
      setError(
        err?.message || 'Failed to load network summary.'
      );
      setSummary(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSummary(asOfParam);
  }, [asOfParam]);

  if (loading) {
    return (
      <div className="loading-container">
        <div className="progress-wrapper">
          <CircularProgressbar
            value={75}
            text="Loading"
            styles={buildStyles({
              rotation: 0.25,
              strokeLinecap: 'round',
              textSize: '12px',
              pathTransitionDuration: 1.5,
              pathColor: 'currentColor',
              textColor: 'currentColor',
              trailColor: '#e5e7eb',
            })}
          />
        </div>

        <p className="loading-text">
          Loading network summary...
        </p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-container">
        <div className="error-icon">!</div>

        <h2>Unable to load network summary</h2>

        <p>{error}</p>

        <button
          className="retry-button"
          onClick={() => fetchSummary(asOfParam)}
        >
          Try Again
        </button>
      </div>
    );
  }

  if (!summary) {
    return (
      <div className="error-container">
        <h2>No data available</h2>

        <button
          className="retry-button"
          onClick={() => fetchSummary(asOfParam)}
        >
          Try Again
        </button>
      </div>
    );
  }

  return (
    <div className="network-summary">

      <div className="summary-header">
        <div>
          <h1>Network Summary</h1>
          <p>
            Overview of the current telecom network activity
          </p>
        </div>

        <div className="as-of-section">
          <label htmlFor="as-of">As of</label>

          <input
            id="as-of"
            type="datetime-local"
            value={asOfParam || ''}
            onChange={(event) => {
              setAsOf(event.target.value || null);
            }}
          />
        </div>
      </div>

      <div className="summary-grid">

        <div className="summary-card">
          <div className="card-title">
            Total Activity
          </div>

          <div className="card-value">
            {summary.total_activity ?? 0}
          </div>

          <div className="card-description">
            Total network activity
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Top Grid
          </div>

          <div className="card-value">
            {summary.top_grid ?? 0}
          </div>

          <div className="card-description">
            Top Grid available
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Peak Hour
          </div>

          <div className="card-value">
            {summary.peak_hour ?? "0:00"}
          </div>

          <div className="card-description">
            Peak Hour available
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Active Grids
          </div>

          <div className="card-value">
            {summary.active_grids ?? 0}
          </div>

          <div className="card-description">
            Total active grids
          </div>
        </div>

      </div>
    </div>
  );
}

export default NetworkSummary;