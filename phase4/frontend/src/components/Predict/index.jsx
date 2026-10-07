import './index.css';
import { useEffect, useState } from 'react';
import {
  CircularProgressbar,
  buildStyles
} from 'react-circular-progressbar';
import 'react-circular-progressbar/dist/styles.css';

import getPredict from '../../api/predict';

function PredictSummary() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [asOfParam, setAsOf] = useState(null);
  const [gridIdParam, setGridId] = useState(1);
  const [error, setError] = useState(null);

  const fetchSummary = async (gridId, asOf = null) => {
    setLoading(true);
    setError(null);

    try {
      const data = await getPredict(gridId, asOf);

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
    fetchSummary(gridIdParam, asOfParam);
  }, [asOfParam, gridIdParam]);

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
          Loading prediction summary...
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
          onClick={() => fetchSummary(gridIdParam, asOfParam)}
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
          onClick={() => fetchSummary(gridIdParam, asOfParam)}
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
          <h1>Prediction Summary</h1>
          <p>
            Overview of the {gridIdParam} Prediction
          </p>
        </div>

        <div className="as-of-section">
          <label htmlFor="grid_id">Grid ID</label>

          <input
            id="grid_id"
            type="number"
            value={gridIdParam || 1}
            onChange={(event) => {
              setGridId(event.target.value || null);
            }}
          />
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
            Risk Score
          </div>

          <div className="card-value">
            {summary.risk_score ?? 0}
          </div>

          <div className="card-description">
            Risk Score for current grid id
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Risk Level
          </div>

          <div className="card-value">
            {summary.risk_level ?? 0}
          </div>

          <div className="card-description">
            Risk Level for current grid id
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Model Version
          </div>

          <div className="card-value">
            {summary.model_version ?? 0}
          </div>

          <div className="card-description">
            Model version used
          </div>
        </div>

        <div className="summary-card">
          <div className="card-title">
            Explantion Note
          </div>

          <div className="card-value">
            {summary.explanation_note ?? ""}
          </div>

          <div className="card-description">
            Explanation note on current model
          </div>
        </div>

      </div>
    </div>
  );
}

export default PredictSummary;