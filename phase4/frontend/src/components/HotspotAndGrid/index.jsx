import React, { useEffect, useMemo, useRef, useState } from "react";
import { MapContainer, TileLayer, GeoJSON, useMap } from "react-leaflet";
import axios from "axios";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import "./index.css";

import { getHotspots, getAlerts } from "../../api/hotspot_alerts";

// Fit map to the loaded GeoJSON bounds
function FitGeoJSON({ geoJson }) {
  const map = useMap();

  useEffect(() => {
    if (!geoJson?.features?.length) return;

    const bounds = L.geoJSON(geoJson).getBounds();

    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [20, 20] });
    }
  }, [geoJson, map]);

  return null;
}

function HotspotsAndAlerts({ onSelectGrid, asOf = null }) {
  const [geoJson, setGeoJson] = useState(null);
  const [geoJsonError, setGeoJsonError] = useState(null);

  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);

  const [hotspotsLoading, setHotspotsLoading] = useState(true);
  const [alertsLoading, setAlertsLoading] = useState(true);

  const [hotspotsError, setHotspotsError] = useState(null);
  const [alertsError, setAlertsError] = useState(null);

  const [hotspotLimit, setHotspotLimit] = useState(10);
  const [alertLimit, setAlertLimit] = useState(10);
  const [alertSeverity, setAlertSeverity] = useState("");

  const [selectedGridId, setSelectedGridId] = useState(null);

  const mapRef = useRef(null);
  const geoJsonRef = useRef(null);

  // Load Milan grid GeoJSON
  useEffect(() => {
    let cancelled = false;

    const loadGeoJSON = async () => {
      setGeoJsonError(null);

      for (const path of [
        "/reference/milano-grid.geojson",
        "/reference/milan-grid.geojson",
      ]) {
        try {
          const response = await axios.get(path);
          const data = response.data;

          if (
            data?.type !== "FeatureCollection" ||
            !Array.isArray(data.features)
          ) {
            throw new Error("Invalid GeoJSON FeatureCollection");
          }

          if (!cancelled) setGeoJson(data);
          return;
        } catch (error) {
          if (!cancelled) setGeoJsonError(error.message);
        }
      }
    };

    loadGeoJSON();

    return () => {
      cancelled = true;
    };
  }, []);

  // Load hotspots and alerts from their separate endpoints
  useEffect(() => {
    let cancelled = false;

    const loadData = async () => {
      setHotspotsLoading(true);
      setAlertsLoading(true);
      setHotspotsError(null);
      setAlertsError(null);

      const [hotspotResponse, alertResponse] = await Promise.all([
        getHotspots(hotspotLimit, "", asOf),
        getAlerts(alertLimit, alertSeverity, asOf),
      ]);

      if (cancelled) return;

      if (hotspotResponse?.error) {
        setHotspots([]);
        setHotspotsError(
          hotspotResponse.message || "Unable to load hotspots"
        );
      } else {
        setHotspots(
          Array.isArray(hotspotResponse?.data)
            ? hotspotResponse.data
            : []
        );
      }

      if (alertResponse?.error) {
        setAlerts([]);
        setAlertsError(
          alertResponse.message || "Unable to load alerts"
        );
      } else {
        setAlerts(
          Array.isArray(alertResponse?.data)
            ? alertResponse.data
            : []
        );
      }

      setHotspotsLoading(false);
      setAlertsLoading(false);
    };

    loadData();

    return () => {
      cancelled = true;
    };
  }, [hotspotLimit, alertLimit, alertSeverity, asOf]);

  // Get grid ID regardless of API/GeoJSON naming convention
  const getGridId = (feature) => {
    const p = feature?.properties || {};

    return (
      p.cellId ??
      p.cell_id ??
      p.grid_id ??
      p.gridId ??
      p.id ??
      feature?.id ??
      null
    );
  };

  const getItemGridId = (item) =>
    item?.grid_id ??
    item?.gridId ??
    item?.cell_id ??
    item?.cellId ??
    item?.grid ??
    null;

  // Combine API results into one grid lookup for map styling
  const gridMap = useMemo(() => {
    const map = new Map();

    hotspots.forEach((item) => {
      const id = getItemGridId(item);
      if (id != null) {
        map.set(String(id), { ...item, source: "hotspot" });
      }
    });

    // Alerts override hotspots when both exist for the same grid
    alerts.forEach((item) => {
      const id = getItemGridId(item);
      if (id != null) {
        map.set(String(id), { ...item, source: "alert" });
      }
    });

    return map;
  }, [hotspots, alerts]);

  const getSeverity = (item) =>
    String(
      item?.severity ||
      item?.level ||
      item?.status ||
      "NORMAL"
    ).toUpperCase();

  // Convert severity into map status
  const getGridStatus = (gridId) => {
    const severity = getSeverity(
      gridMap.get(String(gridId))
    );

    if (severity === "CRITICAL") return "CRITICAL";
    if (severity === "HIGH") return "HIGH";
    if (severity === "MEDIUM") return "MEDIUM";
    if (severity === "LOW") return "LOW";

    return "NORMAL";
  };

  // Dynamically style each GeoJSON grid from API data
  const getGridStyle = (feature) => {
    const gridId = getGridId(feature);
    const status = getGridStatus(gridId);

    if (
      selectedGridId != null &&
      String(selectedGridId) === String(gridId)
    ) {
      return {
        color: "#111827",
        weight: 4,
        fillColor: "#2563eb",
        fillOpacity: 0.8,
      };
    }

    const styles = {
      CRITICAL: {
        color: "#991b1b",
        weight: 2,
        fillColor: "#dc2626",
        fillOpacity: 0.75,
      },
      HIGH: {
        color: "#b91c1c",
        weight: 1.5,
        fillColor: "#ef4444",
        fillOpacity: 0.65,
      },
      MEDIUM: {
        color: "#b45309",
        weight: 1.5,
        fillColor: "#f59e0b",
        fillOpacity: 0.6,
      },
      LOW: {
        color: "#15803d",
        weight: 1,
        fillColor: "#22c55e",
        fillOpacity: 0.4,
      },
      NORMAL: {
        color: "#64748b",
        weight: 0.8,
        fillColor: "#cbd5e1",
        fillOpacity: 0.25,
      },
    };

    return {
      opacity: 1,
      ...styles[status],
    };
  };

  const formatActivity = (item) => {
    const value =
      item?.activity?.total_activity ??
      item?.total_activity ??
      item?.current_activity ??
      item?.activity;

    if (value == null || value === "") return "—";

    const number = Number(value);

    return Number.isNaN(number)
      ? String(value)
      : number.toLocaleString(undefined, {
          maximumFractionDigits: 2,
        });
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return "—";

    const date = new Date(timestamp);

    return Number.isNaN(date.getTime())
      ? String(timestamp)
      : date.toLocaleString();
  };

  const getReason = (item) =>
    item?.reason || item?.message || "—";

  // Select, zoom and highlight a grid
  const selectGrid = (gridId) => {
    if (gridId == null) return;

    const id = String(gridId);
    setSelectedGridId(id);

    onSelectGrid?.(id);

    geoJsonRef.current?.eachLayer((layer) => {
      if (String(getGridId(layer.feature)) !== id) return;

      layer.setStyle({
        color: "#111827",
        weight: 4,
        fillColor: "#2563eb",
        fillOpacity: 0.8,
      });

      layer.bringToFront();

      if (layer.getBounds && mapRef.current) {
        const bounds = layer.getBounds();

        if (bounds.isValid()) {
          mapRef.current.fitBounds(bounds, {
            padding: [40, 40],
            maxZoom: 15,
          });
        }
      }

      layer.openPopup?.();
    });
  };

  // Build popup information for a grid
  const createPopup = (gridId) => {
    const item = gridMap.get(String(gridId));

    if (!item) {
      return `
        <div>
          <strong>Grid #${gridId}</strong><br/>
          Status: NORMAL<br/>
          Activity: —<br/>
          No current hotspot or alert.
        </div>
      `;
    }

    return `
      <div>
        <strong>Grid #${gridId}</strong><br/>
        Source: ${item.source}<br/>
        Severity: ${getSeverity(item)}<br/>
        Activity: ${formatActivity(item)}<br/>
        Status: ${item.status || getSeverity(item)}<br/>
        Reason: ${getReason(item)}
      </div>
    `;
  };

  // Add popup, tooltip and hover/click behavior to each grid
  const handleEachFeature = (feature, layer) => {
    const gridId = getGridId(feature);

    layer.bindPopup(createPopup(gridId));
    layer.bindTooltip(`Grid #${gridId}`, { sticky: true });

    layer.on({
      click: () => selectGrid(gridId),

      mouseover: (event) => {
        if (String(selectedGridId) !== String(gridId)) {
          event.target.setStyle({
            weight: 3,
            color: "#111827",
            fillOpacity: 0.65,
          });

          event.target.bringToFront();
        }
      },

      mouseout: (event) => {
        if (String(selectedGridId) !== String(gridId)) {
          event.target.setStyle(getGridStyle(feature));
        }
      },
    });
  };

  return (
    <div className="network-summary">

      {/* Header and filters */}
      <div className="summary-header">
        <div>
          <h1>Hotspots & Alerts</h1>
          <p>Network activity across Milan grid cells</p>
        </div>

        <div className="as-of-section">
          <div>
            <label>Hotspot Limit</label>
            <select
              value={hotspotLimit}
              onChange={(e) =>
                setHotspotLimit(Number(e.target.value))
              }
              className="filter-select"
            >
              <option value={5}>5</option>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </div>

          <div>
            <label>Alert Limit</label>
            <select
              value={alertLimit}
              onChange={(e) =>
                setAlertLimit(Number(e.target.value))
              }
              className="filter-select"
            >
              <option value={5}>5</option>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </div>

          <div>
            <label>Alert Severity</label>
            <select
              value={alertSeverity}
              onChange={(e) =>
                setAlertSeverity(e.target.value)
              }
              className="filter-select"
            >
              <option value="">All Severities</option>
              <option value="low">LOW</option>
              <option value="medium">MEDIUM</option>
              <option value="high">HIGH</option>
              <option value="critical">CRITICAL</option>
            </select>
          </div>
        </div>
      </div>

      {/* GeoJSON status */}
      {geoJsonError && (
        <div className="error-message">
          {geoJsonError}
        </div>
      )}

      {geoJson && (
        <div className="status-banner">
          Milan grid loaded:{" "}
          <strong>{geoJson.features.length}</strong> grids.
        </div>
      )}

      {/* Dynamic Leaflet grid */}
      <div
        style={{
          height: "550px",
          width: "100%",
          marginBottom: "25px",
          borderRadius: "8px",
          overflow: "hidden",
          border: "1px solid #d1d5db",
        }}
      >
        {geoJson ? (
          <MapContainer
            center={[45.4642, 9.19]}
            zoom={11}
            scrollWheelZoom
            style={{ height: "100%", width: "100%" }}
            ref={mapRef}
          >
            <TileLayer
              attribution="&copy; OpenStreetMap contributors"
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            <FitGeoJSON geoJson={geoJson} />

            <GeoJSON
              key={`${hotspots.length}-${alerts.length}-${selectedGridId}`}
              ref={geoJsonRef}
              data={geoJson}
              style={getGridStyle}
              onEachFeature={handleEachFeature}
            />
          </MapContainer>
        ) : (
          <div className="loading-state">
            Loading Milan grid...
          </div>
        )}
      </div>

      {/* Results table helper */}
      {[
        {
          title: "Hotspots",
          data: hotspots,
          loading: hotspotsLoading,
          error: hotspotsError,
        },
        {
          title: "Alerts",
          data: alerts,
          loading: alertsLoading,
          error: alertsError,
        },
      ].map((section) => (
        <section
          className="results-section"
          key={section.title}
        >
          <div className="section-header">
            <h3>{section.title}</h3>
            <span>{section.data.length} results</span>
          </div>

          {section.loading && (
            <div className="status-banner">
              Loading {section.title.toLowerCase()}...
            </div>
          )}

          {section.error && (
            <div className="error-banner">
              {section.title} unavailable: {section.error}
            </div>
          )}

          {!section.loading &&
            !section.error &&
            section.data.length === 0 && (
              <div className="status-banner">
                No {section.title.toLowerCase()} found.
              </div>
            )}

          {!section.loading &&
            !section.error &&
            section.data.length > 0 && (
              <div className="activity-table-container">
                <table className="activity-table">
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Grid</th>
                      <th>Activity</th>
                      <th>Severity</th>
                      <th>Status</th>
                      <th>Timestamp</th>
                      <th>Reason</th>
                    </tr>
                  </thead>

                  <tbody>
                    {section.data.map((item, index) => {
                      const gridId = getItemGridId(item);
                      const severity = getSeverity(item);
                      const selected =
                        String(selectedGridId) === String(gridId);

                      return (
                        <tr
                          key={`${gridId}-${index}`}
                          className={
                            selected ? "selected-row" : ""
                          }
                          onClick={() => selectGrid(gridId)}
                          style={{ cursor: "pointer" }}
                        >
                          <td>{index + 1}</td>
                          <td>
                            <strong>{gridId ?? "—"}</strong>
                          </td>
                          <td>{formatActivity(item)}</td>
                          <td>
                            <span
                              className={`severity ${severity.toLowerCase()}`}
                            >
                              {severity}
                            </span>
                          </td>
                          <td>
                            {item.status || severity}
                          </td>
                          <td>
                            {formatTimestamp(item.timestamp)}
                          </td>
                          <td>{getReason(item)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
        </section>
      ))}
    </div>
  );
}

export default HotspotsAndAlerts;