import { useState } from "react";

import NetworkSummary from "../../components/NetworkSummary";
import GridActivity from "../../components/NetworkGrid";
import HotspotsAndAlerts from "../../components/HotspotAndGrid";
import PredictSummary from "../../components/Predict";

import "./index.css";


function Dashboard() {

  const [selectedGridId, setSelectedGridId] = useState(1);
  const [activeTab, setActiveTab] = useState("summary");


  return (

    <div className="dashboard-container">


      {/* =====================================================
          HEADER
          ===================================================== */}

      <header className="dashboard-header">

        <div className="header-content">

          <h1>
            Network Operations Dashboard
          </h1>

          <p>
            Network Intelligence Service
          </p>

        </div>

      </header>


      {/* =====================================================
          NAVIGATION BAR
          ===================================================== */}

      <nav className="dashboard-nav">

  


        <button
          className={`nav-tab ${
            activeTab === "summary" ? "active" : ""
          }`}
          onClick={() => setActiveTab("summary")}
        >
          Network Overview
        </button>


        <button
          className={`nav-tab ${
            activeTab === "grid" ? "active" : ""
          }`}
          onClick={() => setActiveTab("grid")}
        >
          Grid Explorer
        </button>


        <button
          className={`nav-tab ${
            activeTab === "hotspot" ? "active" : ""
          }`}
          onClick={() => setActiveTab("hotspot")}
        >
          Hotspots & Alerts
        </button>


        <button
          className={`nav-tab ${
            activeTab === "predict" ? "active" : ""
          }`}
          onClick={() => setActiveTab("predict")}
        >
          Predictive Risk
        </button>

      </nav>


      {/* =====================================================
          MAIN CONTENT
          ===================================================== */}

      <main className="dashboard-main">

        <div className="dashboard-content">


          {/* NETWORK OVERVIEW */}

          {activeTab === "summary" && (
            <NetworkSummary />
          )}


          {/* GRID EXPLORER */}

          {activeTab === "grid" && (
            <GridActivity
              initialGridId={selectedGridId}
            />
          )}


          {/* HOTSPOTS & ALERTS */}

          {activeTab === "hotspot" && (
            <HotspotsAndAlerts />
          )}


          {/* PREDICTIVE RISK */}

          {activeTab === "predict" && (
            <PredictSummary />
          )}


        </div>

      </main>


      {/* =====================================================
          FOOTER
          ===================================================== */}

      <footer className="dashboard-footer">

        Network Operations & Predictive Intelligence

      </footer>


    </div>

  );

}


export default Dashboard;