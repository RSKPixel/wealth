import { useContext, useEffect, useState } from "react";
import "./App.css";

import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import GlobalContext from "./templates/GlobalContext";
import Basetemplate from "./templates/Basetemplate";
import Dashboard from "./pages/Dashboard";
import MutualFunds from "./pages/MutualFunds";

function App() {
  const api = import.meta.env.VITE_API;
  const client_pan = "ARTPS2520D";
  const [selectedMenuItem, setSelectedMenuItem] = useState("dashboard");
  const [loggedIn, setLoggedIn] = useState(true);

  const globalContextValue = {
    api,
    client_pan,
    setLoggedIn,
    loggedIn,
    setSelectedMenuItem,
    selectedMenuItem,
  };

  useEffect(() => {
    document.title = "Wealth Framework";
  }, []);

  return (
    <Router>
      <GlobalContext.Provider value={globalContextValue}>
        <Basetemplate>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/mutualfunds" element={<MutualFunds />} />
          </Routes>
        </Basetemplate>
      </GlobalContext.Provider>
    </Router>
  );
}

export default App;
