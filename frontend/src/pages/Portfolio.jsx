import React, { useContext, useEffect, useRef, useState } from "react";
import BreadCrumbs from "../../components/BreadCrumbs";
import GlobalContext from "../templates/GlobalContext";
import numeral from "numeral";
import ProgressChart from "./ProgressChart";
import Loader from "../../components/Loader";
import { data } from "react-router-dom";

const Portfolio = () => {
  const { api, setSelectedMenuItem, client_pan } = useContext(GlobalContext);
  const portfolio = ["All", "Mutual Fund", "Stocks"];
  const [selectedPortfolio, setSelectedPortfolio] = useState("Mutual Fund");
  const [holdings, setHoldings] = useState([]);
  const [refresh, setRefresh] = useState(false);
  const [summary, setSummary] = useState({});
  const [loading, setLoading] = useState(true);
  const [loadingMessage, setLoadingMessage] = useState("");
  const [fvCycles, setFvCycles] = useState(5);
  const [data, setData] = useState([]);
  const [progressData, setProgressData] = useState({});
  const fileInputRef = useRef(null);

  useEffect(() => {
    setLoading(true);
    setLoadingMessage("Loading portfolio data...");
    setSelectedMenuItem("Portfolio");

    const fd = new FormData();
    fd.append("client_pan", client_pan);
    fd.append("portfolio", selectedPortfolio);

    fetch(`${api}/wealth/portfolio`, {
      method: "POST",
      body: fd,
    })
      .then((response) => response.json())
      .then((data) => {
        setHoldings(data.data.holdings);
        setSummary(data.data.summary);
        setProgressData({
          progress: data.data.progress,
          asset_allocation: data.data.asset_allocation,
          progress_ac: data.data.progress_ac,
        });
        setLoading(false);
      });
  }, [refresh, selectedPortfolio]);

  const handleUpload = (event) => {
    const file = event.target.files[0];
    console.log("Selected file:", file);
    const fd = new FormData();
    fd.append("client_pan", client_pan);
    fd.append("file", file);

    fetch(`${api}/mutualfund/cams/upload`, {
      method: "POST",
      body: fd,
    })
      .then((response) => response.json())
      .then((data) => {
        console.log(data);
        setRefresh(!refresh);
      });
  };

  const handleEOD = () => {
    setLoading(true);
    let endpoint = api;
    if (selectedPortfolio === "Stocks") {
      setLoadingMessage("Fetching EOD data for stocks...");
      endpoint = `${api}/stocks/data/eod`;
    } else if (selectedPortfolio === "Mutual Fund") {
      setLoadingMessage("Fetching EOD data for mutual funds...");
      endpoint = `${api}/mutualfund/data/eod`;
    }

    fetch(endpoint)
      .then((response) => response.json())
      .then((data) => {
        setRefresh(!refresh);
      });
  };

  const handleHistoricalData = () => {
    setLoading(true);
    let endpoint = api;
    if (selectedPortfolio === "Stocks") {
      setLoadingMessage("Fetching historical data for stocks...");
      endpoint = `${api}/stocks/data/historical`;
    } else if (selectedPortfolio === "Mutual Fund") {
      setLoadingMessage("Fetching historical data for mutual funds...");
      endpoint = `${api}/mutualfund/data/historical`;
    }
    fetch(endpoint)
      .then((response) => response.json())
      .then((data) => {
        setRefresh(!refresh);
      });
  };

  const changeCycle = () => {
    if (fvCycles === 5) {
      setFvCycles(10);
    } else if (fvCycles === 10) {
      setFvCycles(15);
    } else {
      setFvCycles(5);
    }
  };

  return (
    <div className="flex flex-col w-full px-4 mx-auto h-full min-h-0">
      {loading && <Loader message={loadingMessage} />}
      <input
        type="file"
        ref={fileInputRef}
        accept=".pdf"
        className="hidden"
        onChange={handleUpload}
      />
      <div className="grid grid-cols-2 w-full mb-4">
        <BreadCrumbs>Portfolio - {selectedPortfolio}</BreadCrumbs>
        <span className="flex flex-row pe-4 text-end hover:underline underline-offset-4 justify-end gap-3 items-center ">
          <i
            onClick={() => fileInputRef.current.click()}
            className="bi bi-upload cursor-pointer hover:text-yellow-400"
          ></i>
          <i
            className="bi bi-clock-history cursor-pointer hover:text-yellow-400"
            onClick={handleHistoricalData}
          ></i>
          <i
            className="bi bi-database  cursor-pointer hover:text-yellow-400"
            onClick={handleEOD}
          ></i>
        </span>
      </div>

      <div className="flex flex-col h-full min-h-0">
        <div className="bg-cyan-950/20 border-cyan-800 border text-gray-300 flex flex-col rounded-lg mb-4">
          <div className="p-1 flex flex-row gap-4 border-b border-cyan-800 bg-cyan-700/40">
            <span className="ms-auto"></span>
            {portfolio.map((p) => (
              <span
                onClick={() => setSelectedPortfolio(p)}
                className={`underline-offset-4 font-bold ${selectedPortfolio === p && "underline text-red-400"} hover:text-red-400 cursor-pointer`}
                key={p}
              >
                {p}
              </span>
            ))}
          </div>
          <div className=" text-gray-300 grid grid-cols-5 gap-4 py-2 rounded-lg cursor-pointer">
            <span className="text-center text-lg font-bold text-orange-200">
              Invested
            </span>
            <span className="text-center text-lg font-bold text-orange-200">
              Current Value
            </span>
            <span
              className="text-center text-lg font-bold text-orange-200 underline underline-offset-4 decoration-dotted decoration-yellow-400"
              onClick={changeCycle}
            >
              FV {fvCycles}Y
            </span>
            <span className="text-center text-lg font-bold text-orange-200">
              Total P/L
            </span>
            <span className="text-center text-lg font-bold text-orange-200">
              XIRR
            </span>

            <span className="text-center font-bold text-lg">
              {numeral(summary.holding_value).format("0,0.00")}
            </span>
            <span className="text-center font-bold text-lg">
              {numeral(summary.current_value).format("0,0.00")}
            </span>
            <span className="text-center font-bold text-lg">
              {numeral(summary[`fv_${fvCycles}y`]).format("0,0.00")}
            </span>
            <span className="text-center font-bold text-lg">
              {numeral(summary.pl).format("0,0.00")} (
              {numeral(summary.plp).format("0.00")}%)
            </span>
            <span className="text-center font-bold text-lg">
              {numeral(summary.xirr).format("0.00")}%
            </span>
          </div>
        </div>
        <div className="flex flex-col mb-4 w-full">
          <ProgressChart data={progressData} />
        </div>
        <div className="grid grid-cols-3 gap-4">
          {holdings.length === 0 && (
            <div className="flex items-center justify-center h-full col-span-3">
              <p>No holdings data available</p>
            </div>
          )}
          {holdings.map((holding, index) => (
            <div
              key={index}
              className="grid grid-cols-[2.5fr_1fr]  bg-cyan-950/60 hover:bg-cyan-900 border-cyan-800 border px-4 cursor-pointer py-4 rounded-lg shadow"
            >
              <span className="text-start items-center text-sm overflow-hidden whitespace-nowrap text-ellipsis">
                {holding.instrument_name}
              </span>
              <span
                className={`text-end ${holding.pl < 0 && "text-red-400"} font-bold`}
              >
                {numeral(holding.pl).format("0,0.00")} <br />(
                {numeral(holding.plp).format("0.00")}%)
              </span>

              <div className=" col-span-2 grid grid-cols-4 mt-2 gap-y-1 text-sm">
                <span className="text-center text-orange-200 font-bold">
                  Invested
                </span>
                <span className="text-center text-orange-200 font-bold">
                  Current Value
                </span>
                <span className="text-center text-orange-200 font-bold">
                  Asset Class
                </span>
                <span className="text-center text-orange-200 font-bold">
                  XIRR
                </span>
                <span className="text-center">
                  {numeral(holding.holding_value).format("0,0.00")}
                </span>
                <span className="text-center">
                  {numeral(holding.current_value).format("0,0.00")}
                </span>
                <span className="text-center">{holding.asset_class}</span>
                <span className="text-center">
                  {numeral(holding.xirr).format("0.00")}%
                </span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default Portfolio;
