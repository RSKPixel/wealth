import React, { useContext, useEffect, useRef, useState } from "react";
import BreadCrumbs from "../../components/BreadCrumbs";
import GlobalContext from "../templates/GlobalContext";
import numeral from "numeral";
import ProgressChart from "./ProgressChart";

const MutualFunds = () => {
  const { api, setSelectedMenuItem, client_pan } = useContext(GlobalContext);
  const [holdings, setHoldings] = useState([]);
  const [refresh, setRefresh] = useState(false);
  const [summary, setSummary] = useState({});
  const [loading, setLoading] = useState(true);
  const [fvCycles, setFvCycles] = useState(5);
  const fileInputRef = useRef(null);

  useEffect(() => {
    setSelectedMenuItem("Mutual Funds");

    const fd = new FormData();
    fd.append("client_pan", client_pan);
    fd.append("portfolio", "Mutual Fund");

    fetch(`${api}/wealth/portfolio`, {
      method: "POST",
      body: fd,
    })
      .then((response) => response.json())
      .then((data) => {
        setHoldings(data.data.holdings);
        setSummary(data.data.summary);
        setLoading(false);
      });
  }, [refresh]);

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
    fetch(`${api}/mutualfund/data/eod`)
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

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-cyan-900"></div>
      </div>
    );
  }

  return (
    <div className="flex flex-col w-full px-4 mx-auto h-full min-h-0">
      <input
        type="file"
        ref={fileInputRef}
        accept=".pdf"
        className="hidden"
        onChange={handleUpload}
      />
      <div className="grid grid-cols-2 w-full mb-4">
        <BreadCrumbs>Mutual Funds</BreadCrumbs>
        <span className="flex flex-row pe-4 text-end hover:underline underline-offset-4 justify-end gap-3 items-center ">
          <i
            onClick={() => fileInputRef.current.click()}
            className="bi bi-upload cursor-pointer hover:text-yellow-400"
          ></i>
          <i
            className="bi bi-database  cursor-pointer hover:text-yellow-400"
            onClick={handleEOD}
          ></i>
        </span>
      </div>

      <div className="flex flex-col h-full overflow-auto">
        <div className="bg-cyan-950/20 border-cyan-800 border text-gray-300 grid grid-cols-5 gap-x-4 gap-y-4 me-4 p-2 rounded-lg mb-4 cursor-pointer">
          <span className="text-center text-lg font-bold text-orange-200">
            Invested
          </span>
          <span className="text-center text-lg font-bold text-orange-200">
            Current Value
          </span>
          <span
            className="text-center text-lg font-bold text-orange-200"
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
        <div className="flex flex-col pe-4 mb-4 w-full">
          <ProgressChart />
        </div>
        <div className="grid grid-cols-3 gap-4 pe-4">
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

export default MutualFunds;
