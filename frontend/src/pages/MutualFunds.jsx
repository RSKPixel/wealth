import React, { useContext, useEffect, useState } from "react";
import BreadCrumbs from "../../components/BreadCrumbs";
import GlobalContext from "../templates/GlobalContext";
import numeral from "numeral";

const MutualFunds = () => {
  const { api, setSelectedMenuItem, client_pan } = useContext(GlobalContext);
  const [holdings, setHoldings] = useState([]);
  const [summary, setSummary] = useState({});

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
      });
  }, []);
  return (
    <div className="flex flex-col w-full px-4 mx-auto h-full min-h-0">
      <BreadCrumbs>Mutual Funds</BreadCrumbs>

      <div className="flex flex-col h-full overflow-scroll">
        <div className="bg-teal-950 border-teal-800 border text-gray-300 grid grid-cols-4 gap-x-4 gap-y-2 me-4 p-2 rounded-lg mb-4 cursor-pointer">
          <span className="text-center text-base underline underline-offset-4">
            Invested
          </span>
          <span className="text-center text-base underline underline-offset-4">
            Current Value
          </span>
          <span className="text-center text-base underline underline-offset-4">
            Total P/L
          </span>
          <span className="text-center text-base underline underline-offset-4">
            XIRR
          </span>

          <span className="text-center font-bold text-base">
            {numeral(summary.holding_value).format("0,0.00")}
          </span>
          <span className="text-center font-bold text-base">
            {numeral(summary.current_value).format("0,0.00")}
          </span>
          <span className="text-center font-bold text-base">
            {numeral(summary.pl).format("0,0.00")} (
            {numeral(summary.plp).format("0.00")}%)
          </span>
          <span className="text-center font-bold text-base">
            {numeral(summary.xirr).format("0.00")}%
          </span>
        </div>
        <div className="grid grid-cols-3 gap-4 pe-4">
          {holdings.map((holding, index) => (
            <div
              key={index}
              className="grid grid-cols-[2.5fr_1fr] bg-teal-950 hover:bg-teal-900 border-teal-800 border px-4 cursor-pointer py-4 rounded-lg shadow"
            >
              <span className="text-start text-base overflow-hidden whitespace-nowrap text-ellipsis">
                {holding.instrument_name}
              </span>
              <span className="text-end font-bold text-base">
                {numeral(holding.current_value).format("0,0.00")}
              </span>

              <div className=" col-span-2 grid grid-cols-3 mt-4 gap-y-1 text-sm">
                <span className="text-center text-gray-400">Invested</span>
                <span className="text-center text-gray-400">Total P/L</span>
                <span className="text-center text-gray-400">XIRR</span>
                <span className="text-center">
                  {numeral(holding.holding_value).format("0,0.00")}
                </span>
                <span className="text-center">
                  {numeral(holding.pl).format("0,0.00")} (
                  {numeral(holding.plp).format("0.00")}%)
                </span>
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
