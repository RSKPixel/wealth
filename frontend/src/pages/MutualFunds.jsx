import React, { useContext, useEffect, useState } from "react";
import BreadCrumbs from "../../components/BreadCrumbs";
import GlobalContext from "../templates/GlobalContext";
import numeral from "numeral";

const MutualFunds = () => {
  const { api, setSelectedMenuItem, client_pan } = useContext(GlobalContext);
  const [holdings, setHoldings] = useState([]);
  const [summary, setSummary] = useState({});
  const [loading, setLoading] = useState(true);

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
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-cyan-900"></div>
      </div>
    );
  }

  return (
    <div className="flex flex-col w-full px-4 mx-auto h-full min-h-0">
      <BreadCrumbs>Mutual Funds</BreadCrumbs>

      <div className="flex flex-col h-full overflow-auto">
        <div className="bg-cyan-950/20 border-cyan-800 border text-gray-300 grid grid-cols-4 gap-x-4 gap-y-4 me-4 p-2 rounded-lg mb-4 cursor-pointer">
          <span className="text-center text-lg font-bold text-orange-200">
            Invested
          </span>
          <span className="text-center text-lg font-bold text-orange-200">
            Current Value
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
            {numeral(summary.pl).format("0,0.00")} (
            {numeral(summary.plp).format("0.00")}%)
          </span>
          <span className="text-center font-bold text-lg">
            {numeral(summary.xirr).format("0.00")}%
          </span>
        </div>
        <div className="grid grid-cols-3 gap-4 pe-4">
          {holdings.map((holding, index) => (
            <div
              key={index}
              className="grid grid-cols-[2.5fr_1fr]  bg-cyan-950/60 hover:bg-cyan-900 border-cyan-800 border px-4 cursor-pointer py-4 rounded-lg shadow"
            >
              <span className="text-start text-base overflow-hidden whitespace-nowrap text-ellipsis">
                {holding.instrument_name}
              </span>
              <span
                className={`text-end ${holding.pl < 0 && "text-red-400"} font-bold`}
              >
                {numeral(holding.pl).format("0,0.00")} (
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
