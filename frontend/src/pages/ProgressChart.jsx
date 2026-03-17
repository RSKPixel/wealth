import React, { useContext, useEffect, useState } from "react";
import GlobalContext from "../templates/GlobalContext";
import { Line, Doughnut, PolarArea } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  RadialLinearScale,
} from "chart.js";

ChartJS.register(
  RadialLinearScale,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
);

import moment from "moment";

const ProgressChart = ({ selectedPortfolio }) => {
  const { api, client_pan } = useContext(GlobalContext);
  const [chartData, setChartData] = useState({});
  const [assetAllocationData, setAssetAllocationData] = useState({});
  // const [assetClassData, setAssetClassData] = useState({});
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const charts = [
    "Equity Curve",
    "Investment Progress",
    "Drawdown",
    "Holding %",
    "Returns %",
  ];
  const period = {
    "1M": 1,
    "3M": 3,
    "6M": 6,
    "1Y": 12,
    "3Y": 36,
    "5Y": 60,
    All: 999,
  };

  const [selectedChart, setSelectedChart] = useState("Equity Curve");
  const [selectedPeriod, setSelectedPeriod] = useState("5Y");

  useEffect(() => {
    const fd = new FormData();
    fd.append("client_pan", client_pan);
    fd.append("portfolio", selectedPortfolio);
    fetch(`${api}/wealth/portfolio-progress`, {
      method: "POST",
      body: fd,
    })
      .then((response) => response.json())
      .then((data) => {
        setData(data.data);
      });
  }, [api, client_pan, selectedPortfolio]);

  useEffect(() => {
    if (data.length === 0) return;

    const filteredData = data.progress.slice(
      -period[selectedPeriod] || data.length,
    );
    const assetClassData = data.asset_allocation;
    const progressAc = data.progress_ac;

    const lables = filteredData.map((item, index, arr) => {
      const m = moment(item.date);
      const prev = index > 0 ? moment(arr[index - 1].date) : null;

      if (index === 0 || m.year() !== prev.year()) {
        return m.format("YYYY-MM");
      }

      return m.format("MM");
    });

    let cd = {};
    if (selectedChart === "Investment Progress") {
      cd = {
        labels: lables,
        datasets: [
          {
            label: "Invested Value",
            data: filteredData.map((d) => d.invested_value / 1000000),
            borderColor: "rgba(75, 192, 192, 1)",
            backgroundColor: "rgba(75, 192, 192, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(153, 102, 255, 1)",
            pointHoverBorderColor: "rgba(153, 102, 255, 1)",
          },
          {
            label: "Current Value",
            data: filteredData.map((d) => d.current_value / 1000000),
            borderColor: "rgba(153, 102, 255, 1)",
            backgroundColor: "rgba(153, 102, 255, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(255, 159, 64, 1)",
            pointHoverBorderColor: "rgba(255, 159, 64, 1)",
          },
        ],
      };
    } else if (selectedChart === "Drawdown") {
      cd = {
        labels: lables,
        datasets: [
          {
            label: "Drawdown",
            data: filteredData.map((d) => d.drawdown),
            borderColor: "rgba(255, 99, 132, 1)",
            backgroundColor: "rgba(255, 99, 132, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(255, 206, 86, 1)",
            pointHoverBorderColor: "rgba(255, 206, 86, 1)",
          },
        ],
      };
    } else if (selectedChart === "Returns %") {
      cd = {
        labels: lables,
        datasets: [
          {
            label: "Returns %",
            data: filteredData.map((d) => d.plp),
            borderColor: "rgba(54, 162, 235, 1)",
            backgroundColor: "rgba(54, 162, 235, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(255, 206, 86, 1)",
            pointHoverBorderColor: "rgba(255, 206, 86, 1)",
          },
        ],
      };
    } else if (selectedChart === "Equity Curve") {
      cd = {
        labels: lables,
        datasets: [
          {
            label: "Equity Curve",
            data: filteredData.map(
              (d) => (d.current_value - d.invested_value) / 1000000,
            ),
            borderColor: "rgba(255, 159, 64, 1)",
            backgroundColor: "rgba(255, 159, 64, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(255, 99, 132, 1)",
            pointHoverBorderColor: "rgba(255, 99, 132, 1)",
          },
        ],
      };
    } else if (selectedChart === "Holding %") {
      cd = {
        labels: lables,
        datasets: [
          {
            label: "Holding %",
            data: progressAc.map((d) => d.cv_percentage),
            borderColor: "rgba(255, 206, 86, 1)",
            backgroundColor: "rgba(255, 206, 86, 0.2)",
            fill: false,
            borderWidth: 1,
            tension: 0.1,
            pointRadius: 1,
            pointHoverRadius: 10,
            pointHoverBackgroundColor: "rgba(54, 162, 235, 1)",
            pointHoverBorderColor: "rgba(54, 162, 235, 1)",
          },
        ],
      };
    }

    const ac = {
      labels: assetClassData.map((d) => `${d.asset_class} (${d.cvp}%)`),
      datasets: [
        {
          label: "Asset Allocation",
          data: assetClassData.map((d) => d.cvp),
          backgroundColor: [
            "#3b82f6", // Equity
            "#10b981", // Debt
            "#f59e0b", // Gold
          ],
          borderColor: ["#2563eb", "#059669", "#d97706"],
          borderWidth: 1,
        },
      ],
    };

    console.log(ac);
    setAssetAllocationData(ac);
    setChartData(cd);
    setLoading(false);
  }, [data, selectedPeriod, selectedChart]);

  if (loading) {
    return (
      <div className="bg-cyan-950/20 h-96 flex flex-col items-center justify-center border-cyan-800 border text-gray-300 rounded-lg cursor-pointer">
        <p>Loading Chart...</p>
        <br />
        <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-cyan-900"></div>
      </div>
    );
  }

  return (
    <div className="flex flex-col bg-cyan-950/20 border-cyan-800 border text-gray-300 rounded-lg cursor-pointer">
      <div className="flex flex-row px-2 py-1 border-b bg-cyan-800/40 border-cyan-800 items-center gap-4">
        {charts.map((chart) => (
          <span
            className={`hover:bg-yellow-950 p-1 rounded-sm text-sm font-bold ${selectedChart === chart ? "bg-yellow-950" : ""}`}
            key={chart}
            onClick={() => setSelectedChart(chart)}
          >
            {chart}
          </span>
        ))}
        <span className="ms-auto"></span>
        {Object.keys(period).map((p) => (
          <span
            className={`hover:bg-yellow-950 p-1 rounded-sm text-sm font-bold ${selectedPeriod === p ? "bg-yellow-950" : ""}`}
            key={p}
            onClick={() => setSelectedPeriod(p)}
          >
            {p}
          </span>
        ))}
      </div>
      <div className="h-96 grid grid-cols-2 gap-4">
        <div className="border-r border-cyan-900">
          {chartData && (
            <Line
              data={chartData}
              style={{ maxWidth: "100%", maxHeight: "100%" }}
              options={{ responsive: true, maintainAspectRatio: false }}
            />
          )}
        </div>
        <div className="pb-4">
          {assetAllocationData && (
            <Doughnut
              data={assetAllocationData}
              style={{ maxWidth: "100%", maxHeight: "100%" }}
              options={{ responsive: true, maintainAspectRatio: false }}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default ProgressChart;
