import React, { useContext, useEffect, useState } from "react";
import GlobalContext from "../templates/GlobalContext";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);
import moment from "moment";

const ProgressChart = () => {
  const { api, client_pan } = useContext(GlobalContext);
  const [chartData, setChartData] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fd = new FormData();
    fd.append("client_pan", client_pan);
    fd.append("portfolio", "Mutual Fund");
    fetch(`${api}/wealth/portfolio-progress`, {
      method: "POST",
      body: fd,
    })
      .then((response) => response.json())
      .then((data) => {
        setChartData({
          labels: data.data.map((item) => moment(item.date).format("YY-MM")),
          datasets: [
            {
              label: "Invested Value",
              data: data.data.map((d) => d.invested_value / 1000000),
              borderColor: "rgba(75, 192, 192, 1)",
              backgroundColor: "rgba(75, 192, 192, 0.2)",
              fill: false,
              borderWidth: 1,
              tension: 0.1,
              pointRadius: 2,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(153, 102, 255, 1)",
              pointHoverBorderColor: "rgba(153, 102, 255, 1)",
            },
            {
              label: "Current Value",
              data: data.data.map((d) => d.current_value / 1000000),
              borderColor: "rgba(153, 102, 255, 1)",
              backgroundColor: "rgba(153, 102, 255, 0.2)",
              fill: false,
              borderWidth: 1,
              tension: 0.1,
              pointRadius: 2,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(255, 159, 64, 1)",
              pointHoverBorderColor: "rgba(255, 159, 64, 1)",
            },
          ],
        });
        setLoading(false);
      });
  }, [api, client_pan]);

  if (loading) {
    return (
      <div className="bg-cyan-950/20 h-[300px] border-cyan-800 border text-gray-300 rounded-lg cursor-pointer">
        <p>Loading</p>
      </div>
    );
  }

  return (
    // <div className="flex flex-col gap-4 h-96 w-full bg-cyan-950/20 border-cyan-800 border text-gray-300 me-4 p-2 rounded-lg mb-4 cursor-pointer">
    <div className="bg-cyan-950/20 h-[300px] border-cyan-800 border text-gray-300 rounded-lg cursor-pointer">
      <Line
        data={chartData}
        style={{ maxWidth: "100%", maxHeight: "100%" }}
        options={{ responsive: true }}
      />
    </div>
  );
};

export default ProgressChart;
