import React, { useContext, useEffect, useState } from "react";
import Loader from "../../components/Loader";
import GlobalContext from "../templates/GlobalContext";
import BreadCrumbs from "../../components/BreadCrumbs";

const Dashboard = () => {
  const [loading, setLoading] = useState(false);
  const { api, setSelectedMenuItem } = useContext(GlobalContext);

  useEffect(() => {
    setSelectedMenuItem("Dashboard");
  }, []);

  return (
    <div className="flex flex-col w-[80%] mx-auto h-full min-h-0">
      <BreadCrumbs>Dashboard</BreadCrumbs>

      <div className="flex flex-col flex-1 min-h-0 gap-1"></div>
    </div>
  );
};

export default Dashboard;
