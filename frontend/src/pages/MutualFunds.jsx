import React, { useContext, useEffect } from "react";
import BreadCrumbs from "../../components/BreadCrumbs";
import GlobalContext from "../templates/GlobalContext";

const MutualFunds = () => {
  const { api, setSelectedMenuItem } = useContext(GlobalContext);

  useEffect(() => {
    setSelectedMenuItem("Mutual Funds");
  }, []);
  return (
    <div className="flex flex-col w-[80%] mx-auto h-full min-h-0">
      <BreadCrumbs>Mutual Funds</BreadCrumbs>

      <div className="flex flex-col flex-1 min-h-0 gap-1"></div>
    </div>
  );
};

export default MutualFunds;
