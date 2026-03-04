import React, { useContext, useEffect, useState } from "react";
import Loader from "../../components/Loader";

const Dashboard = () => {
  const [loading, setLoading] = useState(false);
  return (
    <div className="flex flex-col w-[80%] mx-auto h-full min-h-0">
      <nav className="mb-3">
        <div className="flex flex-row align-middle items-center space-x-2 text-xs uppercase tracking-wide text-gray-500">
          <a href="/" className="hover:text-gray-300 hover:underline underline-offset-4">
            Dashboard
          </a>
        </div>
      </nav>

      <div className="flex flex-col flex-1 min-h-0 gap-1"></div>
    </div>
  );
};

export default Dashboard;
