import React, { Children, useContext } from "react";
import { Link } from "react-router-dom";
import GlobalContext from "./GlobalContext";

const Basetemplate = ({ children }) => {
  const { api, selectedMenuItem, loggedIn } = useContext(GlobalContext);
  const menuItems = [
    { name: "Dashboard", link: "/" },
    { name: "Mutual Funds", link: "/mutualfunds" },
    { name: "Stocks", link: "/stocks" },
    { name: "Data", link: "/data" },
    { name: "User", link: "/client-master" },
  ];
  return (
    <div className="flex flex-col text-sm tracking-wide uppercase h-screen">
      <nav className="bg-teal-950 border-b border-teal-600 px-4 py-3 w-full shrink-0 ">
        <div className="flex flex-row mx-auto justify-between items-center">
          <div className="font-bold text-white cursor-pointer">Wealth Framework</div>
          <div className="flex flex-row gap-4">
            {menuItems.map((item) => (
              <Link key={item.name} to={item.link} className={`font-bold hover:text-red-500 underline-offset-4 ${selectedMenuItem == item.name ? "underline text-red-500" : "text-white"}`}>
                {item.name}
              </Link>
            ))}
            {loggedIn && (
              <Link to="/logout" className="text-yellow-400 font-semibold hover:text-yellow-500">
                Logout
              </Link>
            )}
          </div>
        </div>
      </nav>

      <main className="flex-1 min-h-0 overflow-hidden p-4">{children}</main>
    </div>
  );
};

export default Basetemplate;
