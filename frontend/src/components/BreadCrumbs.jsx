import React from "react";

const BreadCrumbs = ({ children }) => {
  return (
    <nav className="flex flex-row items-center">
      <div className="flex flex-row align-middle items-center space-x-2 text-xs uppercase tracking-wide text-gray-500">
        <a href="/" className="hover:text-gray-300 hover:underline underline-offset-4">
          {children}
        </a>
      </div>
    </nav>
  );
};

export default BreadCrumbs;
