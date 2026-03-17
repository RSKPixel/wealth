import React from "react";

const Loader = ({ message }) => {
  return (
    <div className="fixed inset-0 flex flex-col items-center justify-center bg-black/60 backdrop-blur-xs z-50">
      <div className="flex flex-col gap-4 items-center justify-center h-full">
        <span>{message}</span>
        <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-cyan-900"></div>
      </div>
    </div>
  );
};

export default Loader;
