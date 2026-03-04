import React from "react";

const Loader = ({ message }) => {
  return (
    <div className="fixed inset-0 flex flex-col items-center justify-center bg-black/1 backdrop-blur-xs z-50">
      <div className="w-64 h-2 bg-gray-700 rounded-full overflow-hidden">
        <div className="h-2 bg-linear-to-r from-blue-400 to-blue-600 animate-[progress_1.2s_linear_infinite]"></div>
      </div>
      <div className="text-stone-200 text-lg mt-4 animate-pulse">{message || "Loading ..."}</div>
      <style>{`
      @keyframes progress {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(100%); }
      }
    `}</style>
    </div>
  );
};

export default Loader;
