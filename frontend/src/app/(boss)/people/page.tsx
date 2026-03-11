"use client";

import React from "react";

export default function BossPeoplePage() {
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold text-gray-800">人员效能</h1>
        <p className="text-gray-500 mt-1">查看所有标注员的产量与准确率表现。</p>
      </header>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-8 text-center text-gray-400">
        <div className="text-5xl mb-4">👥</div>
        <p className="text-lg">人员总览数据即将上线</p>
        <p className="text-sm mt-2">数据将从 FastAPI 后端实时拉取并展示人员产量趋势图</p>
      </div>
    </div>
  );
}
