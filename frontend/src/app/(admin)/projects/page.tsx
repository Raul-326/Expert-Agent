"use client";

import React from "react";

export default function AdminProjectsPage() {
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold text-indigo-900">项目数据核校</h1>
        <p className="text-slate-500 mt-1">查看已入库的项目，并对准确率等指标进行人工修订。</p>
      </header>

      <div className="bg-white rounded-xl shadow-lg border border-indigo-50 p-8 text-center text-gray-400">
        <div className="text-5xl mb-4">🛠️</div>
        <p className="text-lg">项目核校台即将上线</p>
        <p className="text-sm mt-2">你将在这里对 Agent 计算结果进行人工干预和分数覆盖</p>
      </div>
    </div>
  );
}
