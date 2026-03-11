"use client";

import React, { useEffect, useState } from "react";

export default function BossDashboard() {
  const [projects, setProjects] = useState<any[]>([]);

  useEffect(() => {
    // 真实环境应当对接: http://127.0.0.1:8501/api/v1/projects/
    fetch("http://127.0.0.1:8000/api/v1/projects/")
      .then((res) => res.json())
      .then((data) => {
        if (data.data) {
          setProjects(data.data);
        }
      })
      .catch((err) => console.error(err));
  }, []);

  return (
    <div className="space-y-6">
      <header className="flex justify-between items-center">
        <h1 className="text-3xl font-bold text-gray-800">项目总览</h1>
        <div className="text-gray-500">今日: {new Date().toLocaleDateString()}</div>
      </header>

      {/* 核心指标卡片区域 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <div className="text-gray-500 text-sm">进行中项目</div>
          <div className="text-3xl font-bold mt-2">{projects.length || 0}</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <div className="text-gray-500 text-sm">本月累计产量</div>
          <div className="text-3xl font-bold mt-2">10.5W</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <div className="text-gray-500 text-sm">平均合格率</div>
          <div className="text-3xl font-bold mt-2 text-green-600">97.2%</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <div className="text-gray-500 text-sm">平均评分 (POC)</div>
          <div className="text-3xl font-bold mt-2 text-blue-600">A+</div>
        </div>
      </div>

      {/* 项目列表区域 */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="p-6 border-b border-gray-100">
          <h2 className="text-xl font-semibold text-gray-800">活跃项目一览</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead className="bg-gray-50 text-gray-600">
              <tr>
                <th className="p-4 font-medium">项目名称</th>
                <th className="p-4 font-medium">负责人</th>
                <th className="p-4 font-medium">录入批次</th>
                <th className="p-4 font-medium">更新时间</th>
                <th className="p-4 font-medium">合格率</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {projects.length > 0 ? (
                projects.map((p, idx) => (
                  <tr key={idx} className="hover:bg-gray-50 transition-colors">
                    <td className="p-4 font-medium">{p.project_name || "未知项目"}</td>
                    <td className="p-4 text-gray-600">{p.poc_name || "-"}</td>
                    <td className="p-4 text-gray-600">{p.run_count}</td>
                    <td className="p-4 text-gray-600">{p.date}</td>
                    <td className="p-4">
                      <span className="px-2 py-1 bg-green-100 text-green-700 rounded-full text-sm">
                        98%
                      </span>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={5} className="p-8 text-center text-gray-400">
                    暂无项目数据，请让管理员先进行数据入库。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
