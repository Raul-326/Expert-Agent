"use client";

import React, { useEffect, useState } from "react";
import { API_BASE_URL } from "@/lib/constants";

export default function AuditLogsPage() {
  const [logs, setLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/admin/audit_logs`)
      .then((res) => res.json())
      .then((data) => {
        if (Array.isArray(data)) setLogs(data);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold text-indigo-900">操作审计日志</h1>
        <p className="text-slate-500 mt-1">追踪所有人工修订、数据覆盖、 Agent 触发的完整记录。</p>
      </header>

      <div className="bg-white rounded-xl shadow-sm border border-indigo-50 overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400 animate-pulse">正在加载审计日志...</div>
        ) : logs.length > 0 ? (
          <table className="w-full text-left text-sm">
            <thead className="bg-indigo-50 text-indigo-700">
              <tr>
                <th className="p-4 font-medium">时间</th>
                <th className="p-4 font-medium">操作人</th>
                <th className="p-4 font-medium">动作</th>
                <th className="p-4 font-medium">表</th>
                <th className="p-4 font-medium">原因</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {logs.map((log, i) => (
                <tr key={i} className="hover:bg-gray-50">
                  <td className="p-4 text-gray-500">{log.created_at || "-"}</td>
                  <td className="p-4 font-medium">{log.operator}</td>
                  <td className="p-4">
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${log.action === "UPDATE" ? "bg-yellow-100 text-yellow-700" : "bg-green-100 text-green-700"}`}>
                      {log.action}
                    </span>
                  </td>
                  <td className="p-4 text-gray-500">{log.table_name}</td>
                  <td className="p-4 text-gray-500">{log.reason || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="p-8 text-center text-gray-400">
            <div className="text-4xl mb-4">📜</div>
            <p>暂无审计日志记录</p>
          </div>
        )}
      </div>
    </div>
  );
}
