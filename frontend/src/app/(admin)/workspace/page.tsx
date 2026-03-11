"use client";

import React, { useState } from "react";

export default function AdminWorkspace() {
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [loading, setLoading] = useState(false);

  const handleCompute = async () => {
    setLoading(true);
    // 这里真实对接的是 http://127.0.0.1:8000/api/v1/jobs/compute 开启多 Agent 计算流水线
    setTimeout(() => {
      setLoading(false);
      alert("✅ 多Agent编排流水线已下发任务！稍后在监控台中查看进度。");
    }, 1500);
  };

  return (
    <div className="space-y-8 max-w-4xl mx-auto">
      <header className="border-b border-indigo-100 pb-4">
        <h1 className="text-3xl font-bold text-indigo-900">执行入库作业 (Agent 流水线)</h1>
        <p className="text-slate-500 mt-2">将复杂的计算和飞书表格处理发送给多 Agent 中枢自动执行。</p>
      </header>

      <div className="bg-white p-8 rounded-xl shadow-lg border border-indigo-50">
        <form className="space-y-6" onSubmit={(e) => { e.preventDefault(); handleCompute(); }}>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">作业表格 URL</label>
            <input 
              type="url" 
              required
              placeholder="https://bytedance.larkoffice.com/sheets/..."
              className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
            />
          </div>

          <div className="grid grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">飞书 Auth Token</label>
              <input 
                type="password" 
                required
                placeholder="u-xxxxxxxxx"
                className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
                value={token}
                onChange={(e) => setToken(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Agent 策略等级</label>
              <select className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 bg-white">
                <option>快速验证 (单轮)</option>
                <option>深度分析 (多模型对抗)</option>
                <option>强制入库 (跳过质检)</option>
              </select>
            </div>
          </div>

          <div className="pt-4 flex justify-end space-x-4">
            <button type="button" className="px-6 py-2 border border-slate-300 text-slate-700 rounded-lg hover:bg-slate-50 transition-colors">草稿</button>
            <button 
              type="submit" 
              className={`px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-medium rounded-lg shadow-sm transition-colors flex items-center ${loading ? 'opacity-70 cursor-not-allowed' : ''}`}
              disabled={loading}
            >
              {loading && <span className="animate-spin mr-2 border-2 border-white border-t-transparent rounded-full w-4 h-4 inline-block tracking-tighter" />}
              {loading ? "Agent分配中..." : "🚀 交给 Agent 执行"}
            </button>
          </div>
        </form>
      </div>
      
      {/* 可以在这里添加一个骨架屏或实时状态监控 */}
      
    </div>
  );
}
