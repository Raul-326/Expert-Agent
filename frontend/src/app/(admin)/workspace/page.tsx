"use client";

import React, { useState } from "react";
import { API_BASE_URL } from "@/lib/constants";

type JobStatus = "idle" | "accepted" | "error";

export default function AdminWorkspace() {
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [projectName, setProjectName] = useState("");
  const [pocName, setPocName] = useState("");
  const [loading, setLoading] = useState(false);
  const [jobStatus, setJobStatus] = useState<JobStatus>("idle");
  const [statusMessage, setStatusMessage] = useState("");

  const handleCompute = async () => {
    if (!url || !token) {
      setJobStatus("error");
      setStatusMessage("请填写作业表 URL 和飞书 Token，两者均为必填项。");
      return;
    }

    setLoading(true);
    setJobStatus("idle");
    setStatusMessage("");

    try {
      const response = await fetch(`${API_BASE_URL}/api/v1/jobs/compute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_url: url,
          user_access_token: token,
          project_group_name: projectName || "Default Project",
          poc_name: pocName || undefined,
        }),
      });

      const data = await response.json();

      if (response.ok) {
        setJobStatus("accepted");
        setStatusMessage(`✅ 多 Agent 流水线已接受任务，正在后台处理中。项目：${data.project}`);
      } else {
        setJobStatus("error");
        setStatusMessage(`❌ 出错: ${data.detail || "请求失败，请检查后端日志"}`);
      }
    } catch (error) {
      setJobStatus("error");
      setStatusMessage("❌ 无法连接到后端 API。请确认 FastAPI 已在 Port 8000 运行。\n提示：在终端运行 bash deploy/start_dev.sh");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-8 max-w-4xl mx-auto">
      <header className="border-b border-indigo-100 pb-4">
        <h1 className="text-3xl font-bold text-indigo-900">执行入库作业 (Agent 流水线)</h1>
        <p className="text-slate-500 mt-2">
          填写飞书表格地址和 Token，多 Agent 中枢将自动执行读取→评价→入库全流程。
          <br/>
          <span className="text-xs text-slate-400">支持格式：<code>sheets/xxx</code> 或 <code>wiki/xxx?sheet=yyy</code></span>
        </p>
      </header>

      {/* 状态反馈区 */}
      {jobStatus === "accepted" && (
        <div className="bg-green-50 border border-green-200 text-green-800 rounded-xl p-4 flex items-start space-x-3">
          <span className="text-2xl">✅</span>
          <div>
            <p className="font-semibold">任务已下发</p>
            <p className="text-sm mt-1">{statusMessage}</p>
            <p className="text-xs text-green-600 mt-1">可在后端终端日志中查看实时处理进度（Agent Pipeline Execution Finished）</p>
          </div>
        </div>
      )}
      {jobStatus === "error" && (
        <div className="bg-red-50 border border-red-200 text-red-800 rounded-xl p-4 flex items-start space-x-3">
          <span className="text-2xl">⚠️</span>
          <div>
            <p className="font-semibold">请求未成功</p>
            <p className="text-sm mt-1 whitespace-pre-line">{statusMessage}</p>
          </div>
        </div>
      )}

      <div className="bg-white p-8 rounded-xl shadow-lg border border-indigo-50">
        <form className="space-y-6" onSubmit={(e) => { e.preventDefault(); handleCompute(); }}>
          
          {/* URL */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              作业表格 URL <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              required
              placeholder="https://bytedance.larkoffice.com/wiki/xxx?sheet=yyy  或  /sheets/xxx"
              className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow font-mono text-sm"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
            />
          </div>

          {/* Token + 项目名 */}
          <div className="grid grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                飞书 Auth Token <span className="text-red-500">*</span>
              </label>
              <input
                type="password"
                required
                placeholder="u-xxxxxxxxxxxxxxxxx"
                className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
                value={token}
                onChange={(e) => setToken(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">项目名称</label>
              <input
                type="text"
                placeholder="e.g. DL-052 第三批"
                className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
              />
            </div>
          </div>

          {/* POC 负责人 + 策略 */}
          <div className="grid grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">POC 负责人（可选）</label>
              <input
                type="text"
                placeholder="e.g. 张三"
                className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
                value={pocName}
                onChange={(e) => setPocName(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Agent 策略</label>
              <select className="w-full border border-slate-300 rounded-lg p-3 outline-none focus:ring-2 focus:ring-indigo-500 bg-white">
                <option>快速验证 (单轮)</option>
                <option>深度分析 (多模型对抗)</option>
              </select>
            </div>
          </div>

          <div className="pt-4 flex justify-end space-x-4">
            <button
              type="button"
              onClick={() => { setUrl(""); setToken(""); setProjectName(""); setPocName(""); setJobStatus("idle"); }}
              className="px-6 py-2 border border-slate-300 text-slate-700 rounded-lg hover:bg-slate-50 transition-colors"
            >
              清空
            </button>
            <button
              type="submit"
              className={`px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-medium rounded-lg shadow-sm transition-colors flex items-center ${loading ? 'opacity-70 cursor-not-allowed' : ''}`}
              disabled={loading}
            >
              {loading && <span className="animate-spin mr-2 border-2 border-white border-t-transparent rounded-full w-4 h-4 inline-block" />}
              {loading ? "Agent 处理中..." : "🚀 交给 Agent 执行"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
