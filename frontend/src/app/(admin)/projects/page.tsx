"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { 
  Settings2, 
  Trash2, 
  Eye, 
  Search, 
  Database,
  RefreshCw,
  Loader2,
  CheckCircle2,
  AlertTriangle
} from "lucide-react";
import { API_BASE_URL } from "@/lib/constants";

export default function AdminProjectsPage() {
  const [projects, setProjects] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");

  const fetchProjects = () => {
    setLoading(true);
    fetch(`${API_BASE_URL}/api/v1/projects/`)
      .then((res) => res.json())
      .then((data) => {
        if (data.data) setProjects(data.data);
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchProjects();
  }, []);

  const filteredProjects = projects.filter(p => 
    p.project_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="max-w-7xl mx-auto space-y-8 animate-in fade-in duration-500">
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-black text-slate-900 tracking-tight">项目资产管理</h1>
          <p className="text-slate-500 mt-1">管理、核校、或删除已入库的项目资产记录</p>
        </div>
        <button 
          onClick={fetchProjects}
          className="flex items-center space-x-2 px-4 py-2 bg-white border border-slate-200 rounded-xl hover:bg-slate-50 transition-colors font-bold text-sm text-slate-600 shadow-sm"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          <span>刷新数据</span>
        </button>
      </header>

      {/* 工具栏 */}
      <div className="flex flex-col md:flex-row gap-4 items-center justify-between bg-white p-4 rounded-2xl shadow-sm border border-slate-100">
        <div className="relative flex-1 w-full">
          <Search className="w-5 h-5 absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" />
          <input 
            type="text" 
            placeholder="搜索项目 ID 或名称..." 
            className="w-full pl-12 pr-4 py-2.5 bg-slate-50 rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 transition-all text-sm"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <div className="flex items-center space-x-2 w-full md:w-auto">
          <select className="bg-slate-50 border-none rounded-xl px-4 py-2.5 text-sm font-bold text-slate-600 outline-none focus:ring-2 focus:ring-indigo-500">
            <option>所有状态</option>
            <option>已完成</option>
            <option>进行中</option>
          </select>
        </div>
      </div>

      {/* 项目列表 */}
      <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100 overflow-hidden">
        {loading && projects.length === 0 ? (
          <div className="py-20 flex flex-col items-center justify-center text-slate-400">
            <Loader2 className="w-10 h-10 animate-spin mb-4" />
            <p className="font-bold">正在拉取最新项目快照...</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="bg-slate-50/50 text-slate-500 text-[10px] font-black uppercase tracking-widest border-b border-slate-50">
                  <th className="px-6 py-4">项目基本信息</th>
                  <th className="px-6 py-4">健康度</th>
                  <th className="px-6 py-4 text-center">样本规模</th>
                  <th className="px-6 py-4 text-center">状态</th>
                  <th className="px-6 py-4 text-right">管理操作</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50">
                {filteredProjects.map((p) => (
                  <tr key={p.project_group_id} className="hover:bg-slate-50/50 transition-colors group">
                    <td className="px-6 py-5">
                      <div className="flex items-center space-x-4">
                        <div className="w-10 h-10 rounded-lg bg-indigo-50 text-indigo-600 flex items-center justify-center shrink-0">
                          <Database className="w-5 h-5" />
                        </div>
                        <div>
                          <div className="font-bold text-slate-900 leading-tight">
                            {p.project_name}
                          </div>
                          <div className="text-[10px] text-slate-400 font-bold mt-1 flex items-center uppercase tracking-tighter">
                            ID: PROJ-{p.project_group_id} • POC: {p.poc_name}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-5">
                      <div className="flex items-center space-x-2">
                        <span className={`text-sm font-black ${p.overall_accuracy > 0.9 ? 'text-emerald-500' : 'text-amber-500'}`}>
                          {(p.overall_accuracy * 100).toFixed(1)}%
                        </span>
                        {p.overall_accuracy > 0.9 ? <CheckCircle2 className="w-4 h-4 text-emerald-400" /> : <AlertTriangle className="w-4 h-4 text-amber-400" />}
                      </div>
                    </td>
                    <td className="px-6 py-5 text-center font-mono text-sm text-slate-500">
                      {p.total_volume.toLocaleString()}
                    </td>
                    <td className="px-6 py-5 text-center">
                      <span className="px-2 py-1 bg-green-50 text-green-600 text-[10px] font-black rounded-md uppercase">
                        ACTIVE
                      </span>
                    </td>
                    <td className="px-6 py-5">
                      <div className="flex items-center justify-end space-x-2">
                        <Link 
                          href={`/dashboard/${p.project_group_id}`}
                          className="p-2 hover:bg-white hover:text-indigo-600 text-slate-400 rounded-lg transition-all"
                          title="查看大盘展示"
                        >
                          <Eye className="w-4 h-4" />
                        </Link>
                        <button 
                          className="p-2 hover:bg-white hover:text-amber-600 text-slate-400 rounded-lg transition-all"
                          title="数据核校"
                        >
                          <Settings2 className="w-4 h-4" />
                        </button>
                        <button 
                          className="p-2 hover:bg-white hover:text-rose-600 text-slate-400 rounded-lg transition-all"
                          title="删除项目"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}

                {filteredProjects.length === 0 && !loading && (
                   <tr>
                     <td colSpan={5} className="py-20 text-center text-slate-400">
                       <Database className="w-12 h-12 mx-auto mb-4 opacity-10" />
                       <p className="font-bold">暂无已入库的项目资产</p>
                       <p className="text-xs uppercase tracking-widest mt-1">NO PROJECT ASSETS FOUND</p>
                       <Link href="/workspace" className="mt-6 inline-block px-6 py-2 bg-indigo-600 text-white rounded-xl font-bold text-sm shadow-lg shadow-indigo-100">
                         去开启第一个任务
                       </Link>
                     </td>
                   </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
