"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import {
  BarChart3,
  Users,
  Database,
  TrendingUp,
  Search,
  ArrowRight,
  ExternalLink,
  Loader2
} from "lucide-react";
import { API_BASE_URL } from "@/lib/constants";

export default function BossDashboard() {
  const [projects, setProjects] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/projects/`)
      .then((res) => res.json())
      .then((data) => {
        if (data.data) {
          setProjects(data.data);
        }
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, []);

  const filteredProjects = projects.filter(p =>
    p.project_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // 计算大盘汇总
  const totalVolume = projects.reduce((acc, p) => acc + (p.total_volume || 0), 0);
  const avgAccuracy = projects.length > 0
    ? projects.reduce((acc, p) => acc + (p.overall_accuracy || 0), 0) / projects.length
    : 0;

  if (loading) {
    return (
      <div className="flex h-96 items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-indigo-600" />
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto space-y-8 animate-in fade-in duration-500">
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">Dola 标评数据看板</h1>
          <p className="text-slate-500 mt-2">实时监控项目产出、合格率与人员效能</p>
        </div>
        <div className="flex items-center space-x-2 text-sm text-slate-400 bg-white px-4 py-2 rounded-full border border-slate-100 shadow-sm">
          <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
          <span>系统运行中 • {new Date().toLocaleDateString()}</span>
        </div>
      </header>

      {/* 核心指标卡片区域 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <MetricCard
          title="活跃项目数"
          value={projects.length}
          icon={<Database className="w-6 h-6 text-indigo-600" />}
          color="indigo"
        />
        <MetricCard
          title="累计总产量"
          value={(totalVolume / 10000).toFixed(1) + "W"}
          icon={<TrendingUp className="w-6 h-6 text-emerald-600" />}
          color="emerald"
        />
        <MetricCard
          title="大盘平均合格率"
          value={(avgAccuracy * 100).toFixed(1) + "%"}
          icon={<BarChart3 className="w-6 h-6 text-amber-600" />}
          color="amber"
          highlight={avgAccuracy > 0.95}
        />
        <MetricCard
          title="参与人员数"
          value={projects.reduce((acc, p) => acc + (p.person_count || 0), 0)}
          icon={<Users className="w-6 h-6 text-rose-600" />}
          color="rose"
        />
      </div>

      {/* 项目列表区域 */}
      <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100 overflow-hidden">
        <div className="p-6 border-b border-slate-50 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <h2 className="text-xl font-bold text-slate-800 flex items-center">
            <Database className="w-5 h-5 mr-2 text-indigo-500" />
            项目列表
          </h2>
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              placeholder="搜索项目..."
              className="pl-10 pr-4 py-2 bg-slate-50 border-none rounded-xl text-sm focus:ring-2 focus:ring-indigo-500 outline-none w-full md:w-64 transition-all"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead>
              <tr className="bg-slate-50/50 text-slate-500 text-xs uppercase tracking-wider">
                <th className="px-6 py-4 font-semibold">项目名称</th>
                <th className="px-6 py-4 font-semibold">POC 负责人</th>
                <th className="px-6 py-4 font-semibold text-center">参与人数</th>
                <th className="px-6 py-4 font-semibold text-center">总产量</th>
                <th className="px-6 py-4 font-semibold text-center">平均准确率</th>
                <th className="px-6 py-4 font-semibold text-right">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-50">
              {filteredProjects.length > 0 ? (
                filteredProjects.map((p) => (
                  <tr key={p.project_group_id} className="hover:bg-slate-50/80 transition-all group">
                    <td className="px-6 py-5">
                      <div className="flex flex-col">
                        <span className="font-bold text-slate-900 group-hover:text-indigo-600 transition-colors">
                          {p.project_name}
                        </span>
                        <span className="text-xs text-slate-400 mt-0.5">创建日期: {p.date}</span>
                      </div>
                    </td>
                    <td className="px-6 py-5">
                      <div className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-indigo-50 text-indigo-700">
                        {p.poc_name}
                      </div>
                    </td>
                    <td className="px-6 py-5 text-center font-medium text-slate-600">{p.person_count}</td>
                    <td className="px-6 py-5 text-center text-slate-600">{p.total_volume.toLocaleString()}</td>
                    <td className="px-6 py-5 text-center">
                      <div className="flex flex-col items-center">
                        <span className={`font-bold ${(p.overall_accuracy > 0.9) ? 'text-emerald-500' : 'text-amber-500'}`}>
                          {(p.overall_accuracy * 100).toFixed(1)}%
                        </span>
                        <div className="w-16 h-1 bg-slate-100 rounded-full mt-1.5 overflow-hidden">
                          <div
                            className={`h-full rounded-full ${(p.overall_accuracy > 0.9) ? 'bg-emerald-500' : 'bg-amber-500'}`}
                            style={{ width: `${p.overall_accuracy * 100}%` }}
                          />
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-5 text-right">
                      <Link
                        href={`/dashboard/${p.project_group_id}`}
                        className="inline-flex items-center text-indigo-600 hover:text-indigo-700 font-semibold text-sm transition-colors"
                      >
                        详情 <ArrowRight className="w-4 h-4 ml-1" />
                      </Link>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} className="px-6 py-20 text-center text-slate-400">
                    <div className="flex flex-col items-center">
                      <Database className="w-12 h-12 text-slate-200 mb-4" />
                      <p className="text-lg font-medium text-slate-500">暂无匹配的项目数据</p>
                      <p className="text-sm">请尝试不同的搜索关键词</p>
                    </div>
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

function MetricCard({ title, value, icon, color, highlight = false }: any) {
  const colors: any = {
    indigo: "bg-indigo-50 border-indigo-100",
    emerald: "bg-emerald-50 border-emerald-100",
    amber: "bg-amber-50 border-amber-100",
    rose: "bg-rose-50 border-rose-100",
  };

  return (
    <div className={`bg-white p-6 rounded-2xl shadow-sm border border-slate-100 flex flex-col justify-between hover:shadow-md transition-shadow group relative overflow-hidden`}>
      <div className={`absolute top-0 right-0 w-24 h-24 -mr-8 -mt-8 opacity-5 rounded-full ${color === 'indigo' ? 'bg-indigo-600' : color === 'emerald' ? 'bg-emerald-600' : 'bg-slate-600'}`}></div>
      <div className="flex items-start justify-between">
        <div className={`p-3 rounded-xl ${colors[color]} transition-transform group-hover:scale-110 duration-300`}>
          {icon}
        </div>
      </div>
      <div className="mt-4">
        <div className="text-slate-400 text-sm font-medium">{title}</div>
        <div className={`text-3xl font-black mt-1 tracking-tight text-slate-900`}>
          {value}
        </div>
      </div>
    </div>
  );
}
