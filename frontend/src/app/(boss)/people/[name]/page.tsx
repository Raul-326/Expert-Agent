"use client";

import React, { useEffect, useState, use } from "react";
import Link from "next/link";
import { 
  ArrowLeft, 
  BarChart, 
  TrendingUp, 
  Loader2, 
  ChevronRight,
  User,
  Activity,
  Calendar
} from "lucide-react";
import { 
  AreaChart, 
  Area, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer 
} from "recharts";
import { API_BASE_URL } from "@/lib/constants";

export default function PersonDetail({ params }: { params: Promise<{ name: string }> }) {
  const { name } = use(params);
  const personName = decodeURIComponent(name);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/projects/people/${encodeURIComponent(personName)}/detail`)
      .then((res) => res.json())
      .then((data) => {
        setData(data);
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, [personName]);

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <Loader2 className="w-10 h-10 animate-spin text-indigo-600" />
      </div>
    );
  }

  if (!data || !data.projects) {
    return (
      <div className="p-8 text-center text-slate-500">
        <h2 className="text-xl font-bold">人员数据未找到</h2>
        <Link href="/people" className="text-indigo-600 hover:underline mt-4 block">返回人员列表</Link>
      </div>
    );
  }

  // 计算人员汇总
  const totalVolume = data.projects.reduce((acc: any, p: any) => acc + p.volume, 0);
  const avgAccuracy = data.projects.filter((p:any) => p.accuracy).length > 0
    ? data.projects.filter((p:any) => p.accuracy).reduce((acc: any, p: any) => acc + p.accuracy, 0) / data.projects.filter((p:any) => p.accuracy).length
    : 0;

  // 准备图表数据
  const chartData = data.projects.map((p: any) => ({
    name: p.project_name,
    accuracy: p.accuracy ? p.accuracy * 100 : null,
    volume: p.volume,
    date: p.date
  }));

  // 等级判断逻辑
  const getLevel = (acc: number) => {
    if (acc >= 0.98) return { label: "Master", color: "indigo" };
    if (acc >= 0.95) return { label: "Expert", color: "emerald" };
    if (acc >= 0.90) return { label: "Senior", color: "amber" };
    if (acc >= 0.85) return { label: "Intermediate", color: "rose" };
    return { label: "Junior", color: "slate" };
  };

  const level = getLevel(avgAccuracy);

  return (
    <div className="max-w-7xl mx-auto space-y-8 animate-in slide-in-from-right-4 duration-500 pb-20">
      <nav className="flex items-center space-x-2 text-sm">
        <Link href="/people" className="text-slate-400 hover:text-indigo-600 transition-colors flex items-center">
          <ArrowLeft className="w-4 h-4 mr-1" /> 人员列表
        </Link>
        <ChevronRight className="w-3 h-3 text-slate-300" />
        <span className="text-slate-600 font-medium">效能详情</span>
      </nav>

      <header className="bg-white rounded-[2rem] p-10 shadow-xl shadow-slate-200/50 border border-slate-50 flex flex-col md:flex-row items-center gap-10">
        <div className="w-32 h-32 rounded-3xl bg-indigo-600 flex items-center justify-center text-5xl font-black text-white shadow-lg shadow-indigo-200 shrink-0">
          {personName.charAt(0)}
        </div>
        <div className="flex-1 text-center md:text-left">
          <h1 className="text-5xl font-black text-slate-900 tracking-tight">{personName}</h1>
          <p className="text-slate-400 mt-2 font-medium flex items-center justify-center md:justify-start">
            <Activity className="w-4 h-4 mr-2 text-indigo-500" />
            数据更新周期内表现最优的项目合作伙伴
          </p>
          <div className="mt-8 grid grid-cols-2 md:grid-cols-4 gap-4 max-w-2xl">
            <StatsMini label="产量" value={totalVolume.toLocaleString()} color="indigo" />
            <StatsMini label="项目数" value={data.projects.length} color="amber" />
            <StatsMini label="准确率" value={(avgAccuracy * 100).toFixed(1) + "%"} color="emerald" />
            <StatsMini label="等级" value={level.label} color={level.color} />
          </div>
        </div>
      </header>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
        {/* 指标趋势图 */}
        <div className="xl:col-span-2 bg-white rounded-3xl p-8 shadow-xl shadow-slate-100 border border-slate-50">
          <div className="flex items-center justify-between mb-8">
            <h3 className="text-xl font-bold text-slate-800 flex items-center">
              <TrendingUp className="w-5 h-5 mr-2 text-indigo-500" />
              准确率增长趋势
            </h3>
          </div>
          <div className="h-[400px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorAcc" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#6366f1" stopOpacity={0.1}/>
                    <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                <XAxis 
                  dataKey="name" 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#94a3b8', fontSize: 10, fontWeight: 600}} 
                  dy={10}
                />
                <YAxis 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#94a3b8', fontSize: 10}}
                  domain={[0, 100]}
                />
                <Tooltip 
                  contentStyle={{borderRadius: '16px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)'}}
                  itemStyle={{fontWeight: 700}}
                />
                <Area 
                  type="monotone" 
                  dataKey="accuracy" 
                  stroke="#6366f1" 
                  strokeWidth={4}
                  fillOpacity={1} 
                  fill="url(#colorAcc)" 
                  dot={{r: 6, fill: '#6366f1', strokeWidth: 2, stroke: '#fff'}}
                  activeDot={{r: 8, strokeWidth: 0}}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* 参与过的项目列表 */}
        <div className="bg-white rounded-3xl shadow-xl shadow-slate-100 border border-slate-50 flex flex-col">
          <div className="px-8 py-6 border-b border-slate-50">
            <h3 className="font-bold text-slate-800 flex items-center uppercase tracking-widest text-[10px]">
              项目参与历史
            </h3>
          </div>
          <div className="flex-1 overflow-auto divide-y divide-slate-50">
            {data.projects.map((p: any, i: number) => (
              <div key={i} className="px-8 py-5 hover:bg-slate-50/50 transition-colors group">
                <div className="flex justify-between items-start mb-2">
                  <div className="font-bold text-slate-800">{p.project_name}</div>
                  <div className={`text-sm font-black ${p.accuracy > 0.95 ? 'text-emerald-500' : 'text-amber-500'}`}>
                    {p.accuracy ? (p.accuracy * 100).toFixed(1) + "%" : "-"}
                  </div>
                </div>
                <div className="flex items-center text-[10px] text-slate-400 font-bold space-x-3 uppercase tracking-tighter">
                  <span className="flex items-center text-indigo-500"><User className="w-3 h-3 mr-1" /> {p.role}</span>
                  <span><BarChart className="w-3 h-3 inline mr-1" /> {p.volume}</span>
                  <span><Calendar className="w-3 h-3 inline mr-1" /> {p.date}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function StatsMini({ label, value, color }: any) {
  const colors: any = {
    indigo: "text-indigo-600",
    amber: "text-amber-600",
    emerald: "text-emerald-600",
    rose: "text-rose-600",
  };
  return (
    <div className="bg-slate-50/50 rounded-2xl p-4 border border-slate-100">
      <div className="text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-wider">{label}</div>
      <div className={`text-xl font-black ${colors[color]}`}>{value}</div>
    </div>
  );
}
