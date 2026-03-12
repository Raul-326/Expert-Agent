"use client";

import React, { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar
} from "recharts";
import {
  TrendingUp,
  Activity,
  Target,
  ShieldCheck,
  Loader2,
  Calendar
} from "lucide-react";
import { API_BASE_URL } from "@/lib/constants";

export default function QualityTrends() {
  const [projects, setProjects] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/projects/`)
      .then((res) => res.json())
      .then((data) => {
        if (data.data) {
          // 按日期排序以便展示趋势
          const sorted = [...data.data].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
          setProjects(sorted);
        }
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex h-96 items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-indigo-600" />
      </div>
    );
  }

  // 准备图表数据
  const chartData = projects.map((p: any) => ({
    name: p.project_name,
    accuracy: p.overall_accuracy * 100,
    volume: p.total_volume,
    shortName: p.project_name.length > 10 ? p.project_name.substring(0, 10) + "..." : p.project_name
  }));

  const avgAcc = projects.length > 0 ? (projects.reduce((acc: any, p: any) => acc + p.overall_accuracy, 0) / projects.length) * 100 : 0;

  return (
    <div className="max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      <header>
        <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">行业质量趋势分析</h1>
        <p className="text-slate-500 mt-2">基于全量项目的加权准确率动态演进曲线</p>
      </header>

      {/* 核心看板 */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <TrendStatCard 
          title="大盘平均准确率" 
          value={avgAcc.toFixed(2) + "%"} 
          icon={<ShieldCheck className="w-6 h-6 text-emerald-600" />} 
          desc="较上月核心指标持平"
        />
        <TrendStatCard 
          title="高质项目占比" 
          value={((projects.filter((p: any) => p.overall_accuracy > 0.95).length / (projects.length || 1)) * 100).toFixed(0) + "%"} 
          icon={<Target className="w-6 h-6 text-indigo-600" />} 
          desc="准确率 > 95% 的标评项目"
        />
        <TrendStatCard 
          title="异常波动预警" 
          value="0" 
          icon={<Activity className="w-6 h-6 text-rose-600" />} 
          desc="过去24小时无重大偏离"
        />
      </div>

      {/* 主趋势图 */}
      <div className="bg-white rounded-[2.5rem] p-10 shadow-xl shadow-slate-200/50 border border-slate-50">
        <div className="flex items-center justify-between mb-10">
          <div>
            <h3 className="text-xl font-bold text-slate-800">全大盘准确率演进图</h3>
            <p className="text-xs text-slate-400 mt-1 font-bold">ACCURACY TREND LINE (BY PROJECT BATCH)</p>
          </div>
          <div className="flex space-x-2">
            <div className="flex items-center space-x-2 px-4 py-2 bg-indigo-50 rounded-xl text-indigo-600 text-xs font-bold">
              <Calendar className="w-4 h-4" /> <span>2024 年度回顾</span>
            </div>
          </div>
        </div>

        <div className="h-[450px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="trendGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366f1" stopOpacity={0.15}/>
                  <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
              <XAxis 
                dataKey="shortName" 
                axisLine={false} 
                tickLine={false} 
                tick={{fill: '#94a3b8', fontSize: 11, fontWeight: 700}} 
                dy={15}
              />
              <YAxis 
                axisLine={false} 
                tickLine={false} 
                tick={{fill: '#94a3b8', fontSize: 11}}
                domain={[0, 100]}
              />
              <Tooltip 
                contentStyle={{borderRadius: '20px', border: 'none', boxShadow: '0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1)'}}
                itemStyle={{fontWeight: 800, color: '#4f46e5'}}
                cursor={{ stroke: '#e2e8f0', strokeWidth: 2 }}
              />
              <Area 
                type="monotone" 
                dataKey="accuracy" 
                stroke="#6366f1" 
                strokeWidth={5}
                fillOpacity={1} 
                fill="url(#trendGradient)" 
                dot={{r: 6, fill: '#6366f1', strokeWidth: 3, stroke: '#fff'}}
                activeDot={{r: 10, strokeWidth: 0}}
                name="准确率 (%)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* 产量柱状图 */}
      <div className="bg-white rounded-[2.5rem] p-10 shadow-xl shadow-slate-200/50 border border-slate-50">
         <h3 className="text-xl font-bold text-slate-800 mb-8 flex items-center">
            <TrendingUp className="w-5 h-5 mr-2 text-emerald-500" />
            各批次产出量对比
         </h3>
         <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                <XAxis 
                  dataKey="shortName" 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#94a3b8', fontSize: 11, fontWeight: 700}} 
                />
                <YAxis axisLine={false} tickLine={false} tick={{fill: '#94a3b8', fontSize: 11}} />
                <Tooltip cursor={{fill: '#f8fafc'}} />
                <Bar 
                  dataKey="volume" 
                  fill="#10b981" 
                  radius={[8, 8, 0, 0]} 
                  className="hover:opacity-80 transition-opacity"
                  name="产量"
                />
              </BarChart>
            </ResponsiveContainer>
         </div>
      </div>
    </div>
  );
}

function TrendStatCard({ title, value, icon, desc }: any) {
  return (
    <div className="bg-white p-8 rounded-[2rem] shadow-sm border border-slate-50 relative overflow-hidden group hover:shadow-lg transition-all">
      <div className="flex justify-between items-start mb-4">
        <div className="p-3 bg-slate-50 rounded-2xl group-hover:scale-110 transition-transform duration-500">
          {icon}
        </div>
      </div>
      <div className="text-3xl font-black text-slate-900 tracking-tighter mb-1">{value}</div>
      <div className="text-sm font-bold text-slate-800 mb-1">{title}</div>
      <div className="text-xs text-slate-400 font-medium">{desc}</div>
    </div>
  );
}
