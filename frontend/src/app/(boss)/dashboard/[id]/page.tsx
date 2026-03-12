"use client";

import React, { useEffect, useState, use } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  Users,
  Trophy,
  BarChart,
  Loader2,
  ChevronRight,
  TrendingUp,
  LineChart
} from "lucide-react";
import { API_BASE_URL } from "@/lib/constants";

export default function ProjectDetail({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [detail, setDetail] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/projects/${id}/detail`)
      .then((res) => res.json())
      .then((data) => {
        setDetail(data);
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <Loader2 className="w-10 h-10 animate-spin text-indigo-600" />
      </div>
    );
  }

  if (!detail || detail.error) {
    return (
      <div className="p-8 text-center text-slate-500">
        <h2 className="text-xl font-bold">项目未找到</h2>
        <Link href="/dashboard" className="text-indigo-600 hover:underline mt-4 block">返回总览</Link>
      </div>
    );
  }

  // 计算项目汇总
  const totalVolume = detail.people.reduce((acc: any, p: any) => acc + p.volume_total, 0);
  const avgAccuracy = detail.people.length > 0
    ? detail.people.filter((p:any) => p.accuracy).reduce((acc: any, p: any) => acc + p.accuracy, 0) / detail.people.filter((p:any) => p.accuracy).length
    : 0;

  return (
    <div className="max-w-7xl mx-auto space-y-8 animate-in slide-in-from-right-4 duration-500">
      <nav className="flex items-center space-x-2 text-sm">
        <Link href="/dashboard" className="text-slate-400 hover:text-indigo-600 transition-colors flex items-center">
          <ArrowLeft className="w-4 h-4 mr-1" /> 项目总览
        </Link>
        <ChevronRight className="w-3 h-3 text-slate-300" />
        <span className="text-slate-600 font-medium">项目详情</span>
      </nav>

      <header className="flex flex-col md:flex-row md:items-end justify-between gap-6">
        <div>
          <span className="inline-block px-3 py-1 rounded-full bg-indigo-50 text-indigo-700 text-xs font-bold uppercase tracking-widest mb-3">
            Project Overview
          </span>
          <h1 className="text-4xl font-black text-slate-900 leading-none">
            {detail.project_name}
          </h1>
          <div className="flex items-center mt-4 space-x-4 text-slate-500">
            <span className="flex items-center"><Users className="w-4 h-4 mr-1.5" /> POC: {detail.poc_name || '未指定'}</span>
            <span className="text-slate-300">|</span>
            <span>创建日期: {new Date(detail.created_at).toLocaleDateString()}</span>
          </div>
        </div>
        
        <div className="flex space-x-4">
          <DetailMetric label="总产量" value={totalVolume.toLocaleString()} icon={<BarChart className="w-4 h-4" />} />
          <DetailMetric label="平均准确率" value={(avgAccuracy * 100).toFixed(1) + "%"} icon={<Trophy className="w-4 h-4" />} />
        </div>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* 左侧表格 */}
        <div className="lg:col-span-2 bg-white rounded-3xl shadow-xl shadow-slate-100 border border-slate-50 overflow-hidden">
          <div className="px-8 py-6 border-b border-slate-50 bg-slate-50/30">
            <h3 className="font-bold text-slate-800 text-lg">项目人员效能明细</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="text-slate-400 text-[10px] uppercase tracking-widest border-b border-slate-50">
                  <th className="px-8 py-4 font-bold">人员</th>
                  <th className="px-8 py-4 font-bold">角色</th>
                  <th className="px-8 py-4 font-bold text-center">总产量</th>
                  <th className="px-8 py-4 font-bold text-center">抽检比</th>
                  <th className="px-8 py-4 font-bold text-center">加权准确率</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50/50 text-sm">
                {detail.people.map((p: any, idx: number) => (
                  <tr key={idx} className="hover:bg-slate-50/50 transition-colors">
                    <td className="px-8 py-5">
                      <Link href={`/people/${encodeURIComponent(p.person_name)}`} className="font-bold text-slate-700 hover:text-indigo-600 transition-colors">
                        {p.person_name}
                      </Link>
                    </td>
                    <td className="px-8 py-5 text-slate-500 break-all">
                      <span className="px-2 py-0.5 rounded-md bg-slate-100 text-[10px] font-bold">
                        {p.role}
                      </span>
                    </td>
                    <td className="px-8 py-5 text-center font-mono text-slate-600 font-medium">
                      {p.volume_total}
                    </td>
                    <td className="px-8 py-5 text-center text-slate-400">
                      {p.inspected_total > 0 ? `${p.pass_total}/${p.inspected_total}` : '-'}
                    </td>
                    <td className="px-8 py-5 text-center">
                      {p.accuracy ? (
                        <span className={`px-2 py-1 rounded-lg text-xs font-bold ${p.accuracy > 0.95 ? 'bg-emerald-50 text-emerald-600' : 'bg-amber-50 text-amber-600'}`}>
                          {(p.accuracy * 100).toFixed(1)}%
                        </span>
                      ) : <span className="text-slate-300">-</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* 右侧卡片：排行榜 */}
        <div className="space-y-6">
          <div className="bg-gradient-to-br from-indigo-600 to-violet-700 rounded-3xl p-8 text-white shadow-lg shadow-indigo-200">
            <h3 className="text-xl font-bold mb-6 flex items-center">
              <Trophy className="w-5 h-5 mr-2 text-indigo-200" />
              Top Performers
            </h3>
            <div className="space-y-4">
              {detail.people
                .filter((p: any) => p.accuracy)
                .sort((a: any, b: any) => b.accuracy - a.accuracy)
                .slice(0, 3)
                .map((p: any, i: number) => (
                  <div key={i} className="bg-white/10 backdrop-blur-md rounded-2xl p-4 flex items-center justify-between border border-white/10">
                    <div className="flex items-center space-x-3">
                      <div className="w-8 h-8 rounded-full bg-white/20 flex items-center justify-center font-bold text-sm">
                        {i + 1}
                      </div>
                      <div>
                        <div className="font-bold text-sm">{p.person_name}</div>
                        <div className="text-[10px] text-indigo-200 uppercase">{p.role}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="font-black text-xs">{(p.accuracy * 100).toFixed(1)}%</div>
                    </div>
                  </div>
                ))}
            </div>
          </div>

          <div className="bg-white rounded-3xl p-8 shadow-xl shadow-slate-100 border border-slate-50">
            <h3 className="font-bold text-slate-800 mb-4 flex items-center">
              <TrendingUp className="w-5 h-5 mr-2 text-emerald-500" />
              产量分布 
            </h3>
            <div className="space-y-4">
               {/* 简化版进度条表示分布 */}
               {detail.people.slice(0, 5).map((p: any, i: number) => (
                 <div key={i}>
                    <div className="flex justify-between text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-tighter">
                      <span>{p.person_name}</span>
                      <span>{p.volume_total}</span>
                    </div>
                    <div className="h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-emerald-400 rounded-full" 
                        style={{ width: `${(p.volume_total / totalVolume * 300)}%` }}
                      ></div>
                    </div>
                 </div>
               ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function DetailMetric({ label, value, icon }: any) {
  return (
    <div className="bg-white px-6 py-4 rounded-2xl border border-slate-100 shadow-sm flex items-center space-x-4">
      <div className="p-3 bg-slate-50 rounded-xl text-slate-400">
        {icon}
      </div>
      <div>
        <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{label}</div>
        <div className="text-xl font-black text-slate-800">{value}</div>
      </div>
    </div>
  );
}
