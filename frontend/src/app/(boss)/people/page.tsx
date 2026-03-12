"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import {
  Users,
  Search,
  ArrowRight,
  Loader2,
  UserCheck,
  BarChart,
  Trophy,
  Filter
} from "lucide-react";
import { API_BASE_URL } from "@/lib/constants";

export default function PeoplePage() {
  const [people, setPeople] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/v1/projects/people/search`)
      .then((res) => res.json())
      .then((data) => {
        if (data.data) {
          setPeople(data.data);
        }
      })
      .catch((err) => console.error(err))
      .finally(() => setLoading(false));
  }, []);

  const filteredPeople = people.filter(p => 
    p.person_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

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
          <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">人员效能全景</h1>
          <p className="text-slate-500 mt-2">跨项目追踪每一位合作伙伴的质量与效率</p>
        </div>
      </header>

      {/* 搜索与过滤 */}
      <div className="bg-white p-4 rounded-2xl shadow-sm border border-slate-100 flex items-center gap-4">
        <div className="relative flex-1">
          <Search className="w-5 h-5 absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" />
          <input 
            type="text" 
            placeholder="搜索姓名..." 
            className="w-full pl-12 pr-4 py-3 bg-slate-50 border-none rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 transition-all font-medium"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <button className="flex items-center space-x-2 px-6 py-3 border border-slate-200 rounded-xl hover:bg-slate-50 transition-colors text-slate-600 font-bold text-sm">
          <Filter className="w-4 h-4" />
          <span>筛选</span>
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        {filteredPeople.map((person, idx) => (
          <PersonCard key={idx} person={person} />
        ))}
        {filteredPeople.length === 0 && (
          <div className="col-span-full py-20 text-center text-slate-400">
            <Users className="w-16 h-16 mx-auto mb-4 text-slate-100" />
            <p className="text-lg">没有找到匹配的人员</p>
          </div>
        )}
      </div>
    </div>
  );
}

function PersonCard({ person }: any) {
  const accuracy = person.accuracy || 0;
  
  return (
    <div className="group bg-white rounded-3xl p-6 shadow-xl shadow-slate-200/40 border border-slate-50 hover:border-indigo-100 hover:shadow-indigo-100 transition-all relative overflow-hidden">
      <div className="absolute top-0 right-0 p-6 opacity-0 group-hover:opacity-100 transition-opacity">
        <ArrowRight className="w-5 h-5 text-indigo-400" />
      </div>
      
      <div className="flex items-start space-x-4 mb-6">
        <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-slate-100 to-slate-200 flex items-center justify-center text-xl font-black text-slate-400 group-hover:from-indigo-500 group-hover:to-indigo-600 group-hover:text-white transition-all duration-500 shadow-inner">
          {person.person_name.charAt(0)}
        </div>
        <div>
          <h3 className="text-xl font-black text-slate-800">{person.person_name}</h3>
          <div className="flex flex-wrap gap-1 mt-1">
            {person.roles.split(',').map((role: string, i: number) => (
              <span key={i} className="px-1.5 py-0.5 bg-slate-100 text-[9px] font-bold text-slate-500 rounded uppercase tracking-tighter">
                {role}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="bg-slate-50/50 rounded-2xl p-4">
          <div className="text-[10px] font-bold text-slate-400 uppercase mb-1">参与项目</div>
          <div className="text-lg font-black text-slate-800">{person.project_count}</div>
        </div>
        <div className="bg-slate-50/50 rounded-2xl p-4">
          <div className="text-[10px] font-bold text-slate-400 uppercase mb-1">总产量</div>
          <div className="text-lg font-black text-slate-800">{person.volume_total.toLocaleString()}</div>
        </div>
      </div>

      <div className="relative pt-6 border-t border-slate-50">
        <div className="flex justify-between items-end mb-2">
          <div className="text-[10px] font-bold text-slate-400 uppercase">平均准确率</div>
          <div className={`text-xl font-black ${accuracy > 0.95 ? 'text-emerald-500' : 'text-amber-500'}`}>
            {(accuracy * 100).toFixed(1)}%
          </div>
        </div>
        <div className="h-2 w-full bg-slate-50 rounded-full overflow-hidden">
          <div 
            className={`h-full rounded-full transition-all duration-1000 ${accuracy > 0.95 ? 'bg-emerald-500' : 'bg-amber-500'}`}
            style={{ width: `${accuracy * 100}%` }}
          />
        </div>
      </div>

      <Link 
        href={`/people/${encodeURIComponent(person.person_name)}`}
        className="absolute inset-0 z-10"
      />
    </div>
  );
}
