import React from "react";
import Link from "next/link";

export default function BossLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="w-64 bg-slate-900 text-white flex flex-col">
        <div className="p-6 text-2xl font-bold border-b border-slate-800">
          BOSS 数据大盘
        </div>
        <nav className="flex-1 p-4 space-y-2">
          <Link href="/dashboard" className="block p-3 rounded hover:bg-slate-800 transition-colors">
            📊 项目总览
          </Link>
          <Link href="/people" className="block p-3 rounded hover:bg-slate-800 transition-colors">
            👥 人员效能
          </Link>
        </nav>
        <div className="p-4 text-sm text-slate-400">登出系统</div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto p-8">
        {children}
      </main>
    </div>
  );
}
