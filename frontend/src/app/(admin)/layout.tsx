import React from "react";
import Link from "next/link";

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex h-screen bg-slate-50">
      {/* Sidebar */}
      <aside className="w-64 bg-indigo-900 text-white flex flex-col">
        <div className="p-6 text-2xl font-bold border-b border-indigo-800 flex items-center">
          <span className="mr-2">⚡️</span> Admin 控制台
        </div>
        <nav className="flex-1 p-4 space-y-2">
          <Link href="/workspace" className="block p-3 rounded hover:bg-indigo-800 transition-colors">
            📥 执行入库作业
          </Link>
          <Link href="/projects" className="block p-3 rounded hover:bg-indigo-800 transition-colors">
            🛠️ 项目数据核校
          </Link>
          <Link href="/audit_logs" className="block p-3 rounded hover:bg-indigo-800 transition-colors">
            📜 操作审计日志
          </Link>
        </nav>
        <div className="p-4 text-sm text-indigo-300 border-t border-indigo-800">
          登录账号: Admin01
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto p-8 border-l border-slate-200 shadow-inner">
        {children}
      </main>
    </div>
  );
}
