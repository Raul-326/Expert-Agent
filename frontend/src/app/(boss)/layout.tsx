import React from "react";
import Link from "next/link";
import { 
  BarChart3, 
  Users, 
  LogOut, 
  Settings,
  ShieldCheck,
  LayoutDashboard
} from "lucide-react";

export default function BossLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex h-screen bg-slate-50 font-sans antialiased text-slate-900">
      {/* Sidebar */}
      <aside className="w-72 bg-white border-r border-slate-100 hidden md:flex flex-col z-20 shadow-2xl shadow-slate-200/50">
        <div className="p-8 pb-10 flex items-center space-x-3">
          <div className="w-10 h-10 bg-indigo-600 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-200">
             <ShieldCheck className="text-white w-6 h-6" />
          </div>
          <div className="font-black text-xl tracking-tight text-slate-800">
            Dola Expert
          </div>
        </div>

        <nav className="flex-1 px-4 space-y-1">
          <SidebarLink href="/dashboard" icon={<LayoutDashboard className="w-5 h-5" />} label="项目总览" />
          <SidebarLink href="/people" icon={<Users className="w-5 h-5" />} label="人员效能" />
          <SidebarLink href="/quality_trends" icon={<BarChart3 className="w-5 h-5" />} label="质量趋势" />
        </nav>

        <div className="p-6 border-t border-slate-50 space-y-4">
          <div className="flex items-center space-x-3 px-2 py-2">
            <div className="w-10 h-10 rounded-full bg-slate-100 border border-slate-200 flex items-center justify-center font-bold text-slate-400">
              B
            </div>
            <div className="flex-1 overflow-hidden">
               <div className="text-sm font-bold truncate">决策席 (Boss)</div>
               <div className="text-[10px] text-slate-400 font-bold uppercase tracking-widest">Administrator</div>
            </div>
          </div>
          <button className="w-full flex items-center justify-center space-x-2 p-3 rounded-2xl bg-slate-50 text-slate-500 hover:bg-rose-50 hover:text-rose-600 transition-all font-bold text-sm">
            <LogOut className="w-4 h-4" />
            <span>退出系统</span>
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto bg-slate-50/50">
        <div className="min-h-full p-8 md:p-12">
          {children}
        </div>
      </main>
    </div>
  );
}

function SidebarLink({ href, icon, label }: any) {
  return (
    <Link 
      href={href} 
      className="flex items-center space-x-3 px-4 py-4 rounded-2xl text-slate-500 hover:bg-indigo-50 hover:text-indigo-600 transition-all font-bold group"
    >
      <div className="transition-transform group-hover:scale-110">
        {icon}
      </div>
      <span>{label}</span>
    </Link>
  );
}
