import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Expert Agent Multi-Agent Dashboard",
  description: "Web platform for project pipeline and multi-agent operations.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="zh">
      <body>{children}</body>
    </html>
  );
}
