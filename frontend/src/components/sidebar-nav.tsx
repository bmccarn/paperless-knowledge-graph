"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  MessageSquare,
  Network,
  FileText,
  Moon,
  Sun,
} from "lucide-react";
import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";
import { useEffect, useState } from "react";
import { getStatus } from "@/lib/api";

const navItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/query", label: "Query", icon: MessageSquare },
  { href: "/graph", label: "Graph Explorer", icon: Network },
  { href: "/documents", label: "Documents", icon: FileText },
];

export function SidebarNav() {
  const pathname = usePathname();
  const { theme, setTheme } = useTheme();
  const [healthy, setHealthy] = useState<boolean | null>(null);

  useEffect(() => {
    const check = () =>
      getStatus()
        .then((s) => setHealthy(s.status === "healthy"))
        .catch(() => setHealthy(false));
    check();
    const i = setInterval(check, 30000);
    return () => clearInterval(i);
  }, []);

  return (
    <div className="flex h-screen w-56 flex-col border-r bg-card">
      <div className="flex items-center gap-2 border-b px-4 py-4">
        <Network className="h-5 w-5 text-primary" />
        <span className="font-semibold text-sm">Knowledge Graph</span>
        <span
          className={cn(
            "ml-auto h-2.5 w-2.5 rounded-full",
            healthy === true && "bg-green-500",
            healthy === false && "bg-red-500",
            healthy === null && "bg-yellow-500"
          )}
        />
      </div>
      <nav className="flex-1 space-y-1 p-2">
        {navItems.map((item) => {
          const active =
            item.href === "/"
              ? pathname === "/"
              : pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors",
                active
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
              )}
            >
              <item.icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="border-t p-2">
        <Button
          variant="ghost"
          size="sm"
          className="w-full justify-start gap-2"
          onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
        >
          <Sun className="h-4 w-4 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
          <Moon className="absolute h-4 w-4 rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
          <span className="ml-2">Toggle theme</span>
        </Button>
      </div>
    </div>
  );
}
