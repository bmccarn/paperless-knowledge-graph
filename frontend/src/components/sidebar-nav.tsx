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
  Zap,
  Terminal,
} from "lucide-react";
import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useEffect, useState } from "react";
import { getStatus } from "@/lib/api";

const navItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/query", label: "Query", icon: MessageSquare },
  { href: "/graph", label: "Graph", icon: Network },
  { href: "/documents", label: "Docs", icon: FileText },
  { href: "/debug", label: "Debug", icon: Terminal },
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
    <>
      {/* Desktop sidebar */}
      <div className="hidden md:flex h-screen w-16 flex-col border-r bg-card/50 backdrop-blur-sm">
        {/* Logo */}
        <div className="flex items-center justify-center py-4 border-b">
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="relative">
                <div className="h-9 w-9 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Zap className="h-5 w-5 text-primary" />
                </div>
                <span
                  className={cn(
                    "absolute -top-0.5 -right-0.5 h-2.5 w-2.5 rounded-full border-2 border-card",
                    healthy === true && "bg-emerald-500",
                    healthy === false && "bg-red-500",
                    healthy === null && "bg-yellow-500 animate-pulse"
                  )}
                />
              </div>
            </TooltipTrigger>
            <TooltipContent side="right">
              <p className="font-medium">Knowledge Graph</p>
              <p className="text-xs text-muted-foreground">
                {healthy === true ? "Connected" : healthy === false ? "Disconnected" : "Checking..."}
              </p>
            </TooltipContent>
          </Tooltip>
        </div>

        {/* Nav items */}
        <nav className="flex-1 flex flex-col items-center gap-1 py-3 px-2">
          {navItems.map((item) => {
            const active =
              item.href === "/"
                ? pathname === "/"
                : pathname.startsWith(item.href);
            return (
              <Tooltip key={item.href}>
                <TooltipTrigger asChild>
                  <Link
                    href={item.href}
                    className={cn(
                      "flex h-10 w-10 items-center justify-center rounded-lg transition-all duration-200",
                      active
                        ? "bg-primary text-primary-foreground shadow-md shadow-primary/25"
                        : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                    )}
                  >
                    <item.icon className="h-[18px] w-[18px]" />
                  </Link>
                </TooltipTrigger>
                <TooltipContent side="right">{item.label}</TooltipContent>
              </Tooltip>
            );
          })}
        </nav>

        {/* Theme toggle */}
        <div className="border-t py-3 flex justify-center">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-10 w-10 rounded-lg"
                onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
              >
                <Sun className="h-[18px] w-[18px] rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
                <Moon className="absolute h-[18px] w-[18px] rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="right">Toggle theme</TooltipContent>
          </Tooltip>
        </div>
      </div>

      {/* Mobile bottom tab bar */}
      <div className="fixed bottom-0 left-0 right-0 z-50 md:hidden border-t bg-card/95 backdrop-blur-md safe-area-bottom">
        <nav className="flex items-center justify-around px-2 py-1">
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
                  "flex flex-col items-center justify-center min-w-[44px] min-h-[44px] py-1 px-2 rounded-lg transition-colors",
                  active
                    ? "text-primary"
                    : "text-muted-foreground"
                )}
              >
                <item.icon className="h-5 w-5" />
                <span className="text-[10px] mt-0.5 font-medium">{item.label}</span>
              </Link>
            );
          })}
          <button
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            className="flex flex-col items-center justify-center min-w-[44px] min-h-[44px] py-1 px-2 rounded-lg text-muted-foreground"
          >
            <Sun className="h-5 w-5 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
            <Moon className="absolute h-5 w-5 rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
            <span className="text-[10px] mt-0.5 font-medium">Theme</span>
          </button>
        </nav>
      </div>
    </>
  );
}
