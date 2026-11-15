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
  CircleCheck,
  CircleX,
  CircleHelp,
  Users,
  Terminal,
  GitMerge,
  FolderKanban,
} from "lucide-react";
import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";
import { useEffect, useState } from "react";
import { getStatus } from "@/lib/api";

const mobileNavItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/query", label: "Query", icon: MessageSquare },
  { href: "/graph", label: "Graph", icon: Network },
  { href: "/documents", label: "Docs", icon: FileText },
  { href: "/entities/review", label: "Review", icon: GitMerge },
  { href: "/hubs", label: "Hubs", icon: FolderKanban },
  { href: "/debug", label: "Debug", icon: Terminal },
];

const desktopNavItems = [
  { href: "/query", label: "Ask", icon: MessageSquare },
  { href: "/documents", label: "Documents", icon: FileText },
  { href: "/graph", label: "Graph", icon: Network },
  { href: "/entities/review", label: "Entities", icon: Users },
  { href: "/", label: "Status", icon: LayoutDashboard },
  { href: "/hubs", label: "Hubs", icon: FolderKanban },
  { href: "/debug", label: "Logs", icon: Terminal },
];

export function SidebarNav() {
  const pathname = usePathname();
  const { resolvedTheme, setTheme } = useTheme();
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

  if (pathname === "/gallery") return null;

  return (
    <>
      <aside aria-label="App navigation" className="hidden md:flex w-[200px] shrink-0 flex-col overflow-y-auto border-r bg-background px-4 py-6">
        <p className="mb-6 px-2 font-[family-name:var(--font-geist-serif-regular)] text-xl leading-6">Knowledge Graph</p>
        <nav aria-label="Main" className="space-y-0.5">
          {desktopNavItems.map((item, index) => {
            const active = item.href === "/" ? pathname === "/" : pathname === item.href || pathname.startsWith(`${item.href}/`);
            const HealthIcon = healthy === true ? CircleCheck : healthy === false ? CircleX : CircleHelp;
            return (
              <div key={item.href}>
                {index === 4 && <p className="label-12 mt-4 mb-1 border-t px-2 pt-3 text-fg-subtle">More</p>}
                <Link href={item.href} aria-current={active ? "page" : undefined}
                  className={cn("label-14 flex h-8 items-center gap-2 rounded-md px-2 transition-colors", active ? "bg-raised text-foreground" : "text-muted-foreground hover:bg-raised hover:text-foreground")}>
                  <item.icon className="size-4" strokeWidth={1.75} aria-hidden="true" />
                  {item.label}
                </Link>
                {item.href === "/" && <p role="status" className="label-12 flex items-center gap-2 px-2 py-1 text-fg-muted">
                  <HealthIcon aria-hidden="true" className={cn("size-3.5", healthy === true ? "text-verified" : healthy === false ? "text-error" : "text-fg-muted")} strokeWidth={1.75} />
                  {healthy === true ? "Backend up" : healthy === false ? "Backend down" : "Checking backend"}
                </p>}
              </div>
            );
          })}
        </nav>
        <Button variant="ghost" className="mt-4 w-full justify-start gap-2 px-2" aria-label="Toggle theme"
          onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}>
          <Sun className="size-4 dark:hidden" strokeWidth={1.75} aria-hidden="true" />
          <Moon className="hidden size-4 dark:block" strokeWidth={1.75} aria-hidden="true" />
          Toggle theme
        </Button>
      </aside>

      {/* Mobile bottom tab bar */}
      <div className="fixed bottom-0 left-0 right-0 z-50 md:hidden border-t bg-card/95 backdrop-blur-md safe-area-bottom">
        <nav className="flex items-center px-1 py-1">
          {mobileNavItems.map((item) => {
            const active =
              item.href === "/"
                ? pathname === "/"
                : pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                    aria-label={item.href === "/" ? "Home" : item.label}
                    aria-current={active ? "page" : undefined}
                className={cn(
                  "flex flex-1 min-w-0 flex-col items-center justify-center min-h-[44px] py-1 px-0.5 rounded-lg transition-colors",
                  active
                    ? "text-primary"
                    : "text-muted-foreground"
                )}
              >
                <item.icon className="h-5 w-5" />
                <span className="text-[10px] mt-0.5 font-medium">{item.href === "/" ? "Home" : item.label}</span>
              </Link>
            );
          })}
          <button
            aria-label="Toggle theme"
                onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
            className="flex flex-1 min-w-0 flex-col items-center justify-center min-h-[44px] py-1 px-0.5 rounded-lg text-muted-foreground"
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
