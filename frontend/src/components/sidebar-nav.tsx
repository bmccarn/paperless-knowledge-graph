"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { LayoutDashboard, MessageSquare, Network, FileText, Moon, Sun, CircleCheck, CircleX, CircleHelp, Users, Terminal, FolderKanban, Ellipsis } from "lucide-react";
import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";
import { Sheet, SheetTrigger, SheetContent, SheetTitle, SheetHeader, SheetClose } from "@/components/ui/sheet";
import { useEffect, useRef, useState } from "react";
import { CommandPalette, PaletteSearchButton } from "@/components/command-palette";
import { getStatus } from "@/lib/api";

const primaryItems = [
  { href: "/query", label: "Ask", icon: MessageSquare },
  { href: "/documents", label: "Documents", icon: FileText },
  { href: "/graph", label: "Graph", icon: Network },
  { href: "/entities/review", label: "Entities", icon: Users },
];
const secondaryItems = [
  { href: "/", label: "Status", icon: LayoutDashboard },
  { href: "/hubs", label: "Hubs", icon: FolderKanban },
  { href: "/debug", label: "Logs", icon: Terminal },
];

function isCurrent(pathname: string, href: string) {
  return pathname === href || (href !== "/" && pathname.startsWith(`${href}/`));
}

function BackendHealth({ healthy }: { healthy: boolean | null }) {
  const Icon = healthy === true ? CircleCheck : healthy === false ? CircleX : CircleHelp;
  return <p role="status" className="label-12 flex items-center gap-2 px-2 py-1 text-fg-muted">
    <Icon aria-hidden="true" className={cn("size-3.5", healthy === true ? "text-verified" : healthy === false ? "text-error" : "text-fg-muted")} strokeWidth={1.75} />
    {healthy === true ? "Backend up" : healthy === false ? "Backend down" : "Checking backend"}
  </p>;
}

function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  return <Button variant="ghost" className="w-full justify-start gap-2 px-2 text-fg-muted" aria-label="Toggle theme"
    onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}>
    <Sun className="size-4 dark:hidden" strokeWidth={1.75} aria-hidden="true" />
    <Moon className="hidden size-4 dark:block" strokeWidth={1.75} aria-hidden="true" />
    Toggle theme
  </Button>;
}

export function TabBar({ pathname, healthy, className }: { pathname: string; healthy: boolean | null; className?: string }) {
  const [open, setOpen] = useState(false);
  const tabClass = "flex min-w-0 flex-col items-center justify-center gap-1 rounded-md text-fg-muted transition-colors hover:bg-raised hover:text-foreground data-[active=true]:bg-raised data-[active=true]:text-foreground";
  return <Sheet open={open} onOpenChange={setOpen}>
    <div className={cn("fixed inset-x-0 bottom-0 z-40 border-t bg-surface pb-[env(safe-area-inset-bottom)] md:hidden", className)}>
      <nav aria-label="Phone" className="grid h-(--tabbar) grid-cols-5 px-1">
        {primaryItems.map(item => <Link key={item.href} href={item.href}
          aria-current={isCurrent(pathname, item.href) ? "page" : undefined}
          data-active={isCurrent(pathname, item.href)} className={tabClass}>
          <item.icon className="size-4 shrink-0" strokeWidth={1.75} aria-hidden="true" />
          <span className="label-12">{item.label}</span>
        </Link>)}
        <SheetTrigger className={tabClass} data-active={secondaryItems.some(item => isCurrent(pathname, item.href))}>
          <Ellipsis className="size-4 shrink-0" strokeWidth={1.75} aria-hidden="true" />
          <span className="label-12">More</span>
        </SheetTrigger>
      </nav>
    </div>
    <SheetContent side="bottom" aria-describedby={undefined} className="max-h-[85dvh] overflow-y-auto rounded-t-xl gap-0 pb-[env(safe-area-inset-bottom)]">
      <SheetHeader><SheetTitle>More</SheetTitle></SheetHeader>
      <nav aria-label="More" className="px-4 pb-3">
        {secondaryItems.map(item => <SheetClose asChild key={item.href}>
          <Link href={item.href} aria-current={isCurrent(pathname, item.href) ? "page" : undefined}
            className={cn("label-14 flex min-h-11 items-center gap-2 rounded-md px-2 hover:bg-raised", isCurrent(pathname, item.href) && "bg-raised")}>
            <item.icon className="size-4" strokeWidth={1.75} aria-hidden="true" />{item.label}
          </Link>
        </SheetClose>)}
      </nav>
      <div className="space-y-2 border-t px-4 py-3">
        <ThemeToggle />
        <BackendHealth healthy={healthy} />
      </div>
    </SheetContent>
  </Sheet>;
}

export function SidebarNav() {
  const pathname = usePathname();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const previousFocus = useRef<HTMLElement | null>(null);
  const openPalette = () => {
    previousFocus.current = document.activeElement as HTMLElement;
    setPaletteOpen(true);
  };
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (location.pathname === "/gallery") return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        if (document.querySelector('[role="dialog"][data-palette]')) return;
        previousFocus.current = document.activeElement as HTMLElement;
        setPaletteOpen(true);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);
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

  return <>
    <aside aria-label="App navigation" className="hidden md:flex w-[200px] shrink-0 flex-col overflow-y-auto border-r bg-background px-4 py-6">
      <p className="mb-6 px-2 font-[family-name:var(--font-geist-serif-regular)] text-xl leading-6">Knowledge Graph</p>
      <PaletteSearchButton onClick={openPalette} className="mb-4 w-full justify-start gap-2 px-2" />
      <nav aria-label="Main" className="space-y-0.5">
        {[...primaryItems, ...secondaryItems].map((item, index) => <div key={item.href}>
          {index === 4 && <p className="label-12 mt-4 mb-1 border-t px-2 pt-3 text-fg-subtle">More</p>}
          <Link href={item.href} aria-current={isCurrent(pathname, item.href) ? "page" : undefined}
            className={cn("label-14 flex h-8 items-center gap-2 rounded-md px-2 transition-colors", isCurrent(pathname, item.href) ? "bg-raised text-foreground" : "text-muted-foreground hover:bg-raised hover:text-foreground")}>
            <item.icon className="size-4" strokeWidth={1.75} aria-hidden="true" />{item.label}
          </Link>
        </div>)}
      </nav>
      <div className="mt-auto space-y-2 border-t pt-3">
        <ThemeToggle />
        <BackendHealth healthy={healthy} />
      </div>
    </aside>
    <header className="fixed inset-x-0 top-0 z-40 flex h-14 items-center justify-between border-b bg-surface px-4 md:hidden">
      <span className="font-[family-name:var(--font-geist-serif-regular)] text-lg">Knowledge Graph</span>
      <PaletteSearchButton onClick={openPalette} className="gap-2" />
    </header>
    <TabBar pathname={pathname} healthy={healthy} />
    {paletteOpen && <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} returnFocus={() => previousFocus.current?.focus()} />}
  </>;
}
