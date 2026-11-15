"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Check, ChevronDown } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { cn } from "@/lib/utils";
import { GRAPH_VIEWS, graphNavigation, keyedView, parseGraphState, rovingIndex, type GraphView, type GraphViewInfo } from "@/lib/graph-url-state";

const NO_FOCUS = "Pick an entity on the map or search first.";
const keyOf = (id: GraphView) => GRAPH_VIEWS.findIndex(view => view.id === id) + 1;

interface SwitcherProps {
  /** The listed views, in switcher order. */
  views: readonly GraphViewInfo[];
  current: GraphView;
  /** The focus entity's name, or null when there is none. */
  focusName: string | null;
  onSwitch: (view: GraphView) => void;
  className?: string;
}

/** Announces "<View> of <focus name>" after any switch (click, sheet or key), and the reason a view is unavailable. */
function useAnnouncement(views: readonly GraphViewInfo[], current: GraphView, focusName: string | null) {
  const [message, setMessage] = useState("");
  const [shown, setShown] = useState(current);
  if (shown !== current) {
    setShown(current);
    const label = views.find(view => view.id === current)?.label;
    setMessage(label && focusName ? `${label} of ${focusName}` : label ?? "");
  }
  return [message, setMessage] as const;
}

function useViewChoice({ views, current, focusName, onSwitch }: SwitcherProps) {
  const [message, setMessage] = useAnnouncement(views, current, focusName);
  const unavailable = (view: GraphViewInfo) => view.needsFocus && !focusName;
  const choose = (view: GraphViewInfo) => {
    if (unavailable(view)) setMessage(NO_FOCUS);
    else if (view.id !== current) onSwitch(view.id);
  };
  return { message, unavailable, choose };
}

/** The graph header's view toolbar: one Tab stop, Left and Right move, Home and End jump. */
export function ViewSwitcher(props: SwitcherProps) {
  const { views, current, className } = props;
  const { message, unavailable, choose } = useViewChoice(props);
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  const [active, setActive] = useState<GraphView | null>(null);
  const tabStop = views.some(view => view.id === active) ? active : current;
  return (
    <>
      <div role="toolbar" aria-label="View" className={cn("inline-flex items-center gap-0.5 rounded-(--r-ctl) border border-border-strong p-0.5", className)}
        onKeyDown={event => {
          const index = rovingIndex(views.findIndex(view => view.id === tabStop), event.key, views.length);
          if (index === null) return;
          event.preventDefault();
          setActive(views[index].id);
          buttons.current[index]?.focus();
        }}>
        {views.map((view, index) => (
          <span key={view.id} className="contents">
            {view.id === "overview" && index > 0 && <span aria-hidden="true" className="mx-1 h-5 w-px bg-border-strong" />}
            <button ref={element => { buttons.current[index] = element; }} type="button"
              aria-pressed={view.id === current} aria-disabled={unavailable(view) || undefined}
              tabIndex={view.id === tabStop ? 0 : -1} title={`${view.label}, key ${keyOf(view.id)}`}
              onFocus={() => setActive(view.id)} onClick={() => choose(view)}
              className="h-[30px] rounded-[4px] px-3 text-[13px] font-medium text-fg-muted outline-none transition-colors hover:bg-raised hover:text-fg focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-background aria-pressed:bg-accent-solid aria-pressed:text-on-accent aria-disabled:cursor-not-allowed aria-disabled:text-fg-subtle aria-disabled:hover:bg-transparent aria-disabled:hover:text-fg-subtle">
              {view.label}
            </button>
          </span>
        ))}
      </div>
      <span role="status" className="sr-only">{message}</span>
    </>
  );
}

/** Below 768 px: a "View: Focus" button that opens a bottom sheet with one 56 px row per view. */
export function ViewSheet(props: SwitcherProps) {
  const { views, current, className } = props;
  const { message, unavailable, choose } = useViewChoice(props);
  const [open, setOpen] = useState(false);
  const label = views.find(view => view.id === current)?.label ?? GRAPH_VIEWS[0].label;
  return (
    <>
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetTrigger asChild>
          <Button variant="outline" className={cn("h-11 gap-2 text-sm", className)}>
            View: <b className="font-semibold">{label}</b><ChevronDown className="size-4" aria-hidden="true" />
          </Button>
        </SheetTrigger>
        <SheetContent side="bottom" aria-describedby={undefined} className="gap-1 rounded-t-xl px-4 pb-[calc(16px+env(safe-area-inset-bottom))] pt-3">
          <SheetTitle className="flex h-8 items-center font-(family-name:--font-geist-serif-regular) text-lg font-normal">View</SheetTitle>
          <ul className="border-t border-border">
            {views.map(view => (
              <li key={view.id} className="border-b border-border">
                <button type="button" aria-pressed={view.id === current} aria-disabled={unavailable(view) || undefined}
                  onClick={() => { choose(view); if (!unavailable(view)) setOpen(false); }}
                  className="grid min-h-14 w-full grid-cols-[minmax(0,1fr)_20px] items-center gap-3 px-1 py-1.5 text-left outline-none focus-visible:ring-2 focus-visible:ring-ring aria-disabled:text-fg-subtle">
                  <span>
                    <span className={cn("block text-[15px] leading-5", view.id === current ? "font-semibold" : "font-medium")}>{view.label}</span>
                    <span className="block label-12 text-fg-muted">{view.question}</span>
                  </span>
                  {view.id === current && <Check className="size-5 text-fg" aria-hidden="true" />}
                </button>
              </li>
            ))}
          </ul>
        </SheetContent>
      </Sheet>
      <span role="status" className="sr-only">{message}</span>
    </>
  );
}

const isTextField = (target: EventTarget | null) =>
  target instanceof HTMLElement && (target.isContentEditable || target.closest("input, textarea, select, [contenteditable]") !== null);

/** Keys 1 to 6 and the n, t and p aliases switch views from anywhere on the graph page. */
export function useViewKeys() {
  const router = useRouter();
  const params = useSearchParams();
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const current = new URLSearchParams(params);
      const view = keyedView(event, isTextField(event.target), parseGraphState(current));
      if (!view) return;
      event.preventDefault();
      router.push(graphNavigation(current, { view }).href, { scroll: false });
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [params, router]);
}
