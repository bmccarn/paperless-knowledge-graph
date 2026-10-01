import { Button } from "@/components/ui/button";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from "lucide-react";

interface PagerProps {
  page: number;
  pageCount: number;
  pageSize: number;
  total: number;
  onPage: (page: number) => void;
  disabled?: boolean;
  /** Accessible noun for the buttons, e.g. "page" → "Next page". */
  noun?: string;
}

export function Pager({ page, pageCount, pageSize, total, onPage, disabled = false, noun = "page" }: PagerProps) {
  const atStart = disabled || page === 0;
  const atEnd = disabled || page >= pageCount - 1;
  const buttons = [
    { label: `First ${noun}`, Icon: ChevronsLeft, to: 0, off: atStart },
    { label: `Previous ${noun}`, Icon: ChevronLeft, to: page - 1, off: atStart },
    { label: `Next ${noun}`, Icon: ChevronRight, to: page + 1, off: atEnd },
    { label: `Last ${noun}`, Icon: ChevronsRight, to: pageCount - 1, off: atEnd },
  ];
  return (
    <nav aria-label={`${noun} navigation`} className="flex items-center justify-between gap-2 border-t px-4 py-2">
      <p className="text-xs text-muted-foreground">
        {total ? page * pageSize + 1 : 0}–{Math.min((page + 1) * pageSize, total)} of {total}
      </p>
      <div className="flex items-center gap-1">
        {buttons.slice(0, 2).map(({ label, Icon, to, off }) => (
          <Button key={label} variant="ghost" size="icon" className="h-8 w-8" onClick={() => onPage(to)} aria-label={label} disabled={off}>
            <Icon className="h-3.5 w-3.5" />
          </Button>
        ))}
        <span className="text-xs text-muted-foreground px-2">{page + 1} / {pageCount}</span>
        {buttons.slice(2).map(({ label, Icon, to, off }) => (
          <Button key={label} variant="ghost" size="icon" className="h-8 w-8" onClick={() => onPage(to)} aria-label={label} disabled={off}>
            <Icon className="h-3.5 w-3.5" />
          </Button>
        ))}
      </div>
    </nav>
  );
}
