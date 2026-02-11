'use client';

import { useState } from 'react';
import { Search, Loader2, Box, Square, Maximize2, Minimize2, Focus, RotateCcw, Settings2, ChevronUp, ChevronDown, Tag, Atom } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { Checkbox } from '@/components/ui/checkbox';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useGraphStore } from '@/lib/stores/graph-store';
import { NODE_PALETTE, getNodeColor } from './graph-legend';

interface GraphControlsProps {
  nodeCount: number;
  linkCount: number;
  nodeTypes: Set<string>;
  hiddenTypes: Set<string>;
  onToggleType: (type: string) => void;
  onSearch: (query: string) => void;
  searchResults: Array<{ labels: string[]; properties: Record<string, unknown> }>;
  onAddFromSearch: (result: { labels: string[]; properties: Record<string, unknown> }) => void;
  onClearSearch: () => void;
  loading: boolean;
  isFullscreen: boolean;
  onToggleFullscreen: () => void;
  onResetView: () => void;
  showLabels: boolean;
  onToggleLabels: () => void;
  showLegend: boolean;
  onToggleLegend: () => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
}

export function GraphControls({
  nodeCount, linkCount, nodeTypes, hiddenTypes, onToggleType,
  onSearch, searchResults, onAddFromSearch, onClearSearch, loading,
  isFullscreen, onToggleFullscreen, onResetView,
  showLabels, onToggleLabels, showLegend, onToggleLegend,
  onZoomIn, onZoomOut,
}: GraphControlsProps) {
  const { is3DMode, toggle3DMode, searchQuery, setSearchQuery } = useGraphStore();
  const [mobileExpanded, setMobileExpanded] = useState(false);

  const handleSearchSubmit = () => {
    if (searchQuery.trim()) onSearch(searchQuery);
  };

  return (
    <>
      {/* Top bar */}
      <div className="absolute top-0 left-0 right-0 z-20 border-b px-4 py-2.5 flex items-center gap-3 bg-card/80 backdrop-blur-sm">
        <h1 className="text-base font-semibold whitespace-nowrap hidden md:block">Graph Explorer</h1>
        <div className="flex-1 flex gap-2 max-w-md ml-2 relative">
          <div className="relative flex-1">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearchSubmit()}
              placeholder="Search nodes..."
              className="pl-8 h-8 text-sm"
            />
          </div>
          <Button onClick={handleSearchSubmit} size="sm" disabled={loading} className="h-8">
            {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Search className="h-3.5 w-3.5" />}
          </Button>
          {searchResults.length > 0 && (
            <div className="absolute z-50 top-full mt-1 left-0 right-0 bg-popover border rounded-lg shadow-xl overflow-hidden">
              <ScrollArea className="max-h-64">
                {searchResults.map((r, i) => {
                  const p = r.properties;
                  return (
                    <button key={i} className="w-full text-left px-3 py-2 text-sm hover:bg-accent/70 flex items-center gap-2" onClick={() => onAddFromSearch(r)}>
                      <span className="h-2.5 w-2.5 rounded-full shrink-0" style={{ backgroundColor: getNodeColor(r.labels?.[0] || '') }} />
                      <span className="truncate flex-1">{(p.name as string) || (p.title as string)}</span>
                      <Badge variant="secondary" className="text-[10px] shrink-0">{r.labels?.[0]}</Badge>
                    </button>
                  );
                })}
              </ScrollArea>
              <button className="w-full text-center py-1.5 text-xs text-muted-foreground hover:bg-accent/50 border-t" onClick={onClearSearch}>Close</button>
            </div>
          )}
        </div>
        <Badge variant="secondary" className="text-xs font-mono shrink-0">
          {nodeCount}N · {linkCount}E
        </Badge>
      </div>

      {/* Bottom toolbar */}
      <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-20 flex items-center gap-1 bg-card/90 backdrop-blur-md border rounded-lg px-2 py-1.5 shadow-lg">
        <Tooltip><TooltipTrigger asChild>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onZoomIn}><span className="text-sm font-bold">+</span></Button>
        </TooltipTrigger><TooltipContent>Zoom in</TooltipContent></Tooltip>
        <Tooltip><TooltipTrigger asChild>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onZoomOut}><span className="text-sm font-bold">−</span></Button>
        </TooltipTrigger><TooltipContent>Zoom out</TooltipContent></Tooltip>
        <Tooltip><TooltipTrigger asChild>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onResetView}><Focus className="h-3.5 w-3.5" /></Button>
        </TooltipTrigger><TooltipContent>Fit to view</TooltipContent></Tooltip>
        <div className="w-px h-4 bg-border mx-0.5" />
        <Tooltip><TooltipTrigger asChild>
          <Button variant={is3DMode ? 'secondary' : 'ghost'} size="icon" className="h-7 w-7" onClick={toggle3DMode}>
            {is3DMode ? <Box className="h-3.5 w-3.5" /> : <Square className="h-3.5 w-3.5" />}
          </Button>
        </TooltipTrigger><TooltipContent>{is3DMode ? 'Switch to 2D' : 'Switch to 3D'}</TooltipContent></Tooltip>
        <div className="w-px h-4 bg-border mx-0.5" />
        <Tooltip><TooltipTrigger asChild>
          <Button variant={showLabels ? 'secondary' : 'ghost'} size="icon" className="h-7 w-7" onClick={onToggleLabels}>
            <Tag className="h-3.5 w-3.5" />
          </Button>
        </TooltipTrigger><TooltipContent>Toggle labels</TooltipContent></Tooltip>
        <Tooltip><TooltipTrigger asChild>
          <Button variant={showLegend ? 'secondary' : 'ghost'} size="icon" className="h-7 w-7" onClick={onToggleLegend}>
            <Atom className="h-3.5 w-3.5" />
          </Button>
        </TooltipTrigger><TooltipContent>Toggle legend</TooltipContent></Tooltip>
        <div className="w-px h-4 bg-border mx-0.5" />
        <Tooltip><TooltipTrigger asChild>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onToggleFullscreen}>
            {isFullscreen ? <Minimize2 className="h-3.5 w-3.5" /> : <Maximize2 className="h-3.5 w-3.5" />}
          </Button>
        </TooltipTrigger><TooltipContent>{isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}</TooltipContent></Tooltip>
      </div>

      {/* Legend overlay (top-left) */}
      {showLegend && nodeTypes.size > 0 && (
        <div className="absolute top-14 left-3 z-20 bg-card/90 backdrop-blur-md border rounded-lg px-3 py-2.5 shadow-lg">
          <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1.5">Legend</p>
          <div className="space-y-1">
            {[...nodeTypes].sort().map((type) => (
              <label key={type} className="flex items-center gap-2 text-xs cursor-pointer hover:text-foreground transition-colors">
                <Checkbox
                  checked={!hiddenTypes.has(type)}
                  onCheckedChange={() => onToggleType(type)}
                  className="h-3.5 w-3.5"
                />
                <span className="h-2.5 w-2.5 rounded-full shrink-0" style={{ backgroundColor: getNodeColor(type) }} />
                <span className={hiddenTypes.has(type) ? 'text-muted-foreground line-through' : ''}>
                  {NODE_PALETTE[type]?.label || type}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}
    </>
  );
}
