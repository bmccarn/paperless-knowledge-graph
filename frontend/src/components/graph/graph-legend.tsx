'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Palette, ChevronUp } from 'lucide-react';

export const NODE_PALETTE: Record<string, { color: string; label: string }> = {
  Person:           { color: '#818cf8', label: 'Person' },
  Organization:     { color: '#34d399', label: 'Organization' },
  Document:         { color: '#64748b', label: 'Document' },
  Location:         { color: '#2dd4bf', label: 'Location' },
  Address:          { color: '#22d3ee', label: 'Address' },
  MedicalResult:    { color: '#fb7185', label: 'Medical' },
  Medical_Result:   { color: '#fb7185', label: 'Medical' },
  FinancialItem:    { color: '#fbbf24', label: 'Financial' },
  Financial_Item:   { color: '#fbbf24', label: 'Financial' },
  Account:          { color: '#a78bfa', label: 'Account' },
  Date:             { color: '#fb923c', label: 'Date' },
  Event:            { color: '#f472b6', label: 'Event' },
  Phone:            { color: '#38bdf8', label: 'Phone' },
  Email:            { color: '#e879f9', label: 'Email' },
};

export const DEFAULT_NODE_COLOR = '#c084fc';

export function getNodeColor(label: string): string {
  return NODE_PALETTE[label]?.color || DEFAULT_NODE_COLOR;
}

// Deduplicate display entries (Medical_Result -> Medical shares with MedicalResult)
const legendEntries = Object.entries(NODE_PALETTE)
  .filter(([key]) => !key.includes('_')) // skip underscore variants
  .map(([, v]) => v);

export function GraphLegend() {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <>
      {/* Desktop */}
      <Card className="absolute bottom-4 right-4 z-10 w-48 hidden md:block bg-background/90 backdrop-blur-sm border-border/50">
        <CardHeader className="py-2 px-3">
          <CardTitle className="text-sm">Node Types</CardTitle>
        </CardHeader>
        <CardContent className="py-2 px-3">
          <div className="grid grid-cols-2 gap-1">
            {legendEntries.map(({ label, color }) => (
              <div key={label} className="flex items-center gap-1.5">
                <div className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
                <span className="text-xs text-muted-foreground">{label}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Mobile */}
      <div className="absolute top-2 right-2 z-10 md:hidden">
        {isExpanded ? (
          <Card className="w-44 bg-background/90 backdrop-blur-sm border-border/50">
            <CardHeader className="py-1.5 px-2 flex flex-row items-center justify-between">
              <CardTitle className="text-xs">Types</CardTitle>
              <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => setIsExpanded(false)}>
                <ChevronUp className="h-3 w-3" />
              </Button>
            </CardHeader>
            <CardContent className="py-1.5 px-2">
              <div className="grid grid-cols-2 gap-0.5">
                {legendEntries.map(({ label, color }) => (
                  <div key={label} className="flex items-center gap-1">
                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                    <span className="text-[10px] text-muted-foreground">{label}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        ) : (
          <Button variant="outline" size="sm" className="h-8 w-8 p-0 bg-background/90 backdrop-blur-sm" onClick={() => setIsExpanded(true)}>
            <Palette className="h-4 w-4" />
          </Button>
        )}
      </div>
    </>
  );
}
