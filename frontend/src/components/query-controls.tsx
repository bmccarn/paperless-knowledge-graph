"use client";

import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { ModelInfo } from "@/lib/api";

export type QueryMode = "quick" | "deep" | "timeline" | "strict";
const modes: [QueryMode, string, string][] = [
  ["quick", "Fast", "Find an answer quickly without checking claims."],
  ["deep", "Deep", "Search more sources and check the answer against them."],
  ["timeline", "Timeline", "Arrange documented events by date and check their sources."],
  ["strict", "Strict", "Show an answer only when the source checks pass."],
];

export function QueryModes({ value, onChange }: { value: QueryMode; onChange: (value: QueryMode) => void }) {
  return <div className="space-y-2">
    <ToggleGroup type="multiple" role="group" aria-label="Query mode" aria-describedby="query-mode-description"
      value={[value]} onValueChange={values => { const next = values.find(item => item !== value); if (next) onChange(next as QueryMode); }}
      className="border border-border-strong bg-surface p-1">
      {modes.map(([id, label]) => <ToggleGroupItem key={id} value={id}>{label}</ToggleGroupItem>)}
    </ToggleGroup>
    <p id="query-mode-description" className="copy-14 text-fg-muted">{modes.find(([id]) => id === value)?.[2]}</p>
  </div>;
}

export function QueryModel({ models, value, defaultModel, onChange }: {
  models: ModelInfo[]; value: string; defaultModel: string; onChange: (value: string) => void;
}) {
  return <Select value={value} onValueChange={onChange} disabled={!models.length}>
    <SelectTrigger aria-label="Model" className="w-full md:w-60"><SelectValue placeholder="Select model" /></SelectTrigger>
    <SelectContent position="popper">{models.map(model => <SelectItem key={model.id} value={model.id}>
      {model.name}{model.id === defaultModel ? " (default)" : ""}
    </SelectItem>)}</SelectContent>
  </Select>;
}
