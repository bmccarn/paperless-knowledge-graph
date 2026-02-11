import { create } from 'zustand';

export type ColorByOption = 'type' | 'community';
export type SizeByOption = 'uniform' | 'degree';

interface GraphState {
  selectedNodeId: string | null;
  hoveredNodeId: string | null;
  is3DMode: boolean;
  colorBy: ColorByOption;
  sizeBy: SizeByOption;
  hideIsolatedNodes: boolean;
  minDegree: number;
  searchQuery: string;
  typeFilter: string | null;

  selectNode: (id: string | null) => void;
  setHoveredNode: (id: string | null) => void;
  toggle3DMode: () => void;
  setColorBy: (colorBy: ColorByOption) => void;
  setSizeBy: (sizeBy: SizeByOption) => void;
  setHideIsolatedNodes: (hide: boolean) => void;
  setMinDegree: (degree: number) => void;
  setSearchQuery: (query: string) => void;
  setTypeFilter: (type: string | null) => void;
  reset: () => void;
}

export const useGraphStore = create<GraphState>((set) => ({
  selectedNodeId: null,
  hoveredNodeId: null,
  is3DMode: true,
  colorBy: 'type',
  sizeBy: 'degree',
  hideIsolatedNodes: false,
  minDegree: 0,
  searchQuery: '',
  typeFilter: null,

  selectNode: (id) => set({ selectedNodeId: id }),
  setHoveredNode: (id) => set({ hoveredNodeId: id }),
  toggle3DMode: () => set((s) => ({ is3DMode: !s.is3DMode })),
  setColorBy: (colorBy) => set({ colorBy }),
  setSizeBy: (sizeBy) => set({ sizeBy }),
  setHideIsolatedNodes: (hide) => set({ hideIsolatedNodes: hide }),
  setMinDegree: (degree) => set({ minDegree: degree }),
  setSearchQuery: (query) => set({ searchQuery: query }),
  setTypeFilter: (type) => set({ typeFilter: type }),
  reset: () => set({ searchQuery: '', typeFilter: null, hideIsolatedNodes: false, minDegree: 0 }),
}));
