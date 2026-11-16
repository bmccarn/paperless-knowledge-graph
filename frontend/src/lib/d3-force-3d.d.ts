/** The part of d3-force-3d (no published types) that the Overview layout uses. */
declare module "d3-force-3d" {
  interface Simulation {
    force(name: string, force: unknown): Simulation;
    stop(): Simulation;
    tick(iterations?: number): Simulation;
  }
  export function forceSimulation(nodes: object[], numDimensions?: number): Simulation;
  export function forceLink(links: { source: number; target: number }[]): unknown;
  export function forceManyBody(): unknown;
  export function forceCenter(): unknown;
  export function forceCollide(radius: (node: { index: number }) => number): unknown;
}
