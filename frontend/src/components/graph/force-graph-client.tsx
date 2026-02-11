'use client';

import { useEffect, useRef, useState, useCallback } from 'react';
import { Loader2 } from 'lucide-react';

interface ForceGraphClientProps {
  fgRef?: React.RefObject<any>;
  is3DMode: boolean;
  graphData: any;
  nodeLabel?: (node: any) => string;
  nodeColor?: (node: any) => string;
  nodeVal?: (node: any) => number;
  nodeRelSize?: number;
  linkDirectionalArrowLength?: number;
  linkDirectionalArrowRelPos?: number;
  linkDirectionalArrowColor?: string | ((link: any) => string);
  linkLabel?: string | ((link: any) => string);
  linkColor?: string | ((link: any) => string);
  linkWidth?: number | ((link: any) => number);
  onNodeClick?: (node: any) => void;
  onNodeHover?: (node: any) => void;
  onNodeRightClick?: (node: any) => void;
  onBackgroundClick?: () => void;
  warmupTicks?: number;
  cooldownTicks?: number;
  backgroundColor?: string;
  d3AlphaDecay?: number;
  d3VelocityDecay?: number;
  nodeOpacity?: number;
  linkOpacity?: number;
  nodeResolution?: number;
  enableNodeDrag?: boolean;
  enableNavigationControls?: boolean;
  showNavInfo?: boolean;
  nodeCanvasObject?: (node: any, ctx: CanvasRenderingContext2D, globalScale: number) => void;
  nodeCanvasObjectMode?: () => string;
  nodePointerAreaPaint?: (node: any, color: string, ctx: CanvasRenderingContext2D) => void;
  linkCanvasObjectMode?: () => string;
  linkCanvasObject?: (link: any, ctx: CanvasRenderingContext2D, globalScale: number) => void;
  onEngineInit?: (fg: any) => void;
  linkDirectionalParticles?: number | ((link: any) => number);
  linkDirectionalParticleSpeed?: number | ((link: any) => number);
  linkDirectionalParticleWidth?: number;
  linkDirectionalParticleColor?: (link: any) => string;
  linkCurvature?: number | ((link: any) => number);
}

function isWebGLAvailable(): boolean {
  try {
    const canvas = document.createElement('canvas');
    return !!(canvas.getContext('webgl') || canvas.getContext('experimental-webgl'));
  } catch {
    return false;
  }
}

function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result
    ? { r: parseInt(result[1], 16) / 255, g: parseInt(result[2], 16) / 255, b: parseInt(result[3], 16) / 255 }
    : { r: 1, g: 1, b: 1 };
}

export function ForceGraphClient({ fgRef, is3DMode, graphData, ...props }: ForceGraphClientProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const localRef = useRef<any>(null);
  const threeRef = useRef<any>(null);
  const sceneInitialized = useRef(false);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const [GraphComponent, setGraphComponent] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const graphRef = fgRef || localRef;

  useEffect(() => {
    let mounted = true;
    const loadGraph = async () => {
      await new Promise(resolve => requestAnimationFrame(resolve));
      if (!mounted) return;
      if (!isWebGLAvailable()) {
        setError('WebGL is not available.');
        setIsLoading(false);
        return;
      }
      try {
        if (is3DMode) {
          const [module, THREE] = await Promise.all([
            import('react-force-graph-3d'),
            import('three'),
          ]);
          if (mounted) { threeRef.current = THREE; setGraphComponent(() => module.default); }
        } else {
          const module = await import('react-force-graph-2d');
          if (mounted) setGraphComponent(() => module.default);
        }
        if (mounted) setIsLoading(false);
      } catch (err) {
        if (mounted) { setError(err instanceof Error ? err.message : 'Failed to load graph'); setIsLoading(false); }
      }
    };
    setIsLoading(true);
    setGraphComponent(null);
    setError(null);
    sceneInitialized.current = false;
    loadGraph();
    return () => { mounted = false; };
  }, [is3DMode]);

  useEffect(() => {
    if (!containerRef.current) return;
    const updateDimensions = () => {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect();
        setDimensions({ width: rect.width || 800, height: rect.height || 600 });
      }
    };
    updateDimensions();
    const observer = new ResizeObserver(updateDimensions);
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const nodeThreeObject = useCallback((node: any) => {
    const THREE = threeRef.current;
    if (!THREE) return null;
    const size = (node.val || 1) * 14;
    const color = node.color || '#ffffff';
    const rgb = hexToRgb(color);
    const geometry = new THREE.SphereGeometry(size, 32, 32);
    const material = new THREE.MeshStandardMaterial({
      color: new THREE.Color(rgb.r, rgb.g, rgb.b),
      roughness: 0.4,
      metalness: 0.1,
      envMapIntensity: 0.5,
    });
    return new THREE.Mesh(geometry, material);
  }, []);

  if (isLoading || !GraphComponent) {
    return (
      <div ref={containerRef} className="h-full w-full flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }
  if (error) {
    return (
      <div ref={containerRef} className="h-full w-full flex items-center justify-center">
        <p className="text-destructive">{error}</p>
      </div>
    );
  }

  const {
    nodeOpacity, linkOpacity, nodeResolution, enableNodeDrag, enableNavigationControls, showNavInfo,
    nodeCanvasObject, nodeCanvasObjectMode, nodePointerAreaPaint,
    linkCanvasObject, linkCanvasObjectMode,
    d3AlphaDecay, d3VelocityDecay, onEngineInit,
    linkDirectionalParticles, linkDirectionalParticleSpeed, linkDirectionalParticleWidth, linkDirectionalParticleColor,
    linkCurvature, ...commonProps
  } = props;

  const graphProps = is3DMode
    ? {
        ...commonProps,
        nodeOpacity, linkOpacity,
        nodeResolution: nodeResolution || 20,
        enableNodeDrag, enableNavigationControls, showNavInfo,
        d3AlphaDecay, d3VelocityDecay,
        nodeThreeObject, nodeThreeObjectExtend: false,
        linkWidth: 0.8, linkCurvature,
        linkDirectionalParticles, linkDirectionalParticleSpeed, linkDirectionalParticleWidth, linkDirectionalParticleColor,
      }
    : {
        ...commonProps,
        nodeCanvasObject, nodeCanvasObjectMode, nodePointerAreaPaint,
        linkCanvasObject, linkCanvasObjectMode,
        d3AlphaDecay, d3VelocityDecay,
        linkCurvature,
        linkDirectionalParticles, linkDirectionalParticleSpeed, linkDirectionalParticleWidth, linkDirectionalParticleColor,
      };

  const setGraphRefCb = (instance: any) => {
    if (graphRef && 'current' in graphRef) {
      (graphRef as React.MutableRefObject<any>).current = instance;
    }
    if (instance && !sceneInitialized.current) {
      sceneInitialized.current = true;
      setTimeout(() => {
        if (onEngineInit) onEngineInit(instance);
        if (is3DMode) setup3DScene(instance);
      }, 150);
    }
  };

  return (
    <div ref={containerRef} className="h-full w-full relative" style={{ zIndex: 1 }}>
      <GraphComponent ref={setGraphRefCb} graphData={graphData} width={dimensions.width} height={dimensions.height} {...graphProps} />
    </div>
  );
}

async function setup3DScene(instance: any) {
  try {
    const THREE = await import('three');
    const scene = instance.scene();
    if (!scene) return;
    const lightsToRemove: any[] = [];
    scene.traverse((child: any) => { if (child.isLight && child.userData?.custom) lightsToRemove.push(child); });
    lightsToRemove.forEach((l: any) => scene.remove(l));

    const ambient = new THREE.AmbientLight(0xffffff, 0.6);
    ambient.userData = { custom: true };
    scene.add(ambient);

    const main = new THREE.DirectionalLight(0xffffff, 1.0);
    main.position.set(100, 150, 100);
    main.userData = { custom: true };
    scene.add(main);

    const fill = new THREE.DirectionalLight(0xffffff, 0.3);
    fill.position.set(-100, -50, -100);
    fill.userData = { custom: true };
    scene.add(fill);

    const hemi = new THREE.HemisphereLight(0xffffff, 0x444444, 0.4);
    hemi.userData = { custom: true };
    scene.add(hemi);
  } catch (err) {
    console.warn('Failed to setup 3D scene:', err);
  }
}

export default ForceGraphClient;
