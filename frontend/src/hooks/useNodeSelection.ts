import { useCallback } from "react";
import { IGraph } from "@ant-design/charts";
import { Profile } from "../types/ProfileGraphDashboard";

export function useNodeSelection(
  graphRef: React.RefObject<IGraph>,
  plainData: Profile[],
  setSelectedNodeId: React.Dispatch<React.SetStateAction<string>>,
): {
  handleNodeSelection: (nodeId: string) => void;
} {
  const setNodeActiveState = useCallback((nodeId: string) => {
    if (!graphRef.current) return;
    const graph: IGraph = graphRef.current;
    const nodes = graph?.getNodes();
    
    // Clear all active states first
    nodes?.forEach(n => {
      graph?.clearItemStates(n);
    });
    
    // Set active state for selected node
    const node = nodes?.find(n => n?._cfg?.id === nodeId);
    if (node) {
      graph?.setItemState(node, 'highlight', true);
    }
  }, [graphRef]);

  const handleNodeSelection = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
    setNodeActiveState(nodeId);
  }, [setSelectedNodeId, setNodeActiveState]);

  return { handleNodeSelection };
}
