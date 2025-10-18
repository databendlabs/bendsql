import { useCallback } from "react";
import type { Dispatch, MutableRefObject, RefObject, SetStateAction } from "react";
import { IG6GraphEvent, IGraph } from "@ant-design/charts";

import { ALL_NODE_ID } from "../constants";
import { IOverview } from "../types/ProfileGraphDashboard";

export function useGraphEvents(
  handleNodeSelection: (nodeId: string) => void,
  setSelectedNodeId: Dispatch<SetStateAction<string>>,
  profileWrapRefCanvas: MutableRefObject<HTMLCanvasElement | null>,
  profileWrapRef: MutableRefObject<HTMLDivElement | null>,
  overviewInfoCurrent: RefObject<IOverview | undefined>,
  setOverviewInfo: Dispatch<SetStateAction<IOverview | undefined>>,
) {
  const bindGraphEvents = useCallback((graph: IGraph) => {
    const getAllNodes = (graph: IGraph) => {
      return graph?.getNodes();
    };

    const clearNodeActive = (graph: IGraph) => {
      getAllNodes(graph)?.forEach(n => {
        graph?.clearItemStates(n);
      });
    };

    const handleNodeClick = (evt: IG6GraphEvent) => {
      const modal = evt.item?._cfg?.model;
      const nodeId = modal?.id != null ? String(modal.id) : undefined;
      if (!nodeId) return;

      handleNodeSelection(nodeId);
    };

    const handleNodeMouseLeave = () => {
      if (!profileWrapRefCanvas.current) {
        profileWrapRefCanvas.current = document.getElementsByTagName("canvas")[0] ?? null;
      }
      if (profileWrapRefCanvas.current) {
        profileWrapRefCanvas.current.style.cursor = "move";
      }
    };

    const handleCanvasClick = () => {
      setSelectedNodeId(ALL_NODE_ID);
      setOverviewInfo(overviewInfoCurrent.current || undefined);
      clearNodeActive(graph);
    };

    const handleCanvasDragStart = () => {
      if (profileWrapRef?.current) {
        profileWrapRef.current.style.userSelect = "none";
      }
    };

    const handleCanvasDragEnd = () => {
      if (profileWrapRef?.current) {
        profileWrapRef.current.style.userSelect = "unset";
      }
    };

    graph.on("node:click", handleNodeClick);
    graph.on("node:mouseleave", handleNodeMouseLeave);
    graph.on("canvas:click", handleCanvasClick);
    graph.on("canvas:dragstart", handleCanvasDragStart);
    graph.on("canvas:dragend", handleCanvasDragEnd);

  }, [profileWrapRefCanvas, profileWrapRef, overviewInfoCurrent, setOverviewInfo, handleNodeSelection, setSelectedNodeId]);

  return {
    bindGraphEvents,
  };
}
