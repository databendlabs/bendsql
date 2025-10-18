import { useCallback, useEffect, useState } from "react";
import { IG6GraphEvent, IGraph, Item } from "@ant-design/charts";

import { ALL_NODE_ID } from "../constants";
import { IOverview, Profile } from "../types/ProfileGraphDashboard";

export function useGraphEvents(
  plainData: Profile[],
  setOverInfo: React.Dispatch<React.SetStateAction<IOverview>>,
  setSelectedNodeId: React.Dispatch<React.SetStateAction<string>>,
  profileWrapRefCanvas: React.MutableRefObject<HTMLCanvasElement>,
  profileWrapRef: React.RefObject<HTMLDivElement>,
  overviewInfoCurrent: React.RefObject<IOverview | undefined>,
  setOverviewInfo: React.Dispatch<React.SetStateAction<IOverview | undefined>>,
) {
  const getAllNodes = useCallback((graph: IGraph) => {
    return graph?.getNodes();
  }, []);

  const setNodeActive = useCallback((graph: IGraph, node?: Item | string) => {
    if (node) {
      graph?.setItemState(node, "highlight", true);
    }
  }, []);

  const clearNodeActive = useCallback((graph: IGraph) => {
    getAllNodes(graph)?.forEach(n => {
      graph?.clearItemStates(n);
    });
  }, [getAllNodes]);

  const bindGraphEvents = useCallback((graph: IGraph) => {
    const handleNodeClick = (evt: IG6GraphEvent) => {
      const modal = evt.item?._cfg?.model;
      setOverInfo({
        ...plainData.find(item => item.id === modal?.id),
      } as IOverview);
      setSelectedNodeId(modal?.id as string);

      const nodes = getAllNodes(graph);
      const id = evt.item?._cfg?.id;
      const node = nodes?.find(node => node?._cfg?.id === id);
      nodes
        ?.filter(node => node?._cfg?.id !== id)
        .forEach(n => {
          graph?.clearItemStates(n);
        });

      setNodeActive(graph, node);
    };

    const handleNodeMouseLeave = () => {
      if (!profileWrapRefCanvas.current) {
        profileWrapRefCanvas.current = document.getElementsByTagName("canvas")[0];
      }
      profileWrapRefCanvas.current.style.cursor = "move";
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

  }, [profileWrapRefCanvas, profileWrapRef, overviewInfoCurrent, setOverviewInfo, getAllNodes, setNodeActive, clearNodeActive, plainData, setOverInfo, setSelectedNodeId]);

  return {
    bindGraphEvents,
  };
}
