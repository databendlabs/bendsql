import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { useRouter } from "next/router";
import { Layout } from "antd";
import { IGraph } from "@ant-design/charts";

import MostExpensiveNodes from './components/MostExpensiveNodes';
import ProfileOverview from './components/ProfileOverview';
import ProfileOverviewNode from './components/ProfileOverviewNode';
import Statistics from './components/Statistics';
import Attributes from './components/Attributes';
import FlowAnalysisGraph from './components/FlowAnalysisGraph';

import { transformErrors, getPercent } from "./utills";
import { IGraphSize, IOverview, Profile, IStatisticsDesc, IErrors, MessageResponse, StatisticsData, AttributeData } from "./types/ProfileGraphDashboard";
import { ALL_NODE_ID } from "./constants";

const { Content, Sider } = Layout;

const CPU_TIME_KEY = "CpuTime";
const WAIT_TIME_KEY = "WaitTime";

const ProfileGraphDashboard: React.FC = () => {
  const router = useRouter();
  
  // Get perf ID from URL parameters
  const pathPerfId = router.query.slug && Array.isArray(router.query.slug)
    ? router.query.slug.join('/')
    : router.query.slug;
  const perfId = pathPerfId || '0';

  // Basic state
  const [selectedNodeId, setSelectedNodeId] = useState<string>(ALL_NODE_ID);
  const [plainData, setPlainData] = useState<Profile[]>([]);
  const [rangeData, setRangeData] = useState<Profile[]>([]);
  const [statisticsData, setStatisticsData] = useState<StatisticsData[]>([]);
  const [labels, setLabels] = useState<AttributeData[]>([]);
  const [overviewInfo, setOverviewInfo] = useState<IOverview | undefined>(undefined);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [graphSize, setGraphSize] = useState<IGraphSize>({ width: 800, height: 600 });

  // Refs
  const profileRef = useRef<HTMLDivElement | null>(null);
  const profileWrapRef = useRef<HTMLDivElement | null>(null);
  const profileWrapRefCanvas = useRef<HTMLCanvasElement | null>(null);
  const graphRef = useRef<IGraph | null>(null);
  const overviewInfoCurrent = useRef<IOverview | undefined>(undefined);

  // Data transformation utilities (moved inline for simplicity)
  const transformProfiles = useCallback((profiles: Profile[], statistics_desc: any) => {
    const cpuTimeIndex = statistics_desc[CPU_TIME_KEY]?.index;
    const waitTimeIndex = statistics_desc[WAIT_TIME_KEY]?.index;
    let cpuTime = 0;
    let waitTime = 0;

    profiles.forEach(item => {
      item.id = String(item.id);
      item.parent_id = String(item.parent_id);
      const cpuT = item?.statistics[cpuTimeIndex] || 0;
      const waitT = item?.statistics[waitTimeIndex] || 0;
      item.totalTime = cpuT + waitT;
      item.cpuTime = cpuT;
      item.waitTime = waitT;
      cpuTime += cpuT;
      waitTime += waitT;
      item.errors = item?.errors?.length > 0 ? transformErrors(item?.errors) : [];
      
      // Create statistics description array
      item.statisticsDescArray = Object.entries(statistics_desc).map(
        ([_type, descObj]: [string, any]) => ({
          _type,
          desc: descObj?.desc,
          display_name: descObj?.display_name || descObj?.displayName,
          index: descObj?.index,
          unit: descObj.unit,
          plain_statistics: descObj?.plain_statistics,
          _value: item.statistics[descObj?.index],
        })
      );
    });

    const totalTime = cpuTime + waitTime;
    profiles.forEach(item => {
      item.totalTimePercent = getPercent(item?.totalTime, totalTime);
      item.cpuTimePercent = getPercent(item?.cpuTime, item.totalTime);
      item.waitTimePercent = getPercent(item?.waitTime, item.totalTime);
    });

    return profiles;
  }, []);

  const calculateOverviewInfo = useCallback((profiles: Profile[]) => {
    const cpuTime = profiles.reduce((sum: number, item: Profile) => sum + item.cpuTime, 0);
    const waitTime = profiles.reduce((sum: number, item: Profile) => sum + item.waitTime, 0);
    const totalTime = cpuTime + waitTime;
    const cpuTimePercent = getPercent(cpuTime, totalTime);
    const waitTimePercent = getPercent(waitTime, totalTime);

    return {
      cpuTime,
      waitTime,
      totalTime,
      totalTimePercent: "100%",
      cpuTimePercent,
      waitTimePercent,
      statisticsDescArray: [],
      errors: [],
    };
  }, []);

  // Handle window resize
  useEffect(() => {
    const handleResize = () => {
      if (profileRef.current) {
        const rect = profileRef.current.getBoundingClientRect();
        setGraphSize({ width: rect.width, height: rect.height });
      }
    };

    handleResize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Load data from API
  useEffect(() => {
    const fetchData = async () => {
      if (!router.isReady) return;

      try {
        setIsLoading(true);
        const response = await fetch(`/api/message?perf_id=${perfId}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result: MessageResponse = await response.json();
        const data = JSON.parse(result?.result);

        const profiles = transformProfiles(data.profiles, data.statistics_desc);
        const overview = calculateOverviewInfo(profiles);

        setPlainData(profiles);
        setRangeData(profiles
          .filter(item => parseFloat(item.totalTimePercent) > 0)
          .sort((a, b) => b.totalTime - a.totalTime)
        );
        setOverviewInfo(overview);
        overviewInfoCurrent.current = overview;

        // Generate statistics and labels data
        setStatisticsData(profiles.map(profile => {
          const statistics = Object.entries(data.statistics_desc).map(([key, value]: [string, any]) => ({
            name: value.display_name || key,
            desc: value.desc,
            value: profile.statistics[value.index],
            unit: value.unit,
          }));
          return { statistics, id: profile?.id?.toString() || '' };
        }));

        setLabels(profiles.map(profile => ({
          labels: profile.labels,
          id: profile?.id?.toString() || '',
        })));
      } catch (error) {
        console.error("Error fetching data:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [router.isReady, perfId, transformProfiles, calculateOverviewInfo]);

  // Node selection handler
  const handleNodeSelection = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
    
    if (!graphRef.current) return;
    const graph = graphRef.current;
    const nodes = graph.getNodes();
    
    // Clear all active states first
    nodes?.forEach(n => {
      graph.clearItemStates(n);
    });
    
    // Set active state for selected node
    const node = nodes?.find(n => n?._cfg?.id === nodeId);
    if (node) {
      graph.setItemState(node, 'highlight', true);
    }
  }, []);

  // Graph events setup
  const bindGraphEvents = useCallback((graph: IGraph) => {
    const handleNodeClick = (evt: any) => {
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
      const nodes = graph.getNodes();
      nodes?.forEach(n => {
        graph.clearItemStates(n);
      });
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
  }, [handleNodeSelection]);

  // Get overview info for selected node
  const getSelectedNodeOverview = useMemo((): IOverview | undefined => {
    if (selectedNodeId === ALL_NODE_ID) return overviewInfo;
    const selectedProfile = plainData.find(item => item.id === selectedNodeId);
    if (!selectedProfile) return undefined;
    
    return {
      cpuTime: selectedProfile.cpuTime,
      waitTime: selectedProfile.waitTime,
      totalTime: selectedProfile.totalTime,
      totalTimePercent: selectedProfile.totalTimePercent,
      cpuTimePercent: selectedProfile.cpuTimePercent,
      waitTimePercent: selectedProfile.waitTimePercent,
      labels: selectedProfile.labels,
      statisticsDescArray: selectedProfile.statisticsDescArray as IStatisticsDesc[],
      errors: selectedProfile.errors as IErrors[],
      name: selectedProfile.name,
    };
  }, [selectedNodeId, overviewInfo, plainData]);

  // Render loading state
  if (isLoading) {
    return (
      <div className="h-screen flex items-center justify-center">
        <div className="text-lg">Loading performance data...</div>
      </div>
    );
  }

  return (
    <Layout>
      <div ref={profileRef} className="bg-white w-full rounded-lg">
        <Layout>
          <Content className="p-6 w-full flex">
            {/* Graph Panel */}
            <div
              ref={profileWrapRef}
              className="flex-1 flex justify-center items-center h-screen"
            >
              <FlowAnalysisGraph
                plainData={plainData}
                graphSize={graphSize}
                graphRef={graphRef}
                overviewInfoCurrent={overviewInfoCurrent}
                onReady={(graph: IGraph) => {
                  if (isLoading) {
                    graph.fitView();
                    graph.refresh();
                    setIsLoading(false);
                  } else {
                    graphRef.current = graph;
                    graph.setMaxZoom(2);
                    graph.setMinZoom(0.5);
                    bindGraphEvents(graph);
                  }
                }}
              />
            </div>

            {/* Sidebar Panel */}
            <Sider width={308} style={{ background: "#fff" }}>
              <div className="overflow-y-auto" style={{ height: graphSize.height }}>
                <MostExpensiveNodes
                  data={rangeData}
                  plainData={plainData}
                  selectedNodeId={selectedNodeId}
                  handleNodeSelection={handleNodeSelection}
                />
                {selectedNodeId !== ALL_NODE_ID ? (
                  <>
                    <ProfileOverviewNode overviewInfo={getSelectedNodeOverview} />
                    <Statistics statisticsData={statisticsData.find((stat) => stat.id === selectedNodeId)!} />
                    <Attributes attributesData={labels.find((label) => label.id === selectedNodeId)?.labels!} />
                  </>
                ) : (
                  <ProfileOverview queryDuration={overviewInfo?.totalTime || 0} overviewInfo={overviewInfo} />
                )}
              </div>
            </Sider>
          </Content>
        </Layout>
      </div>
    </Layout>
  );
};

export default ProfileGraphDashboard;