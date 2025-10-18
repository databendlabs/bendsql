import { useState, useEffect, useRef, useCallback } from "react";
import { useRouter } from "next/router";
import { clone } from "lodash-es";
import { transformErrors, getPercent } from "../utills";
import { Profile, StatisticsDesc, StatisticsData, AttributeData, IOverview, MessageResponse } from "../types/ProfileGraphDashboard";

const CPU_TIME_KEY = "CpuTime";
const WAIT_TIME_KEY = "WaitTime";

export function useProfileData(): {
  plainData: Profile[];
  rangeData: Profile[];
  statisticsData: StatisticsData[];
  labels: AttributeData[];
  overviewInfo: IOverview | undefined;
  setOverviewInfo: React.Dispatch<React.SetStateAction<IOverview | undefined>>;
  isLoading: boolean;
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  overviewInfoCurrent: React.RefObject<IOverview | undefined>;
} {
  const router = useRouter();
  const [plainData, setPlainData] = useState<Profile[]>([]);
  const [rangeData, setRangeData] = useState<Profile[]>([]);
  const [statisticsData, setStatisticsData] = useState<StatisticsData[]>([]);
  const [labels, setLabels] = useState<AttributeData[]>([]);
  const [overviewInfo, setOverviewInfo] = useState<IOverview | undefined>(undefined);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const overviewInfoCurrent = useRef<IOverview | undefined>(undefined);

  const createStatisticsDescArray = useCallback((item: Profile, statistics_desc: StatisticsDesc) => {
    return Object.entries(statistics_desc).map(
      ([_type, descObj]) => ({
        _type,
        desc: descObj?.desc,
        display_name: descObj?.display_name || descObj?.displayName,
        index: descObj?.index,
        unit: descObj.unit,
        plain_statistics: descObj?.plain_statistics,
        _value: item.statistics[descObj?.index],
      })
    );
  }, []);

  const transformProfiles = useCallback((profiles: Profile[], statistics_desc: StatisticsDesc) => {
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
      item.statisticsDescArray = createStatisticsDescArray(item, statistics_desc);
    });

    const totalTime = cpuTime + waitTime;
    profiles.forEach(item => {
      item.totalTimePercent = getPercent(item?.totalTime, totalTime);
      item.cpuTimePercent = getPercent(item?.cpuTime, item.totalTime);
      item.waitTimePercent = getPercent(item?.waitTime, item.totalTime);
    });

    return profiles;
  }, [createStatisticsDescArray]);

  const calculateOverviewInfo = useCallback((profiles: Profile[], statistics_desc: StatisticsDesc) => {
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

  const getRangeData = useCallback((profiles: Profile[]) => {
    return clone(profiles)
      ?.filter(item => parseFloat(item.totalTimePercent) > 0)
      ?.sort((a, b) => b.totalTime - a.totalTime);
  }, []);

  const getStatisticsData = useCallback((profiles: Profile[], statistics_desc: StatisticsDesc) => {
    return profiles.map(profile => {
      const statistics = Object.entries(statistics_desc).map(([key, value]) => ({
        name: value.display_name || key,
        desc: value.desc,
        value: profile.statistics[value.index],
        unit: value.unit,
      }));
      return { statistics, id: profile?.id?.toString() };
    });
  }, []);

  const getLabels = useCallback((profiles: Profile[]) => {
    return profiles.map(profile => ({
      labels: profile.labels,
      id: profile?.id?.toString(),
    }));
  }, []);

  // Extract the complex expression to a variable for easier static analysis
  const slugString = Array.isArray(router.query.slug) ? router.query.slug.join('/') : router.query.slug;

  useEffect(() => {
    const fetchMessage = async () => {
      try {
        const pathPerfId = router.query.slug && Array.isArray(router.query.slug)
          ? router.query.slug.join('/')
          : router.query.slug;
        const perf_id = pathPerfId || '0';

        const response: Response = await fetch(`/api/message?perf_id=${perf_id}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result: MessageResponse = await response.json();

        const data = JSON.parse(result?.result);

        const profiles = transformProfiles(data.profiles, data.statistics_desc);
        const overviewInfo = calculateOverviewInfo(profiles, data.statistics_desc);

        setPlainData(profiles);
        setRangeData(getRangeData(profiles));
        setOverviewInfo(overviewInfo);
        overviewInfoCurrent.current = overviewInfo;

        setStatisticsData(getStatisticsData(data.profiles, data.statistics_desc) as StatisticsData[]);
        setLabels(getLabels(data.profiles) as AttributeData[]);
      } catch (error) {
        console.error("Error fetching message:", error);
      } finally {
        setIsLoading(false);
      }
    };

    if (router.isReady) {
      setIsLoading(true);
      fetchMessage();
    }
    // Only depend on the serialized slug string and perf_id, not the callback functions
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    router.isReady,
    slugString
  ]);

  return {
    plainData,
    rangeData,
    statisticsData,
    labels,
    overviewInfo,
    setOverviewInfo,
    isLoading,
    setIsLoading,
    overviewInfoCurrent,
  };
}