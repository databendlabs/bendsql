import { useMemo, useCallback } from "react";
import { FlowAnalysisGraphConfig } from "@ant-design/charts";

import { pathArrow, pathMoon } from "../constants";
import { createRoundedRectPath, formatRows, getTextWidth } from "../utills";
import type { StaticImageData } from "next/image";
import fullScreenUrl from '../images/icons/full-screen.svg';
import zoomInUrl from '../images/icons/zoom-in.svg';
import zoomOutUrl from '../images/icons/zoom-out.svg';
import downloadUrl from '../images/icons/download.svg';
import { EdgeWithLineWidth } from "../components/FlowAnalysisGraph";

const resolveImageSrc = (asset: string | StaticImageData): string =>
  typeof asset === "string" ? asset : asset.src;

const fullScreenSrc = resolveImageSrc(fullScreenUrl);
const zoomInSrc = resolveImageSrc(zoomInUrl);
const zoomOutSrc = resolveImageSrc(zoomOutUrl);
const downloadSrc = resolveImageSrc(downloadUrl);

export const useFlowAnalysisGraphConfig = ({
  graphSize,
  onReady,
  data,
  graphRef,
  overviewInfoCurrent,
  handleResetView,
}): FlowAnalysisGraphConfig => {
  const getToolbarContent = useCallback(({ zoomIn, zoomOut }) => (
    <div className="flex justify-around items-center">
      <span
        className="cursor-pointer flex justify-center items-center"
        onClick={() => handleResetView()}
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={fullScreenSrc} alt="full screen" />
      </span>
      <span className="cursor-pointer flex justify-center items-center" onClick={zoomOut}>
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={zoomOutSrc} alt="zoom out" />
      </span>
      <span className="cursor-pointer flex justify-center items-center" onClick={zoomIn}>
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={zoomInSrc} alt="zoom in" />
      </span>
      <span
        className="g-cursor g-box-c"
        onClick={() =>
          graphRef?.current?.downloadFullImage(
            `databend-profile`,
            "image/png"
          )
        }
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={downloadSrc} alt="download" />
      </span>
    </div>
  ), [handleResetView, graphRef]);

  const getNodeStyle = edge => ({
    radius: 5,
    fill: "#fff",
    stroke: "#ccc",
    filter: "drop-shadow(2px 3px 2px rgba(255, 255, 255, .2))",
  });

  const getNodeTitleStyle = edge => ({
    fontWeight: 600,
    fill: edge?.errors?.length ? "#fff" : "#000",
  });

  const getEdgeStyle = (edge: EdgeWithLineWidth) => ({
    lineWidth: (edge?.lineWidth as number) || 1,
  });

  const getCustomNodeContent = useCallback((item, group, cfg) => {
    const { startX, startY, width } = cfg;
    const { text } = item;
    const totalWidth = 230;
    const textLength = text?.length;
    const model = group?.cfg?.item?._cfg?.model;
    const longRate = model?.totalTime / (overviewInfoCurrent.current?.totalTime || 1);
    const isExistedError = model?.errors?.length;
    const parentId = model?.parent_id;

    const textShape = group.addShape("text", {
      attrs: {
        textBaseline: "top",
        x: startX,
        y: startY,
        text,
        fill: isExistedError ? "rgba(255,255,255,0.8)" : "#75767a",
        textAlign: "left",
      },
      name: `text-${Math.random()}`,
    });

    const textWidth = textShape.getBBox().width;
    if (textLength > 26 && textWidth > width) {
      const ellipsisText = text.slice(0, Math.floor((width / textWidth) * textLength - 3)) + "...";
      textShape.attr("text", ellipsisText);
    }
    const textHeight = textShape?.getBBox().height ?? 0;

    const height = 8;
    const borderRadius = 4;
    const progressWidth = longRate * totalWidth;

    const backgroundPath = createRoundedRectPath(
      startX,
      startY + textHeight + 10,
      totalWidth,
      height,
      borderRadius
    );
    group.addShape("path", {
      attrs: {
        path: backgroundPath,
        fill: "#f2f2f2",
      },
      name: `progress-bg-${Math.random()}`,
    });

    const foregroundPath = createRoundedRectPath(
      startX,
      startY + textHeight + 10,
      progressWidth,
      height,
      borderRadius
    );
    group.addShape("path", {
      attrs: {
        path: foregroundPath,
        fill: progressWidth <= 0 ? "rgba(0,0,0,0)" : "rgb(1, 117, 246)",
      },
      name: `progress-fg-${Math.random()}`,
    });

    if (progressWidth > 0 && progressWidth < 9) {
      group.addShape("path", {
        attrs: {
          path: pathMoon,
          fill: isExistedError ? "#f73920" : "#fff",
        },
        name: `circle-path-bg-${Math.random()}`,
      });
    }

    if (parentId === "null") {
      const edgeObj = data.edges?.find(edge => edge?.source === "null");
      group.addShape("path", {
        attrs: {
          path: pathArrow,
          fill: "#ccc",
          stroke: "#ccc",
          lineWidth: (edgeObj as EdgeWithLineWidth)?.lineWidth || 1,
        },
        name: `percentage-output-text-${Math.random()}`,
      });
      const outputRowsFormat = formatRows(model?.outputRows);
      group.addShape("text", {
        attrs: {
          textBaseline: "top",
          x: 125 + getTextWidth(outputRowsFormat) / 2,
          y: -30,
          text: outputRowsFormat,
          fill: "rgba(12, 22, 43, 0.6)",
          fontWeight: "bold",
          fontSize: 12,
          textAlign: "right",
        },
        name: "percentage-output-text",
      });
    }

    const percentageText = longRate > 0 ? `${(longRate * 100).toFixed(1)}%` : "0%";
    group.addShape("text", {
      attrs: {
        textBaseline: "top",
        x: startX + width,
        y: startY - 27,
        text: percentageText,
        fill: isExistedError ? "#fff" : "#000",
        fontSize: 11,
        textAlign: "right",
      },
      name: `percentage-text-${Math.random()}`,
    });

    return Math.max(textHeight, height);
  }, [overviewInfoCurrent, data]);

  return useMemo(() => ({
    ...graphSize,
    onReady,
    data,
    layout: {
      rankdir: "TB",
      ranksepFunc: () => 20,
    },
    toolbarCfg: {
      className: 'absolute top-0 left-0 w-[100px]',
      show: true,
      customContent: getToolbarContent,
    },
    nodeCfg: {
      padding: 10,
      size: [250, 40],
      title: {
        autoEllipsis: true,
        containerStyle: {
          fill: "transparent",
        },
        style: getNodeTitleStyle,
      },
      anchorPoints: [
        [0.5, 0],
        [0.5, 1],
      ],
      style: getNodeStyle,
      nodeStateStyles: {
        highlight: {
          stroke: "#2c91ff",
          lineWidth: 2,
        },
        hover: {
          stroke: "#2c91ff",
          lineWidth: 2,
        },
      },
      customContent: getCustomNodeContent,
    },
    edgeCfg: {
      type: "cubic-vertical",
      endArrow: false,
      style: getEdgeStyle,
      label: {
        style: {
          fontWeight: 600,
          fill: "rgba(12, 22, 43, 0.6)",
        },
      },
      startArrow: {
        type: "triangle",
      },
    },
    markerCfg: cfg => ({
      animate: true,
      position: "bottom",
      show: data.edges.filter(item => item.source === cfg.id)?.length,
    }),
    behaviors: ["drag-canvas", "zoom-canvas"],
  }), [graphSize, onReady, data, getCustomNodeContent, getToolbarContent]);
};
