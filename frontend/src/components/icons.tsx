import React from 'react';

export const PlayIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <path d="M7 5v10l7-5-7-5z" fill="currentColor" stroke="none" />
  </svg>
);

export const ChevronDownIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <path d="M6 8l4 4 4-4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

export const ChevronUpIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <path d="M6 12l4-4 4 4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

export const ColumnsIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <rect x="4" y="4" width="5" height="12" rx="1.5" />
    <rect x="11" y="4" width="5" height="12" rx="1.5" />
  </svg>
);

export const CodeIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <path d="M7 6 4 10l3 4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    <path d="m13 6 3 4-3 4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

export const GripIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <circle cx="7" cy="6" r="1" />
    <circle cx="13" cy="6" r="1" />
    <circle cx="7" cy="10" r="1" />
    <circle cx="13" cy="10" r="1" />
    <circle cx="7" cy="14" r="1" />
    <circle cx="13" cy="14" r="1" />
  </svg>
);

export const ExpandIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" {...props}>
    <path d="M8 4H4v4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M12 4h4v4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M8 16H4v-4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M12 16h4v-4" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

export const EllipsisIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg viewBox="0 0 20 20" fill="currentColor" stroke="none" {...props}>
    <circle cx="5" cy="10" r="1.5" />
    <circle cx="10" cy="10" r="1.5" />
    <circle cx="15" cy="10" r="1.5" />
  </svg>
);
