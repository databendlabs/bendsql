// Simplified CodeMirror theme using pre-built themes for stability
import { EditorView } from '@codemirror/view';
import { githubLight } from '@uiw/codemirror-theme-github';

// Use the stable GitHub Light theme
export const xcodeLight = githubLight;

// Simple custom styling without complex syntax highlighting
export const xcodeLightPatch = EditorView.theme({
  '&': {
    fontSize: '14px',
    fontFamily: 'SF Mono, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  },
  '&.cm-editor.cm-focused': {
    outline: 'none',
  },
  '.cm-content': {
    padding: '12px',
    lineHeight: '1.5',
  },
  '.cm-gutters': {
    backgroundColor: '#f8f9fa',
    border: 'none',
    borderRight: '1px solid #e1e4e8',
  },
  '.cm-lineNumbers .cm-gutterElement': {
    fontSize: '13px',
  },
  '.cm-activeLine': {
    backgroundColor: '#f1f8ff',
  },
  '.cm-selectionBackground': {
    backgroundColor: '#c8e1ff !important',
  },
  '.cm-searchMatch': {
    backgroundColor: '#ffdf5d',
    outline: '1px solid #c6b700',
  },
  '.cm-searchMatch.cm-searchMatch-selected': {
    backgroundColor: '#ffa500',
  },
});