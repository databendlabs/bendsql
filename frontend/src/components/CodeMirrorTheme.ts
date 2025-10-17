import { EditorView } from '@codemirror/view';
import { Extension } from '@codemirror/state';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags as t } from '@lezer/highlight';

// Light theme colors inspired by Xcode Light
const lightColors = {
  foreground: '#000000',
  background: '#ffffff',
  selectionBackground: '#b3d4fc',
  selectionForeground: '#000000',
  cursorColor: '#000000',
  lineNumberColor: '#999999',
  activeLine: '#f5f5f5',
  matchingBracket: '#e8f4fd',
  keyword: '#ad3da4', // Purple for keywords
  string: '#c41e3a', // Red for strings
  comment: '#65a30d', // Green for comments
  number: '#1c01ce', // Blue for numbers
  operator: '#000000',
  function: '#3f6ec7', // Blue for functions
  variable: '#000000',
  type: '#0f68a0', // Teal for types
};

// Xcode Light theme base
export const xcodeLight = EditorView.theme({
  '&': {
    color: lightColors.foreground,
    backgroundColor: lightColors.background,
    fontSize: '14px',
    fontFamily: 'SF Mono, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  },
  '.cm-content': {
    caretColor: lightColors.cursorColor,
    padding: '12px',
    lineHeight: '1.5',
  },
  '.cm-cursor, .cm-dropCursor': {
    borderLeftColor: lightColors.cursorColor,
  },
  '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground': {
    backgroundColor: lightColors.selectionBackground,
  },
  '.cm-selectionBackground': {
    backgroundColor: lightColors.selectionBackground,
  },
  '.cm-activeLine': {
    backgroundColor: lightColors.activeLine,
  },
  '.cm-gutters': {
    backgroundColor: lightColors.background,
    color: lightColors.lineNumberColor,
    border: 'none',
    borderRight: '1px solid #e0e0e0',
  },
  '.cm-activeLineGutter': {
    backgroundColor: lightColors.activeLine,
  },
  '.cm-lineNumbers .cm-gutterElement': {
    color: lightColors.lineNumberColor,
    fontSize: '13px',
  },
  '.cm-foldPlaceholder': {
    backgroundColor: 'transparent',
    border: 'none',
    color: lightColors.comment,
  },
  '.cm-searchMatch': {
    backgroundColor: '#ffdd44',
    outline: '1px solid #c4901e',
  },
  '.cm-searchMatch.cm-searchMatch-selected': {
    backgroundColor: '#ffa500',
  },
  '.cm-editor.cm-focused': {
    outline: 'none',
  },
  '.cm-scroller': {
    fontFamily: 'SF Mono, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  },
}, { dark: false });

// Syntax highlighting for Xcode Light theme
const xcodeLightHighlight = HighlightStyle.define([
  { tag: t.keyword, color: lightColors.keyword, fontWeight: 'bold' },
  { tag: [t.name, t.deleted, t.character, t.propertyName, t.macroName], color: lightColors.foreground },
  { tag: [t.function(t.variableName), t.labelName], color: lightColors.function },
  { tag: [t.color, t.constant(t.name), t.standard(t.name)], color: lightColors.function },
  { tag: [t.definition(t.name), t.separator], color: lightColors.foreground },
  { tag: [t.typeName, t.className, t.number, t.changed, t.annotation, t.modifier, t.self, t.namespace], 
    color: lightColors.type },
  { tag: [t.operator, t.operatorKeyword, t.url, t.escape, t.regexp, t.link, t.special(t.string)], 
    color: lightColors.operator },
  { tag: [t.meta, t.comment], color: lightColors.comment, fontStyle: 'italic' },
  { tag: t.strong, fontWeight: 'bold' },
  { tag: t.emphasis, fontStyle: 'italic' },
  { tag: t.strikethrough, textDecoration: 'line-through' },
  { tag: t.link, color: lightColors.function, textDecoration: 'underline' },
  { tag: t.heading, fontWeight: 'bold', color: lightColors.keyword },
  { tag: [t.atom, t.bool, t.special(t.variableName)], color: lightColors.number },
  { tag: [t.processingInstruction, t.string, t.inserted], color: lightColors.string },
  { tag: t.invalid, color: '#ff0000' },
  { tag: t.number, color: lightColors.number },
]);

export const xcodeLightPatch: Extension = syntaxHighlighting(xcodeLightHighlight);