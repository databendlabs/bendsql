import { python } from '@codemirror/lang-python';
import { indentUnit, syntaxHighlighting } from '@codemirror/language';
import { HighlightStyle } from '@codemirror/language';
import { tags } from '@lezer/highlight';
import type { Extension } from '@codemirror/state';

const pythonStyle = HighlightStyle.define([
  { tag: [tags.comment, tags.quote], color: '#707F8D', fontStyle: 'italic' },
  { tag: [tags.keyword], color: '#aa0d91', fontWeight: 'bold' },
  { tag: [tags.string, tags.special(tags.string)], color: '#D23423' },
  { tag: [tags.number, tags.bool], color: '#c026d3' },
  { tag: [tags.name], color: '#032f62' },
]);

export function python_syntax(): Extension {
  return [
    python(),
    indentUnit.of('    '),
    syntaxHighlighting(pythonStyle),
  ];
}
