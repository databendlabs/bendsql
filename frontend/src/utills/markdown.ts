const escapeHtml = (input: string): string =>
  input
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const renderInline = (input: string): string => {
  let html = escapeHtml(input);
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/__([^_]+)__/g, '<strong>$1</strong>');
  html = html.replace(/(?<!\*)\*([^*]+)\*/g, '<em>$1</em>');
  html = html.replace(/(?<!_)_([^_]+)_/g, '<em>$1</em>');
  html = html.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer noopener">$1</a>');
  return html;
};

export const renderBasicMarkdown = (content: string): string => {
  const lines = content.replace(/\r\n?/g, '\n').split('\n');
  const htmlParts: string[] = [];
  let listBuffer: string[] = [];
  let codeBuffer: string[] = [];
  let inCodeBlock = false;

  const flushList = () => {
    if (listBuffer.length === 0) {
      return;
    }
    const items = listBuffer
      .map(item => {
        const text = item.replace(/^\s*[-*+]\s+/, '');
        return `<li>${renderInline(text)}</li>`;
      })
      .join('');
    htmlParts.push(`<ul>${items}</ul>`);
    listBuffer = [];
  };

  const flushCode = () => {
    if (codeBuffer.length === 0) {
      return;
    }
    htmlParts.push(`<pre><code>${escapeHtml(codeBuffer.join('\n'))}</code></pre>`);
    codeBuffer = [];
  };

  for (const rawLine of lines) {
    const line = rawLine;
    const trimmed = line.trimEnd();

    if (/^```/.test(trimmed)) {
      if (inCodeBlock) {
        flushCode();
        inCodeBlock = false;
      } else {
        flushList();
        inCodeBlock = true;
      }
      continue;
    }

    if (inCodeBlock) {
      codeBuffer.push(line);
      continue;
    }

    if (/^\s*[-*+]\s+/.test(line)) {
      listBuffer.push(line);
      continue;
    }

    flushList();

    if (!trimmed.trim()) {
      htmlParts.push('');
      continue;
    }

    if (/^>/.test(trimmed)) {
      const quote = trimmed.replace(/^>\s?/, '');
      htmlParts.push(`<blockquote>${renderInline(quote)}</blockquote>`);
      continue;
    }

    const headingMatch = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      htmlParts.push(`<h${level}>${renderInline(headingMatch[2])}</h${level}>`);
      continue;
    }

    htmlParts.push(`<p>${renderInline(trimmed)}</p>`);
  }

  flushList();
  if (inCodeBlock) {
    flushCode();
  }

  return htmlParts.filter(Boolean).join('');
};
