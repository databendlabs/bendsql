export function formatRelativeTime(input?: Date | string | number): string {
  if (!input) {
    return '';
  }

  const date = input instanceof Date ? input : new Date(input);
  if (isNaN(date.getTime())) {
    return '';
  }

  const diff = Date.now() - date.getTime();
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;

  if (diff < minute) {
    return 'just now';
  }
  if (diff < hour) {
    const minutes = Math.floor(diff / minute);
    return `${minutes}m ago`;
  }
  if (diff < day) {
    const hours = Math.floor(diff / hour);
    return `${hours}h ago`;
  }
  if (diff < week) {
    const days = Math.floor(diff / day);
    return `${days}d ago`;
  }

  return date.toLocaleDateString();
}
