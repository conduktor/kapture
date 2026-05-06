/**
 * Scroll a virtualised row into view by adjusting the parent container's
 * scrollTop. Used for arrow-key navigation in both MessageList and
 * ProtoList — kept here so both lists can import without producing a
 * react-refresh "non-component export" warning.
 */
export function ensureRowVisible(body: HTMLElement | null, index: number, rowHeight: number): void {
  if (!body) {
    return;
  }
  const top = index * rowHeight;
  const bottom = top + rowHeight;
  const viewTop = body.scrollTop;
  const viewBottom = viewTop + body.clientHeight;
  if (top < viewTop) {
    body.scrollTop = top;
  } else if (bottom > viewBottom) {
    body.scrollTop = bottom - body.clientHeight;
  }
}
