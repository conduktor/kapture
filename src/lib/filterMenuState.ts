import type { FilterTarget } from "../components/FilterMenu";

/** State of the global FilterMenu portal. The `scope` discriminator
 *  routes `apply` / `currentFilter` to the right top-bar (messages
 *  vs protocol — same `==` / `!=` / `&&` grammar, separate parsers
 *  and state slots). */
export interface MenuState {
  target: FilterTarget;
  position: { x: number; y: number };
  /** Optional row id of the cell whose icon was clicked. Used to
   *  pin the icon visible on the *exact* row even when the cursor
   *  drifts away. `null` when the menu was opened from a non-row
   *  context (e.g. the LayerTree detail). */
  anchorId: string | null;
  scope: "messages" | "protocol";
}

/** Stable identity for the cell that anchored the active menu —
 *  `anchorId | path | kind | value`. The anchor id keeps the
 *  highlight scoped to a single row when many rows share the same
 *  predicate target (e.g. all rows on `topic == "streams-input"`)
 *  — without it every matching cell would light up. */
export function menuAnchorKey(anchorId: string, t: FilterTarget): string {
  return `${anchorId}|${t.path}|${t.literal.kind}|${t.literal.value}`;
}

/** Derive the `activeMenuKey` value passed down to a list/cell. Returns
 *  `null` unless the open menu's scope matches AND its anchor id is
 *  set, so cells in the wrong tab don't get pinned. */
export function activeMenuKeyFor(menu: MenuState | null, scope: MenuState["scope"]): string | null {
  if (menu?.scope !== scope || menu.anchorId === null) {
    return null;
  }
  return menuAnchorKey(menu.anchorId, menu.target);
}
