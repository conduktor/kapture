import { useEffect, useRef, type JSX } from "react";
import { createPortal } from "react-dom";
import { composeAnd, equalityExpr, inequalityExpr, type PrimitiveLiteral } from "../lib/filterExpr";

export interface FilterTarget {
  path: string;
  literal: PrimitiveLiteral;
  /**
   * When true the menu adds "Has value" / "No value" entries that filter
   * by presence/absence of the path (truthy check). Useful for fields
   * that may be missing entirely — typically `headers.<name>` and
   * `envelope.key`.
   */
  supportsPresence?: boolean;
}

interface Props {
  target: FilterTarget;
  /** Anchor position in client coordinates (event.clientX/Y). */
  position: { x: number; y: number };
  /** Current filter; used to compose AND clauses on top. */
  currentFilter: string;
  onApply: (expression: string) => void;
  onClose: () => void;
}

// Wireshark-style 4-option apply menu. Rendered through a portal to escape
// the parent's overflow:hidden / z-index stacking and positioned absolutely
// at the cursor. Closes on outside-click, Escape, scroll, or window resize.
export function FilterMenu({
  target,
  position,
  currentFilter,
  onApply,
  onClose,
}: Props): JSX.Element | null {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onDocClick = (event: MouseEvent): void => {
      if (ref.current && !ref.current.contains(event.target as Node)) {
        onClose();
      }
    };
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    // Capture phase so the click that triggered this menu doesn't itself
    // bubble back up and immediately close it. Listener attaches AFTER
    // the current event finishes via the next tick.
    const id = window.setTimeout(() => {
      document.addEventListener("mousedown", onDocClick);
      document.addEventListener("keydown", onKey);
      window.addEventListener("scroll", onClose, true);
      window.addEventListener("resize", onClose);
    }, 0);
    return () => {
      window.clearTimeout(id);
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("scroll", onClose, true);
      window.removeEventListener("resize", onClose);
    };
  }, [onClose]);

  const eq = equalityExpr(target.path, target.literal);
  const ne = inequalityExpr(target.path, target.literal);
  if (!eq || !ne) {
    return null;
  }

  // Clamp to viewport so menus near the right / bottom edges don't get
  // cut off. 200×140 is a safe upper bound for the menu size.
  const MAX_W = 220;
  const MAX_H = 160;
  const left = Math.min(position.x, window.innerWidth - MAX_W - 8);
  const top = Math.min(position.y, window.innerHeight - MAX_H - 8);

  const applySet = (expression: string): void => {
    onApply(expression);
    onClose();
  };
  const applyAnd = (expression: string): void => {
    onApply(composeAnd(currentFilter, expression));
    onClose();
  };

  return createPortal(
    <div
      ref={ref}
      className="filter-menu"
      style={{ left, top }}
      role="menu"
      aria-label="Filter actions"
    >
      {/* Each item renders the actual DSL predicate that will land in
       *  the top filter bar — no abstract "Filter ==" labels — so the
       *  user reads exactly what they're applying. The first group
       *  REPLACES the current filter; the AND group composes on top
       *  of it. */}
      <button
        type="button"
        className="filter-menu__item"
        onClick={() => {
          applySet(eq);
        }}
        title="Replace the current filter with this predicate"
      >
        <code>{eq}</code>
      </button>
      <button
        type="button"
        className="filter-menu__item"
        onClick={() => {
          applySet(ne);
        }}
        title="Replace the current filter with this predicate"
      >
        <code>{ne}</code>
      </button>
      {target.supportsPresence ? (
        <>
          <button
            type="button"
            className="filter-menu__item"
            onClick={() => {
              applySet(target.path);
            }}
            title="Match messages where this field is present (any value)"
          >
            <code>{target.path}</code>
          </button>
          <button
            type="button"
            className="filter-menu__item"
            onClick={() => {
              applySet(`!${target.path}`);
            }}
            title="Match messages where this field is absent"
          >
            <code>!{target.path}</code>
          </button>
        </>
      ) : null}
      <div className="filter-menu__sep" />
      <button
        type="button"
        className="filter-menu__item"
        onClick={() => {
          applyAnd(eq);
        }}
        title="Compose: existing filter && this predicate"
      >
        <span className="filter-menu__item-prefix">AND</span> <code>{eq}</code>
      </button>
      <button
        type="button"
        className="filter-menu__item"
        onClick={() => {
          applyAnd(ne);
        }}
        title="Compose: existing filter && this predicate"
      >
        <span className="filter-menu__item-prefix">AND</span> <code>{ne}</code>
      </button>
      {target.supportsPresence ? (
        <>
          <button
            type="button"
            className="filter-menu__item"
            onClick={() => {
              applyAnd(target.path);
            }}
          >
            <span className="filter-menu__item-prefix">AND</span> <code>{target.path}</code>
          </button>
          <button
            type="button"
            className="filter-menu__item"
            onClick={() => {
              applyAnd(`!${target.path}`);
            }}
          >
            <span className="filter-menu__item-prefix">AND</span> <code>!{target.path}</code>
          </button>
        </>
      ) : null}
    </div>,
    document.body,
  );
}
