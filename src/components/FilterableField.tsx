import type { JSX, MouseEvent, ReactNode } from "react";

import type { FilterTarget } from "./FilterMenu";

type OpenFilterMenu = (
  target: FilterTarget,
  position: { x: number; y: number },
  anchorId?: string,
) => void;

interface Props {
  /** Inline content of the cell (a string, a number, JSX). */
  children: ReactNode;
  /** Predicate target the popover will resolve to. `null` disables
   *  the affordance entirely — render plain content. */
  target: FilterTarget | null;
  /** Stable identity for *this cell instance* — typically the
   *  surrounding row's id. Used to pin the icon visible on the exact
   *  cell whose popover is open, even when the cursor drifts. */
  anchorId: string;
  /** Stable key of the FilterTarget whose popover is currently open
   *  (parent state, computed via `menuAnchorKey` in App.tsx). The
   *  matching cell adds `is-menu-anchor`. `null` when no popover. */
  activeMenuKey: string | null;
  /** Parent callback — opens the global FilterMenu portal. */
  onOpenFilterMenu: OpenFilterMenu;
  /** Extra classes applied to the wrapper (e.g. grid-column class). */
  className?: string;
  /** Native `title` attribute on the wrapper (tooltip). */
  title?: string;
}

/**
 * Filterable cell with a hover-revealed funnel button. Click on the
 * funnel — or right-click anywhere on the cell — opens the global
 * `FilterMenu` popover at the cursor for the supplied
 * `FilterTarget`. Used everywhere the user can pivot from a value to
 * a DSL predicate (envelope fields, payload tree, schema layer, …).
 *
 * Style mirrors `.msglist__filter-icon` chrome: naked SVG funnel,
 * absolute right-edge, opacity 0 idle / 1 on hover (or while the
 * cell anchors the active popover). Matching against the parent's
 * `activeMenuKey` keeps the icon — and its accent colour — pinned
 * while the popover is up so the user always knows which cell the
 * popover targets.
 *
 * Pass `target = null` to render plain content with no affordance —
 * convenient for fields where the predicate is conditionally
 * available (e.g. a key may be `null`).
 */
export function FilterableField({
  children,
  target,
  anchorId,
  activeMenuKey,
  onOpenFilterMenu,
  className,
  title,
}: Props): JSX.Element {
  if (target === null) {
    return (
      <span className={`filterable-field${className ? ` ${className}` : ""}`} title={title}>
        <span className="filterable-field__content">{children}</span>
      </span>
    );
  }
  const cellKey = `${anchorId}|${target.path}|${target.literal.kind}|${target.literal.value}`;
  const isAnchor = activeMenuKey !== null && activeMenuKey === cellKey;

  const onContextMenu = (event: MouseEvent<HTMLSpanElement>): void => {
    event.preventDefault();
    event.stopPropagation();
    onOpenFilterMenu(target, { x: event.clientX, y: event.clientY }, anchorId);
  };
  const onIconClick = (event: MouseEvent<HTMLSpanElement>): void => {
    event.preventDefault();
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    onOpenFilterMenu(target, { x: rect.right, y: rect.bottom }, anchorId);
  };

  return (
    <span
      className={`filterable-field filterable-field--filterable${
        isAnchor ? " is-menu-anchor" : ""
      }${className ? ` ${className}` : ""}`}
      title={title}
      onContextMenu={onContextMenu}
    >
      <span className="filterable-field__content">{children}</span>
      <span
        className="filterable-field__icon"
        role="button"
        aria-label="Filter actions"
        title="Filter actions"
        tabIndex={-1}
        onClick={onIconClick}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            event.stopPropagation();
            const rect = event.currentTarget.getBoundingClientRect();
            onOpenFilterMenu(target, { x: rect.right, y: rect.bottom }, anchorId);
          }
        }}
      >
        <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" focusable="false">
          <path
            d="M2.5 3h11l-4 5v4l-3 1.2V8l-4-5z"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    </span>
  );
}
