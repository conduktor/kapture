import { useRef, type JSX, type MouseEvent } from "react";

type Orientation = "vertical" | "horizontal";

interface Props {
  /**
   * Splitter orientation. `vertical` (default) is a horizontal bar that
   * resizes two stacked panes (drag along Y). `horizontal` is a vertical
   * bar that resizes two side-by-side panes (drag along X).
   */
  orientation?: Orientation;
  onResize: (deltaPx: number) => void;
}

/**
 * 6 px drag handle for resizing two adjacent panes. The parent owns the
 * size state (typically a ratio) and translates the pixel delta into its
 * own units. Listeners attach to the window so the drag continues even if
 * the cursor leaves the splitter.
 */
export function Splitter({ orientation = "vertical", onResize }: Props): JSX.Element {
  const dragging = useRef(false);
  const last = useRef(0);
  const isHorizontal = orientation === "horizontal";

  const handleMouseDown = (event: MouseEvent<HTMLDivElement>): void => {
    event.preventDefault();
    dragging.current = true;
    last.current = isHorizontal ? event.clientX : event.clientY;
    const onMove = (ev: globalThis.MouseEvent): void => {
      if (!dragging.current) {
        return;
      }
      const cur = isHorizontal ? ev.clientX : ev.clientY;
      const delta = cur - last.current;
      last.current = cur;
      onResize(delta);
    };
    const cursor = isHorizontal ? "col-resize" : "row-resize";
    const onUp = (): void => {
      dragging.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      // Restore text-selection
      document.body.style.removeProperty("user-select");
      document.body.style.removeProperty("cursor");
    };
    document.body.style.userSelect = "none";
    document.body.style.cursor = cursor;
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  return (
    <div
      className={`splitter splitter--${orientation}`}
      role="separator"
      aria-orientation={isHorizontal ? "vertical" : "horizontal"}
      onMouseDown={handleMouseDown}
    />
  );
}
