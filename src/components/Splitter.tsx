import { useRef, type JSX, type MouseEvent } from "react";

interface Props {
  onResize: (deltaPx: number) => void;
}

/**
 * 6 px drag handle for vertically resizing two stacked panes. The
 * parent owns the size state (typically a ratio) and translates the
 * pixel delta into its own units. Listeners attach to the window so
 * the drag continues even if the cursor leaves the splitter.
 */
export function Splitter({ onResize }: Props): JSX.Element {
  const dragging = useRef(false);
  const lastY = useRef(0);

  const handleMouseDown = (event: MouseEvent<HTMLDivElement>): void => {
    event.preventDefault();
    dragging.current = true;
    lastY.current = event.clientY;
    const onMove = (ev: globalThis.MouseEvent): void => {
      if (!dragging.current) {
        return;
      }
      const dy = ev.clientY - lastY.current;
      lastY.current = ev.clientY;
      onResize(dy);
    };
    const onUp = (): void => {
      dragging.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      // Restore text-selection
      document.body.style.removeProperty("user-select");
      document.body.style.removeProperty("cursor");
    };
    document.body.style.userSelect = "none";
    document.body.style.cursor = "row-resize";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  return (
    <div
      className="splitter"
      role="separator"
      aria-orientation="horizontal"
      onMouseDown={handleMouseDown}
    />
  );
}
