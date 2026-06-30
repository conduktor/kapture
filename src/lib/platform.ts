/**
 * Host-OS detection for the frontend. Backed by the `app_info` Tauri
 * command (`os` = `std::env::consts::OS`). Used to hide Unix-only
 * features — JVM tap mode transports over a Unix domain socket and is
 * compiled out on Windows (the tap commands return an error there).
 */

import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";

let cachedOs: string | null = null;

/** Resolve the host OS once and cache it for the session. */
export async function getOs(): Promise<string> {
  if (cachedOs !== null) {
    return cachedOs;
  }
  try {
    const info = await invoke<{ os: string }>("app_info");
    cachedOs = info.os;
  } catch {
    cachedOs = "unknown";
  }
  return cachedOs;
}

/** `true` when running on Windows (where JVM tap mode is unavailable). */
export function useIsWindows(): boolean {
  const [isWindows, setIsWindows] = useState(false);
  useEffect(() => {
    void getOs().then((os) => {
      setIsWindows(os === "windows");
    });
  }, []);
  return isWindows;
}
