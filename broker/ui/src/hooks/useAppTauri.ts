// useAppTauri.ts — bootstraps and supervises the desktop app's control plane.
//
// When running inside the Tauri shell this hook:
//   - loads the app store (connections, first-run flag, mcp),
//   - starts the local broker when it is (or becomes) the active connection,
//   - points the dashboard's reactive apiBase at the active broker and
//     invalidates queries so stats/tasks/workers re-fetch,
//   - exposes first-run completion + connection switching.
// In the browser it is a no-op strip.

import { useCallback, useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  appAddRemote,
  appCompleteFirstRun,
  appGetState,
  appSetLastUsed,
  appStartLocal,
  appStopLocal,
  isTauri,
  localHttpBase,
  type AppState,
  type LocalStatus,
} from "../lib/appBridge";
import { setApiBase } from "../lib/api";

export type BootStep = "starting-broker" | "checking-connections" | "loading-dashboard";

export function useAppTauri() {
  const tauri = isTauri();
  const qc = useQueryClient();
  const [state, setState] = useState<AppState | null>(null);
  const [local, setLocal] = useState<LocalStatus | null>(null);
  const [step, setStep] = useState<BootStep | null>(null);
  const [bootError, setBootError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const [active, setActive] = useState<string>("local");

  // Point the query layer at a connection's api base and refetch everything.
  const pointAt = useCallback(
    (base: string | null) => {
      setApiBase(base);
      qc.invalidateQueries();
      qc.removeQueries();
    },
    [qc],
  );

  useEffect(() => {
    if (!tauri) return;
    let cancelled = false;
    void (async () => {
      try {
        const st = await appGetState();
        if (cancelled) return;
        setState(st);
        setActive(st.last_used || "local");

        if (st.last_used === "local" || !st.first_run_done) {
          setStep("starting-broker");
          const loc = await appStartLocal();
          if (cancelled) return;
          setLocal(loc);
          const base = localHttpBase(loc);
          if (base) pointAt(base);
        } else {
          const remote = st.remotes.find((r) => r.name === st.last_used);
          if (remote) pointAt(remote.http_url);
        }

        setStep("checking-connections");
        await new Promise((r) => setTimeout(r, 250));
        if (cancelled) return;
        setStep("loading-dashboard");
        await new Promise((r) => setTimeout(r, 350));
        if (cancelled) return;
        setStep(null);
        setReady(true);
      } catch (e) {
        if (cancelled) return;
        setBootError(String(e));
        setStep(null);
        setReady(true);
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tauri]);

  /// Start the local broker (welcome / tray-initiated).
  const startLocal = useCallback(async () => {
    const loc = await appStartLocal();
    setLocal(loc);
    setState((prev) => (prev ? { ...prev, local: loc } : prev));
    const base = localHttpBase(loc);
    if (base) {
      setActive("local");
      pointAt(base);
    }
    await appSetLastUsed("local");
    return loc;
  }, [pointAt]);

  /// Stop the local broker (adopted brokers are untouched by the Rust side).
  const stopLocal = useCallback(async () => {
    const loc = await appStopLocal();
    setLocal(loc);
    return loc;
  }, []);

  /// Add a remote from the welcome/settings UI.
  const addRemote = useCallback(async (name: string, httpUrl: string) => {
    await appAddRemote(name, httpUrl);
    const st = await appGetState();
    setState(st);
  }, []);

  /// Switch the active connection. "local" boots the local broker if needed.
  const switchTo = useCallback(
    async (name: string) => {
      if (name !== "local" && !state?.remotes.some((r) => r.name === name)) {
        return;
      }
      const base =
        name === "local"
          ? localHttpBase(local ?? (await appStartLocal()))
          : state!.remotes.find((r) => r.name === name)!.http_url;
      await appSetLastUsed(name);
      setActive(name);
      pointAt(base);
    },
    [state, local, pointAt],
  );

  /// Finish the welcome gate (first run).
  const completeFirstRun = useCallback(async () => {
    await appCompleteFirstRun();
    setState((prev) => (prev ? { ...prev, first_run_done: true } : prev));
  }, []);

  /// Nothing to do in the browser: expose a no-op surface.
  const noop = useCallback(async () => {}, []);

  if (!tauri) {
    return {
      tauri: false,
      ready: true,
      step: null,
      firstRun: false,
      state: null,
      local: null,
      active: "local",
      bootError: null,
      startLocal: noop,
      stopLocal: noop,
      addRemote: noop,
      switchTo: noop,
      completeFirstRun: noop,
    };
  }

  return {
    tauri: true,
    ready,
    step,
    firstRun: state ? !state.first_run_done : true,
    state,
    local,
    active,
    bootError,
    startLocal,
    stopLocal,
    addRemote,
    switchTo,
    completeFirstRun,
  };
}
