import { useEffect, useRef, useState } from "react";

import { asProblem } from "./api";
import {
  emptyCatalog,
  listCatalog,
  listCatalogVersions,
  type CatalogExecutor,
} from "./catalog";
import type { WorkbenchProblem } from "./types";

export function useCatalogNavigation({
  connected,
  sessionVersion,
  busy,
  execute,
  onBusyChange,
  canExecute,
}: {
  connected: boolean;
  sessionVersion: number;
  busy: boolean;
  execute: CatalogExecutor;
  onBusyChange: (busy: boolean) => void;
  canExecute: () => boolean;
}) {
  const [objects, setObjects] = useState(emptyCatalog);
  const [loading, setLoading] = useState(false);
  const [problem, setProblem] = useState<WorkbenchProblem | null>(null);
  const [revision, setRevision] = useState(0);
  const [loadedRevision, setLoadedRevision] = useState(-1);
  const controller = useRef<AbortController | null>(null);
  const generation = useRef(0);
  const props = useRef({ execute, onBusyChange, canExecute });
  props.current = { execute, onBusyChange, canExecute };

  useEffect(() => {
    setObjects(emptyCatalog());
    setProblem(null);
    setLoadedRevision(-1);
    return () => {
      generation.current += 1;
      controller.current?.abort();
    };
  }, [connected, sessionVersion]);

  useEffect(() => {
    if (
      !connected ||
      busy ||
      controller.current ||
      loadedRevision === revision ||
      !props.current.canExecute()
    )
      return;
    const operation = new AbortController();
    const currentGeneration = generation.current;
    controller.current = operation;
    setLoading(true);
    setProblem(null);
    props.current.onBusyChange(true);
    void (async () => {
      try {
        await Promise.resolve();
        operation.signal.throwIfAborted();
        const next = emptyCatalog();
        // Definitions are loaded only on selection. Version metadata is shared
        // by a Model's entries in the Models and Functions branches.
        for (const section of ["tables", "models", "functions"] as const) {
          next[section] = await listCatalog(
            section,
            props.current.execute,
            operation.signal,
          );
        }
        for (const object of next.models) {
          if ((object.versionCount ?? 0) < 2 || object.problem) continue;
          let metadata;
          try {
            metadata = {
              versions: await listCatalogVersions(
                object,
                props.current.execute,
                operation.signal,
              ),
            };
          } catch (error) {
            operation.signal.throwIfAborted();
            metadata = { versionsProblem: asProblem(error) };
          }
          for (const section of ["models", "functions"] as const) {
            next[section] = next[section].map((item) =>
              item.id === object.id ? { ...item, ...metadata } : item,
            );
          }
        }
        if (currentGeneration === generation.current) setObjects(next);
      } catch (error) {
        if (
          !operation.signal.aborted &&
          currentGeneration === generation.current
        )
          setProblem(asProblem(error));
      } finally {
        controller.current = null;
        if (currentGeneration === generation.current)
          setLoadedRevision(revision);
        setLoading(false);
        props.current.onBusyChange(false);
      }
    })();
  }, [connected, sessionVersion, busy, revision, loadedRevision, loading]);

  return {
    objects,
    loading,
    problem,
    refresh: () => setRevision((current) => current + 1),
  };
}
