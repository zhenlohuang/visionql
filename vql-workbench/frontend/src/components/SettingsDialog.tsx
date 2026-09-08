import * as DialogPrimitive from "@radix-ui/react-dialog";
import { Cable, FileKey2, LockKeyhole, Unplug } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import type { SessionInput } from "../lib/api";
import { Button } from "./ui/button";
import { Dialog, DialogContent } from "./ui/dialog";

export function SettingsDialog({
  open,
  onOpenChange,
  endpoint,
  connected,
  busy,
  onConnect,
  onDisconnect,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  endpoint: string;
  connected: boolean;
  busy: boolean;
  onConnect: (input: SessionInput) => Promise<void>;
  onDisconnect: () => Promise<void>;
}) {
  const [nextEndpoint, setNextEndpoint] = useState(endpoint);
  const [credential, setCredential] = useState("");
  const [tlsCaPem, setTlsCaPem] = useState<string | undefined>();
  const [submitting, setSubmitting] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);
  useEffect(() => setNextEndpoint(endpoint), [endpoint, open]);
  const connect = async () => {
    setSubmitting(true);
    try {
      await onConnect({
        endpoint: nextEndpoint.trim(),
        credential: credential || undefined,
        tlsCaPem,
      });
      setCredential("");
      setTlsCaPem(undefined);
      onOpenChange(false);
    } finally {
      setSubmitting(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        title="Workbench connection settings"
        description="Connect the loopback Workbench bridge to one vqld endpoint."
      >
        <header className="border-b border-hairline px-5 py-4 pr-14">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-accent">
            Browser Session
          </p>
          <h2 className="mt-1 text-lg font-semibold tracking-[-0.02em] text-ink">
            Connection settings
          </h2>
          <p className="mt-1 max-w-md text-[12px] leading-5 text-body">
            The loopback backend holds credentials and TLS material in memory.
            They are never saved to browser storage.
          </p>
        </header>
        <div className="space-y-4 p-5">
          <label className="grid gap-1.5 text-[10px] font-semibold uppercase tracking-[0.1em] text-muted">
            vqld endpoint
            <div className="flex h-10 items-center gap-2 rounded-md border border-hairline bg-surface px-3 focus-within:border-accent">
              <Cable size={14} className="text-muted" />
              <input
                type="url"
                required
                value={nextEndpoint}
                onChange={(event) => setNextEndpoint(event.target.value)}
                placeholder="http://127.0.0.1:6031"
                className="min-w-0 flex-1 bg-transparent font-mono text-[12px] font-normal text-ink outline-none placeholder:text-muted"
              />
            </div>
          </label>
          <label className="grid gap-1.5 text-[10px] font-semibold uppercase tracking-[0.1em] text-muted">
            Service credential{" "}
            <span className="normal-case tracking-normal">(optional)</span>
            <div className="flex h-10 items-center gap-2 rounded-md border border-hairline bg-surface px-3 focus-within:border-accent">
              <LockKeyhole size={14} className="text-muted" />
              <input
                type="password"
                autoComplete="off"
                value={credential}
                onChange={(event) => setCredential(event.target.value)}
                placeholder="Uses backend default when empty"
                className="min-w-0 flex-1 bg-transparent font-mono text-[12px] font-normal text-ink outline-none placeholder:text-muted"
              />
            </div>
          </label>
          <div className="grid gap-1.5 text-[10px] font-semibold uppercase tracking-[0.1em] text-muted">
            TLS CA certificate{" "}
            <span className="normal-case tracking-normal">(optional PEM)</span>
            <input
              ref={fileRef}
              type="file"
              accept=".pem,.crt,application/x-pem-file"
              className="sr-only"
              onChange={async (event) => {
                const file = event.target.files?.[0];
                setTlsCaPem(file ? await file.text() : undefined);
              }}
            />
            <Button
              className="h-10 justify-start"
              onClick={() => fileRef.current?.click()}
            >
              <FileKey2 size={14} />
              <span className="normal-case tracking-normal">
                {tlsCaPem ? "CA certificate loaded" : "Choose CA certificate"}
              </span>
            </Button>
          </div>
        </div>
        <footer className="flex items-center justify-between border-t border-hairline bg-canvas-soft px-5 py-3">
          {connected ? (
            <Button
              variant="ghost"
              size="sm"
              className="text-danger hover:text-danger"
              disabled={busy || submitting}
              onClick={() => void onDisconnect()}
            >
              <Unplug size={13} /> Disconnect
            </Button>
          ) : (
            <span className="font-mono text-[10px] text-muted">
              Not connected
            </span>
          )}
          <div className="flex gap-2">
            <DialogPrimitive.Close asChild>
              <Button size="sm">Cancel</Button>
            </DialogPrimitive.Close>
            <Button
              size="sm"
              variant="primary"
              disabled={!nextEndpoint.trim() || busy || submitting}
              onClick={() => void connect()}
            >
              <Cable size={13} />{" "}
              {submitting ? "Connecting…" : connected ? "Reconnect" : "Connect"}
            </Button>
          </div>
        </footer>
      </DialogContent>
    </Dialog>
  );
}
