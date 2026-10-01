import { useState } from "react";

import type { Draft } from "../lib/types";
import { Button } from "./ui/button";
import { Dialog, DialogClose, DialogContent } from "./ui/dialog";

export function RenameDraftDialog({
  draft,
  drafts,
  onClose,
  onRename,
}: {
  draft: Draft;
  drafts: Draft[];
  onClose: () => void;
  onRename: (name: string) => void;
}) {
  const [name, setName] = useState(draft.name);
  const trimmed = name.trim();
  const filename = /\.sql$/i.test(trimmed) ? trimmed : `${trimmed}.sql`;
  const error =
    !trimmed || /^\.sql$/i.test(trimmed)
      ? "Enter a file name."
      : /[/\\]/.test(trimmed)
        ? "Enter a file name without slashes."
        : drafts.some((item) => item.id !== draft.id && item.name === filename)
          ? "A query file with this name already exists."
          : null;

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        title="Rename query file"
        description="Choose a unique file name. The .sql extension is added automatically."
      >
        <form
          onSubmit={(event) => {
            event.preventDefault();
            if (!error) onRename(filename);
          }}
        >
          <header className="border-b border-hairline px-5 py-4">
            <h2 className="text-[16px] font-semibold">Rename query file</h2>
          </header>
          <div className="space-y-2 p-5">
            <label className="grid gap-1.5 text-[12px] text-body">
              File name
              <input
                autoFocus
                value={name}
                onFocus={(event) => {
                  const end = event.currentTarget.value.replace(
                    /\.sql$/i,
                    "",
                  ).length;
                  event.currentTarget.setSelectionRange(0, end);
                }}
                onChange={(event) => setName(event.target.value)}
                aria-invalid={!!error}
                aria-describedby="rename-draft-help"
                className="h-10 min-w-0 rounded-md border border-hairline bg-surface px-3 font-mono text-[12px] text-ink outline-none focus:border-accent"
              />
            </label>
            <p
              id="rename-draft-help"
              aria-live="polite"
              className={`text-[12px] ${error ? "text-danger" : "text-muted"}`}
            >
              {error ?? "The .sql extension is added automatically."}
            </p>
          </div>
          <footer className="flex justify-end gap-2 border-t border-hairline bg-canvas-soft px-5 py-3">
            <DialogClose asChild>
              <Button size="sm">Cancel</Button>
            </DialogClose>
            <Button
              type="submit"
              size="sm"
              variant="primary"
              disabled={!!error}
            >
              Rename
            </Button>
          </footer>
        </form>
      </DialogContent>
    </Dialog>
  );
}
