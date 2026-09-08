import * as DialogPrimitive from "@radix-ui/react-dialog";
import { Download, ScanSearch } from "lucide-react";
import { useMemo } from "react";

import { isBox2d } from "../lib/overlay";
import type { OverlayConfig, ResultRow, ResultValue } from "../lib/types";
import { asImageValue, ImagePreview, imageMime } from "./ImagePreview";
import { Button } from "./ui/button";
import { Dialog, DialogContent } from "./ui/dialog";

export function InspectorDrawer({
  row,
  overlay,
  onOpenChange,
}: {
  row: ResultRow | null;
  overlay: OverlayConfig;
  onOpenChange: (open: boolean) => void;
}) {
  const image = overlay.imageColumn
    ? asImageValue(row?.values[overlay.imageColumn])
    : null;
  const box = overlay.boxColumn && row ? row.values[overlay.boxColumn] : null;
  const label =
    overlay.labelColumn && row ? row.values[overlay.labelColumn] : null;
  const confidence =
    overlay.confidenceColumn && row
      ? row.values[overlay.confidenceColumn]
      : null;
  const raw = useMemo(() => safeJson(row?.values ?? null), [row]);
  return (
    <Dialog open={Boolean(row)} onOpenChange={onOpenChange}>
      <DialogContent
        title={row ? `Row ${row.id} inspection` : "Row inspection"}
        description="Inspect returned thumbnail, overlay geometry, and nested Arrow values."
        side
        className="flex flex-col"
      >
        <header className="flex shrink-0 items-center gap-3 border-b border-hairline bg-canvas-soft px-5 py-4 pr-14">
          <span className="flex size-9 items-center justify-center rounded-lg border border-[#f2c7b6] bg-[#fff0e9] text-accent">
            <ScanSearch size={17} />
          </span>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-accent">
              Multimodal row inspector
            </p>
            <h2 className="mt-0.5 text-[16px] font-semibold text-ink">
              Row {row?.id ?? "—"}
            </h2>
          </div>
        </header>
        <div className="min-h-0 flex-1 space-y-5 overflow-y-auto p-5">
          <section>
            <div className="mb-2 flex items-center justify-between gap-3">
              <h3 className="text-[10px] font-semibold uppercase tracking-[0.11em] text-muted">
                Returned thumbnail
              </h3>
              {typeof image?.width === "number" &&
              typeof image?.height === "number" ? (
                <span className="font-mono text-[10px] text-muted">
                  {image.width}×{image.height}
                </span>
              ) : null}
            </div>
            <ImagePreview
              image={image}
              box={box ?? undefined}
              label={label ?? undefined}
              confidence={confidence ?? undefined}
              large
              className="rounded-xl"
            />
          </section>
          {isBox2d(box) ? (
            <section className="grid grid-cols-4 overflow-hidden rounded-lg border border-hairline bg-canvas-soft">
              {Object.entries(box).map(([key, value]) => (
                <div
                  key={key}
                  className="border-r border-hairline px-3 py-2.5 last:border-r-0"
                >
                  <p className="text-[9px] font-semibold uppercase tracking-[0.12em] text-muted">
                    {key}
                  </p>
                  <p className="mt-1 font-mono text-[12px] font-medium text-ink">
                    {value.toFixed(4)}
                  </p>
                </div>
              ))}
            </section>
          ) : null}
          <section>
            <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-[0.11em] text-muted">
              Arrow row values
            </h3>
            <pre className="max-h-[360px] overflow-auto rounded-lg border border-hairline bg-[#f9f8f3] p-4 font-mono text-[11px] leading-5 text-body">
              {raw}
            </pre>
          </section>
        </div>
        <footer className="flex shrink-0 justify-end gap-2 border-t border-hairline bg-canvas-soft px-5 py-3">
          <DialogPrimitive.Close asChild>
            <Button>Dismiss</Button>
          </DialogPrimitive.Close>
          <Button
            variant="primary"
            disabled={!image?.encoded || !isBox2d(box)}
            onClick={() =>
              image?.encoded &&
              isBox2d(box) &&
              exportCrop(image, box, row?.id ?? 0)
            }
          >
            <Download size={14} /> Export crop
          </Button>
        </footer>
      </DialogContent>
    </Dialog>
  );
}

async function exportCrop(
  image: NonNullable<ReturnType<typeof asImageValue>>,
  box: { x: number; y: number; w: number; h: number },
  rowId: number,
) {
  if (!image.encoded) return;
  const mime = imageMime(image.encoding) ?? "image/jpeg";
  const sourceUrl = URL.createObjectURL(
    new Blob([image.encoded as BlobPart], { type: mime }),
  );
  try {
    const source = await loadImage(sourceUrl);
    const x = clamp(box.x, 0, 1) * source.naturalWidth;
    const y = clamp(box.y, 0, 1) * source.naturalHeight;
    const right = clamp(box.x + box.w, 0, 1) * source.naturalWidth;
    const bottom = clamp(box.y + box.h, 0, 1) * source.naturalHeight;
    const width = Math.max(1, Math.round(right - x));
    const height = Math.max(1, Math.round(bottom - y));
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    canvas
      .getContext("2d")
      ?.drawImage(source, x, y, width, height, 0, 0, width, height);
    const blob = await new Promise<Blob | null>((resolve) =>
      canvas.toBlob(resolve, "image/png"),
    );
    if (!blob) return;
    const downloadUrl = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = downloadUrl;
    anchor.download = `visionql-row-${rowId}-crop.png`;
    anchor.click();
    URL.revokeObjectURL(downloadUrl);
  } finally {
    URL.revokeObjectURL(sourceUrl);
  }
}

function loadImage(url: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("Thumbnail could not be decoded"));
    image.src = url;
  });
}

function safeJson(
  value: ResultValue | Record<string, ResultValue> | null,
): string {
  return JSON.stringify(
    value,
    (_, nested) => {
      if (typeof nested === "bigint") return nested.toString();
      if (nested instanceof Uint8Array)
        return `<${nested.byteLength} encoded bytes>`;
      return nested;
    },
    2,
  );
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}
