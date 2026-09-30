import { ImageOff } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { cn } from "../lib/cn";
import {
  isBox2d,
  overlayRect,
  type PixelRect,
  type Size,
} from "../lib/overlay";
import type { Box2dValue, ImageValue, ResultValue } from "../lib/types";

export function ImagePreview({
  image,
  box,
  label,
  confidence,
  large = false,
  className,
}: {
  image: ImageValue | null;
  box?: ResultValue;
  label?: ResultValue;
  confidence?: ResultValue;
  large?: boolean;
  className?: string;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [viewport, setViewport] = useState<Size>({ width: 0, height: 0 });
  const [natural, setNatural] = useState<Size>({ width: 0, height: 0 });
  const [decodeFailed, setDecodeFailed] = useState(false);
  const url = useImageUrl(image);
  const parsedBox = isBox2d(box) ? box : null;
  const imageSize = validImageSize(natural) ?? validImageSize(image);
  const rect =
    parsedBox && imageSize ? overlayRect(parsedBox, imageSize, viewport) : null;

  useLayoutEffect(() => {
    setDecodeFailed(false);
    setNatural({ width: 0, height: 0 });
  }, [url]);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const update = () =>
      setViewport({ width: element.clientWidth, height: element.clientHeight });
    update();
    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, [decodeFailed, url]);

  if (!url || decodeFailed) {
    return (
      <div
        className={cn(
          "flex items-center justify-center rounded-lg border border-dashed border-hairline-strong bg-canvas font-mono text-[10px] text-muted",
          large ? "aspect-video w-full" : "h-[72px] w-32",
          className,
        )}
        title={
          decodeFailed
            ? "Thumbnail could not be decoded"
            : "No encoded thumbnail"
        }
      >
        <span className="flex items-center gap-1.5">
          <ImageOff size={14} />{" "}
          {decodeFailed ? "Decode failed" : "No thumbnail"}
        </span>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={cn(
        "group/image relative overflow-hidden rounded-lg border border-hairline bg-[#eeece5]",
        large ? "aspect-video w-full" : "h-[72px] w-32",
        className,
      )}
    >
      <img
        src={url}
        alt="Returned VisionQL thumbnail"
        className="size-full object-contain"
        onLoad={(event) => {
          setDecodeFailed(false);
          setNatural({
            width: event.currentTarget.naturalWidth,
            height: event.currentTarget.naturalHeight,
          });
        }}
        onError={() => setDecodeFailed(true)}
      />
      {rect ? (
        <OverlayRect rect={rect} label={label} confidence={confidence} />
      ) : null}
    </div>
  );
}

function OverlayRect({
  rect,
  label,
  confidence,
}: {
  rect: PixelRect;
  label?: ResultValue;
  confidence?: ResultValue;
}) {
  const readableLabel = typeof label === "string" ? label : null;
  const score = typeof confidence === "number" ? confidence.toFixed(2) : null;
  return (
    <>
      <svg
        className="pointer-events-none absolute inset-0 size-full"
        aria-hidden="true"
      >
        <rect
          x={rect.x}
          y={rect.y}
          width={rect.width}
          height={rect.height}
          rx="2"
          fill="rgb(240 75 15 / 13%)"
          stroke="#f04b0f"
          strokeWidth="2"
        />
      </svg>
      {readableLabel || score ? (
        <span
          className="pointer-events-none absolute max-w-[calc(100%-6px)] -translate-y-full truncate rounded-sm bg-ink/92 px-1.5 py-0.5 font-mono text-[9px] text-white shadow-sm"
          style={{ left: Math.max(3, rect.x), top: Math.max(18, rect.y) }}
        >
          {readableLabel}{" "}
          {score ? <strong className="text-[#ffc0a8]">{score}</strong> : null}
        </span>
      ) : null}
    </>
  );
}

export function asImageValue(
  value: ResultValue | undefined,
): ImageValue | null {
  if (
    !value ||
    typeof value !== "object" ||
    value instanceof Uint8Array ||
    Array.isArray(value)
  ) {
    return null;
  }
  return value as ImageValue;
}

export function useImageUrl(image: ImageValue | null): string | null {
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    const encoded = image?.encoded;
    const mime = imageMime(image?.encoding);
    if (!(encoded instanceof Uint8Array) || !encoded.byteLength || !mime) {
      setUrl(null);
      return;
    }
    const next = URL.createObjectURL(
      new Blob([encoded as BlobPart], { type: mime }),
    );
    setUrl(next);
    return () => URL.revokeObjectURL(next);
  }, [image]);
  return url;
}

export function imageMime(encoding?: string | null): string | null {
  if (!encoding) return null;
  const normalized = encoding.toLowerCase();
  const aliases: Record<string, string> = {
    jpeg: "image/jpeg",
    jpg: "image/jpeg",
    png: "image/png",
    webp: "image/webp",
    gif: "image/gif",
    avif: "image/avif",
    "image/jpeg": "image/jpeg",
    "image/png": "image/png",
    "image/webp": "image/webp",
    "image/gif": "image/gif",
    "image/avif": "image/avif",
  };
  return aliases[normalized] ?? null;
}

function validImageSize(
  value: Pick<ImageValue, "width" | "height"> | Size | null,
): Size | null {
  const width = value?.width;
  const height = value?.height;
  return typeof width === "number" &&
    Number.isFinite(width) &&
    width > 0 &&
    typeof height === "number" &&
    Number.isFinite(height) &&
    height > 0
    ? { width, height }
    : null;
}
