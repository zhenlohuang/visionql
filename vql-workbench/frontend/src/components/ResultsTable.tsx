import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { useMemo } from "react";

import { cn } from "../lib/cn";
import { isBox2d } from "../lib/overlay";
import type {
  OverlayConfig,
  QueryResult,
  ResultField,
  ResultRow,
  ResultValue,
} from "../lib/types";
import { asImageValue, ImagePreview } from "./ImagePreview";
import { Button } from "./ui/button";

export function ResultsTable({
  result,
  overlay,
  selectedRowId,
  onSelectRow,
}: {
  result: QueryResult;
  overlay: OverlayConfig;
  selectedRowId: number | null;
  onSelectRow: (row: ResultRow) => void;
}) {
  const columns = useMemo<ColumnDef<ResultRow>[]>(
    () => [
      {
        id: "row",
        header: "#",
        cell: ({ row }) => (
          <span className="font-mono text-[11px] text-muted">
            {row.original.id}
          </span>
        ),
        size: 48,
      },
      ...result.fields.map((field, index) => ({
        id: `column:${index}`,
        accessorFn: (row: ResultRow) => row.values[field.key],
        header: () => <ColumnHeader field={field} overlay={overlay} />,
        cell: ({ row }: { row: { original: ResultRow } }) => (
          <ResultCell field={field} row={row.original} overlay={overlay} />
        ),
      })),
    ],
    [overlay, result.fields],
  );
  const table = useReactTable({
    data: result.rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageIndex: 0, pageSize: 25 } },
  });
  const pagination = table.getState().pagination;
  const first = result.rows.length
    ? pagination.pageIndex * pagination.pageSize + 1
    : 0;
  const last = Math.min(result.rows.length, first + pagination.pageSize - 1);
  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface">
      <div className="min-h-0 flex-1 overflow-auto">
        <table className="w-full min-w-[760px] border-collapse text-left">
          <thead className="sticky top-0 z-10 bg-canvas-soft/95 backdrop-blur-sm">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b border-hairline">
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="h-10 whitespace-nowrap px-4 text-[10px] font-semibold uppercase tracking-[0.1em] text-muted"
                    style={
                      header.column.id === "row" ? { width: 48 } : undefined
                    }
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext(),
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y divide-hairline">
            {table.getRowModel().rows.map((row, index) => (
              <tr
                key={row.id}
                className={cn(
                  "row-enter group cursor-pointer transition-colors hover:bg-canvas-soft",
                  selectedRowId === row.original.id &&
                    "border-l-2 border-l-accent bg-[#fff8f5]",
                )}
                style={{ animationDelay: `${Math.min(index, 8) * 24}ms` }}
                onClick={() => onSelectRow(row.original)}
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className="px-4 py-3 align-middle text-[12px] text-body"
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex h-11 shrink-0 items-center justify-between border-t border-hairline bg-canvas-soft px-4 font-mono text-[10px] text-muted">
        <span>
          {result.receivedBatches} batch
          {result.receivedBatches === 1 ? "" : "es"}
          {result.rolling ? " · rolling 500-row window" : ""}
        </span>
        <div className="flex items-center gap-1">
          <span className="mr-1">
            Showing {first}–{last} of {result.rows.length}
          </span>
          <Button
            size="icon"
            variant="ghost"
            className="size-7"
            disabled={!table.getCanPreviousPage()}
            onClick={(event) => {
              event.stopPropagation();
              table.previousPage();
            }}
            aria-label="Previous page"
          >
            <ChevronLeft size={14} />
          </Button>
          <Button
            size="icon"
            variant="ghost"
            className="size-7"
            disabled={!table.getCanNextPage()}
            onClick={(event) => {
              event.stopPropagation();
              table.nextPage();
            }}
            aria-label="Next page"
          >
            <ChevronRight size={14} />
          </Button>
        </div>
      </div>
    </div>
  );
}

function ColumnHeader({
  field,
  overlay,
}: {
  field: ResultField;
  overlay: OverlayConfig;
}) {
  const mapped =
    field.key === overlay.imageColumn && field.extensionName === "vql.image"
      ? overlay.boxColumn
        ? " + BOX2D"
        : ""
      : "";
  return (
    <span>
      {field.name}{" "}
      <span className="font-mono font-normal tracking-normal text-[#a7a299]">
        [{field.type}
        {mapped}]
      </span>
    </span>
  );
}

function ResultCell({
  field,
  row,
  overlay,
}: {
  field: ResultField;
  row: ResultRow;
  overlay: OverlayConfig;
}) {
  const value = row.values[field.key];
  if (field.extensionName === "vql.image") {
    const mapped = field.key === overlay.imageColumn;
    return (
      <ImagePreview
        image={asImageValue(value)}
        box={
          mapped && overlay.boxColumn
            ? row.values[overlay.boxColumn]
            : undefined
        }
        label={
          mapped && overlay.labelColumn
            ? row.values[overlay.labelColumn]
            : undefined
        }
        confidence={
          mapped && overlay.confidenceColumn
            ? row.values[overlay.confidenceColumn]
            : undefined
        }
      />
    );
  }
  if (field.extensionName === "vql.box2d" && isBox2d(value)) {
    return (
      <code className="whitespace-nowrap rounded border border-hairline bg-canvas-soft px-2 py-1 text-[10px] text-ink">
        {`{x: ${decimal(value.x)}, y: ${decimal(value.y)}, w: ${decimal(value.w)}, h: ${decimal(value.h)}}`}
      </code>
    );
  }
  if (field.key === overlay.labelColumn && typeof value === "string") {
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-surface-raised px-2 py-1 text-[11px] font-medium text-ink">
        <span className="size-1.5 rounded-full bg-accent" /> {value}
      </span>
    );
  }
  if (field.key === overlay.confidenceColumn && typeof value === "number") {
    return (
      <span className="flex min-w-28 items-center gap-2 font-mono text-[11px] text-ink">
        <span className="w-9">{decimal(value)}</span>
        <span className="h-1.5 w-20 overflow-hidden rounded-full bg-hairline">
          <span
            className="block h-full rounded-full bg-accent"
            style={{ width: `${Math.max(0, Math.min(100, value * 100))}%` }}
          />
        </span>
      </span>
    );
  }
  return (
    <span className="font-mono text-[11px] text-ink">{formatValue(value)}</span>
  );
}

export function formatValue(value: ResultValue): string {
  if (value == null) return "NULL";
  if (value instanceof Uint8Array) return `<${value.byteLength} bytes>`;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "object") {
    return JSON.stringify(value, (_, nested) =>
      typeof nested === "bigint" ? nested.toString() : nested,
    );
  }
  return String(value);
}

function decimal(value: number): string {
  return Number.isInteger(value)
    ? String(value)
    : value.toFixed(3).replace(/0+$/, "").replace(/\.$/, "");
}
