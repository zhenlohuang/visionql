import { SlidersHorizontal } from "lucide-react";

import type { OverlayConfig, ResultField } from "../lib/types";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogTrigger } from "./ui/dialog";
import { Select } from "./ui/select";

const NONE = "__none__";

export function OverlayDialog({
  fields,
  value,
  onChange,
}: {
  fields: ResultField[];
  value: OverlayConfig;
  onChange: (value: OverlayConfig) => void;
}) {
  const options = fields.map((field) => ({
    value: field.key,
    label: field.key,
  }));
  const optional = [{ value: NONE, label: "Not mapped" }, ...options];
  const set = (key: keyof OverlayConfig, selected: string) =>
    onChange({ ...value, [key]: selected === NONE ? null : selected });
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button size="sm" className="h-8">
          <SlidersHorizontal size={14} className="text-accent" />
          Overlay config
        </Button>
      </DialogTrigger>
      <DialogContent
        title="Overlay configuration"
        description="Map returned Arrow columns to thumbnail overlays."
      >
        <div className="border-b border-hairline px-5 py-4 pr-14">
          <p className="text-[11px] font-semibold uppercase tracking-[0.12em] text-accent">
            Presentation only
          </p>
          <h2 className="mt-1 text-lg font-semibold tracking-[-0.02em] text-ink">
            Overlay configuration
          </h2>
          <p className="mt-1 text-[13px] leading-5 text-body">
            Pair one returned IMAGE with BOX2D, label, and confidence columns.
            This does not change the query or server result.
          </p>
        </div>
        <div className="grid gap-4 p-5 sm:grid-cols-2">
          <Field label="Image column">
            <Select
              value={value.imageColumn ?? undefined}
              onValueChange={(selected) => set("imageColumn", selected)}
              options={options.filter((field) =>
                fields.find(
                  (candidate) =>
                    candidate.key === field.value &&
                    candidate.extensionName === "vql.image",
                ),
              )}
              placeholder="Select IMAGE"
            />
          </Field>
          <Field label="Box column">
            <Select
              value={value.boxColumn ?? undefined}
              onValueChange={(selected) => set("boxColumn", selected)}
              options={optional.filter(
                (field) =>
                  field.value === NONE ||
                  fields.find(
                    (candidate) =>
                      candidate.key === field.value &&
                      candidate.extensionName === "vql.box2d",
                  ),
              )}
              placeholder="Select BOX2D"
            />
          </Field>
          <Field label="Label column">
            <Select
              value={value.labelColumn ?? NONE}
              onValueChange={(selected) => set("labelColumn", selected)}
              options={optional}
              placeholder="Not mapped"
            />
          </Field>
          <Field label="Confidence column">
            <Select
              value={value.confidenceColumn ?? NONE}
              onValueChange={(selected) => set("confidenceColumn", selected)}
              options={optional}
              placeholder="Not mapped"
            />
          </Field>
        </div>
        <div className="border-t border-hairline bg-canvas-soft px-5 py-3 font-mono text-[10px] text-muted">
          Cleared automatically when the browser Session closes.
        </div>
      </DialogContent>
    </Dialog>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="grid gap-1.5 text-[11px] font-semibold uppercase tracking-[0.1em] text-muted">
      {label}
      {children}
    </label>
  );
}
