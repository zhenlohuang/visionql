import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import type { ReactNode } from "react";

import { cn } from "../../lib/cn";

export const Dialog = DialogPrimitive.Root;
export const DialogTrigger = DialogPrimitive.Trigger;
export const DialogClose = DialogPrimitive.Close;

export function DialogContent({
  children,
  className,
  title,
  description,
  side = false,
}: {
  children: ReactNode;
  className?: string;
  title: string;
  description?: string;
  side?: boolean;
}) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-[#26251e]/20 backdrop-blur-[1px] data-[state=open]:animate-in" />
      <DialogPrimitive.Content
        className={cn(
          "fixed z-50 border border-hairline bg-surface workbench-shadow focus:outline-none",
          side
            ? "panel-enter inset-y-0 right-0 w-full max-w-[560px] overflow-hidden"
            : "left-1/2 top-1/2 w-[calc(100%-32px)] max-w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-xl panel-enter",
          className,
        )}
      >
        <DialogPrimitive.Title className="sr-only">
          {title}
        </DialogPrimitive.Title>
        {description ? (
          <DialogPrimitive.Description className="sr-only">
            {description}
          </DialogPrimitive.Description>
        ) : null}
        {children}
        <DialogPrimitive.Close className="absolute right-4 top-4 flex size-8 items-center justify-center rounded-md text-muted transition-colors hover:bg-surface-raised hover:text-ink">
          <X size={17} aria-hidden="true" />
          <span className="sr-only">Close</span>
        </DialogPrimitive.Close>
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}
