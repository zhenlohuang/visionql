import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/cn";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-[13px] font-medium transition-[background,color,border-color,transform,opacity] duration-150 disabled:pointer-events-none disabled:opacity-45 active:translate-y-px",
  {
    variants: {
      variant: {
        primary: "bg-accent text-white hover:bg-accent-strong",
        secondary:
          "border border-hairline bg-surface text-body shadow-sm hover:border-hairline-strong hover:bg-canvas-soft hover:text-ink",
        ghost: "text-muted hover:bg-surface-raised/60 hover:text-ink",
        danger: "bg-danger text-white hover:bg-[#ac2848]",
      },
      size: {
        sm: "h-8 px-3",
        md: "h-9 px-4",
        icon: "size-8 p-0",
      },
    },
    defaultVariants: { variant: "secondary", size: "md" },
  },
);

export interface ButtonProps
  extends
    React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, type = "button", ...props }, ref) => (
    <button
      ref={ref}
      type={type}
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  ),
);
Button.displayName = "Button";
