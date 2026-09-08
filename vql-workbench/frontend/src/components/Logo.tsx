import { cn } from "../lib/cn";

export function Logo({ className }: { className?: string }) {
  return (
    <span
      aria-hidden="true"
      className={cn(
        "relative inline-block size-6 shrink-0 overflow-hidden",
        className,
      )}
    >
      <img
        src="/visionql-logo.png"
        alt=""
        className="pointer-events-none absolute left-1/2 top-1/2 h-[176%] w-[171%] max-w-none -translate-x-1/2 -translate-y-1/2 object-contain"
      />
    </span>
  );
}
