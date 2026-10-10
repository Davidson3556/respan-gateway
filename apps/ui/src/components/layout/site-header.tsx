import { useLocation } from "react-router";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useLiveMode, type LiveMode } from "@/hooks/use-live-requests";
import { cn } from "@/lib/utils";
import { ThemeToggle } from "./theme-toggle";

const titles: [prefix: string, title: string][] = [
  ["/requests", "Requests"],
  ["/playground", "Playground"],
  ["/providers", "Providers"],
  ["/routes", "Routes"],
  ["/settings", "Settings"],
];

const liveCopy: Record<LiveMode, { label: string; hint: string; dot: string }> = {
  live: { label: "Live", hint: "Streaming request events", dot: "bg-success" },
  polling: {
    label: "Polling",
    hint: "Event stream unavailable; refreshing every few seconds",
    dot: "bg-warning",
  },
  connecting: { label: "Connecting", hint: "Opening the event stream", dot: "bg-muted-foreground" },
};

function LiveIndicator() {
  const mode = useLiveMode();
  const { label, hint, dot } = liveCopy[mode];
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className="inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs text-muted-foreground"
          tabIndex={0}
          role="status"
        >
          <span className="relative flex size-1.5">
            {mode === "live" && (
              <span className={cn("absolute inset-0 animate-ping rounded-full opacity-70", dot)} />
            )}
            <span className={cn("relative size-1.5 rounded-full", dot)} />
          </span>
          {label}
        </span>
      </TooltipTrigger>
      <TooltipContent>{hint}</TooltipContent>
    </Tooltip>
  );
}

export function SiteHeader() {
  const { pathname } = useLocation();
  const title = titles.find(([prefix]) => pathname.startsWith(prefix))?.[1] ?? "Overview";
  return (
    <header className="sticky top-0 z-10 flex h-14 shrink-0 items-center gap-2 rounded-t-xl border-b bg-background/80 px-4 backdrop-blur supports-[backdrop-filter]:bg-background/70">
      <SidebarTrigger className="-ml-1" />
      <Separator orientation="vertical" className="mx-1 data-[orientation=vertical]:h-4" />
      <span className="text-sm font-medium">{title}</span>
      <div className="ml-auto flex items-center gap-2">
        <LiveIndicator />
        <ThemeToggle />
      </div>
    </header>
  );
}
