import { CircleAlert, Wrench } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { ToolLoopStep } from "@/lib/api";
import { formatBytes, formatCount, formatMs } from "@/lib/format";
import { cn } from "@/lib/utils";

/**
 * The calls the gateway made to the model for one request, and the tools it
 * ran between them: the tool loop, in order.
 */
export function ToolLoopSteps({ steps }: { steps: ToolLoopStep[] }) {
  const tools = steps.reduce((n, s) => n + s.tools.length, 0);
  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-3">
      <p className="text-sm text-muted-foreground">
        {steps.length} {steps.length === 1 ? "call" : "calls"} to the model, {tools}{" "}
        {tools === 1 ? "tool" : "tools"} run by the gateway.
      </p>
      <ol className="grid grid-cols-[minmax(0,1fr)] gap-2" aria-label="Tool loop steps">
        {steps.map((step) => (
          <li key={step.index} className="grid gap-2 rounded-lg border bg-muted/30 px-3 py-2.5">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <span className="font-mono text-sm font-medium">Step {step.index}</span>
              <span className="font-mono text-xs text-muted-foreground tabular">
                {formatMs(step.duration_ms)}
              </span>
              <span className="font-mono text-xs text-muted-foreground tabular">
                {formatCount(step.input_tokens)} in / {formatCount(step.output_tokens)} out
              </span>
              {step.finish && (
                <Badge variant="outline" className="ml-auto font-normal">
                  {step.finish}
                </Badge>
              )}
            </div>
            {step.tools.length > 0 && (
              <ul className="grid gap-1 border-l-2 border-brand/40 pl-3">
                {step.tools.map((tool, i) => (
                  <li
                    key={`${tool.name}-${i}`}
                    className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs"
                  >
                    {tool.error ? (
                      <CircleAlert className="size-3.5 shrink-0 text-destructive" aria-label="failed" />
                    ) : (
                      <Wrench className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                    )}
                    <span className={cn("min-w-0 break-all font-mono", tool.error && "text-destructive")}>
                      {tool.name}
                    </span>
                    <span className="font-mono text-muted-foreground tabular">
                      {formatMs(tool.duration_ms)} · {formatBytes(tool.bytes)}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ol>
    </div>
  );
}
