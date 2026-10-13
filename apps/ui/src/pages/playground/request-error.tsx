import { CircleAlert, PlugZap } from "lucide-react";
import { Link } from "react-router";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { ApiError } from "@/lib/api";
import { describeError } from "@/lib/query-client";

/**
 * How a playground call failed. A 404 means no served format or provider
 * handles this kind yet, which deserves guidance rather than an error.
 */
export function RequestError({ error, what }: { error: unknown; what: string }) {
  if (error instanceof ApiError && error.status === 404) {
    return (
      <EmptyState icon={PlugZap} title={`No provider serves ${what} yet`}>
        <p>
          Add a provider for {what} in <code>gateway.toml</code>.
        </p>
        <Button asChild variant="outline" size="sm">
          <Link to="/providers">View providers</Link>
        </Button>
      </EmptyState>
    );
  }
  const interrupted = error instanceof Error && error.message === "stream ended before completion";
  return (
    <div
      role="alert"
      className="flex gap-3 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm"
    >
      <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
      <div className="min-w-0">
        <p className="font-medium">
          {interrupted ? "Response interrupted" : error instanceof ApiError ? `Request failed · ${error.status}` : "Request failed"}
        </p>
        <p className="mt-1 leading-relaxed whitespace-pre-wrap break-words text-foreground/90">{interrupted ? "The stream ended early. You can continue chatting." : describeError(error)}</p>
      </div>
    </div>
  );
}
