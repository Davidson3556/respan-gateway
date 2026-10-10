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
          The gateway answered 404: none of its providers handles {what}. Add a provider whose
          format covers it to <code className="text-foreground">gateway.yaml</code>, serve that
          format, and restart.
        </p>
        <Button asChild variant="outline" size="sm">
          <Link to="/providers">See configured providers</Link>
        </Button>
      </EmptyState>
    );
  }
  return (
    <div
      role="alert"
      className="flex gap-3 rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm"
    >
      <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
      <div className="min-w-0">
        <p className="font-medium">
          {error instanceof ApiError ? `The gateway answered ${error.status}` : "Request failed"}
        </p>
        <p className="break-words text-muted-foreground">{describeError(error)}</p>
      </div>
    </div>
  );
}
