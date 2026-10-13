import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlert, CircleCheck, RefreshCw } from "lucide-react";
import { CodeBlock } from "@/components/code-block";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useConfig } from "@/hooks/queries";
import { api, type ConfigView, type Reloaded } from "@/lib/api";
import { describeError } from "@/lib/query-client";
import { cn } from "@/lib/utils";

function ReloadResult({ result, error }: { result?: Reloaded; error?: unknown }) {
  if (error) {
    return (
      <p role="alert" className="flex items-start gap-2 text-sm text-destructive">
        <CircleAlert className="mt-0.5 size-4 shrink-0" />
        <span className="min-w-0 break-words">Reload failed: {describeError(error)}. The previous config keeps serving.</span>
      </p>
    );
  }
  if (!result) return null;
  return (
    <div role="status" className="grid gap-1 text-sm">
      <p className="flex items-center gap-2">
        <CircleCheck className="size-4 text-emerald-600 dark:text-emerald-400" />
        Reloaded{result.reloaded_at ? ` at ${new Date(result.reloaded_at).toLocaleTimeString()}` : ""}.
      </p>
      {result.restart_required.map((note) => (
        <p key={note} className="pl-6 text-muted-foreground">
          Restart required: {note}
        </p>
      ))}
    </div>
  );
}

function ReloadButton() {
  const queryClient = useQueryClient();
  const reload = useMutation({
    mutationFn: () => api.reload(),
    onSuccess: () => void queryClient.invalidateQueries(),
  });
  return (
    <div className="flex flex-col items-end gap-2">
      <Button variant="outline" onClick={() => reload.mutate()} disabled={reload.isPending}>
        <RefreshCw className={cn(reload.isPending && "animate-spin")} />
        Reload
      </Button>
      <ReloadResult result={reload.data} error={reload.error} />
    </div>
  );
}

function source(view: ConfigView) {
  return view.source === "file" ? (view.path ?? "config file") : `the ${view.source}`;
}

export function ConfigPage() {
  const { data, error, isPending } = useConfig();
  return (
    <div className="grid max-w-4xl grid-cols-1 gap-6">
      <PageHeader title="Config" actions={<ReloadButton />} />
      {isPending && <Skeleton className="h-64 w-full" />}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          Couldn't load the config: {describeError(error)}
        </p>
      )}
      {data && (
          <Card>
            <CardHeader>
              <CardTitle className="font-mono text-sm">{source(data)}</CardTitle>
              <CardDescription>Secrets hidden</CardDescription>
            </CardHeader>
            <CardContent>
              <CodeBlock language="toml" code={data.text} className="max-h-[28rem] overflow-auto" />
            </CardContent>
          </Card>
      )}
    </div>
  );
}
