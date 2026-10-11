import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlert, CircleCheck, RefreshCw } from "lucide-react";
import { useRef, useState } from "react";
import { toast } from "sonner";
import { CodeBlock } from "@/components/code-block";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useConfig } from "@/hooks/queries";
import { api, ApiError, type ConfigView, type Reloaded } from "@/lib/api";
import { formatDateTime } from "@/lib/format";
import { describeError } from "@/lib/query-client";
import { cn } from "@/lib/utils";

/** A problem in the document, and where, if the gateway said. */
interface Problem {
  message: string;
  line: number | null;
  column: number | null;
}

function where({ line, column }: Problem) {
  if (line == null) return null;
  return column == null ? `Line ${line}` : `Line ${line}, column ${column}`;
}

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
        Reloaded{result.reloaded_at ? ` at ${formatDateTime(result.reloaded_at)}` : ""}.
        {result.restart_required.length === 0 && (
          <span className="text-muted-foreground">Nothing needs a restart.</span>
        )}
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

/** A monospace textarea with line numbers beside it, the line at fault marked. */
function Editor({
  value,
  onChange,
  problemLine,
}: {
  value: string;
  onChange: (value: string) => void;
  problemLine: number | null;
}) {
  const gutter = useRef<HTMLDivElement>(null);
  const lines = value.split("\n").length;
  return (
    <div className="flex overflow-hidden rounded-md border bg-muted/20 font-mono text-[12.5px] leading-6">
      <div
        ref={gutter}
        aria-hidden
        className="w-12 shrink-0 overflow-hidden border-r bg-muted/40 py-2 pr-2 text-right text-muted-foreground select-none"
        style={{ maxHeight: "28rem" }}
      >
        {Array.from({ length: lines }, (_, i) => (
          <div key={i} className={cn(problemLine === i + 1 && "bg-destructive/15 font-semibold text-destructive")}>
            {i + 1}
          </div>
        ))}
      </div>
      <Textarea
        aria-label="Config YAML"
        spellCheck={false}
        wrap="off"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={(e) => {
          if (gutter.current) gutter.current.scrollTop = e.currentTarget.scrollTop;
        }}
        className="max-h-[28rem] min-h-64 rounded-none border-0 bg-transparent py-2 font-mono text-[12.5px] leading-6 whitespace-pre shadow-none focus-visible:ring-0 dark:bg-transparent md:text-[12.5px]"
      />
    </div>
  );
}

function EditCard({ view }: { view: ConfigView }) {
  const queryClient = useQueryClient();
  const [text, setText] = useState(view.yaml);
  const [problem, setProblem] = useState<Problem | null>(null);
  const [valid, setValid] = useState(false);
  const edit = (next: string) => {
    setText(next);
    setProblem(null);
    setValid(false);
  };

  const validate = useMutation({
    mutationFn: () => api.validateConfig(text),
    onSuccess: (result) => {
      setValid(result.ok);
      setProblem(result.ok ? null : { message: result.error, line: result.line, column: result.column });
    },
    onError: (e) => setProblem({ message: describeError(e), line: null, column: null }),
  });
  const save = useMutation({
    mutationFn: () => api.saveConfig(text),
    onSuccess: (saved) => {
      toast.success("Config saved and reloaded", { description: saved.backup ? `Previous file kept as ${saved.backup}` : undefined });
      void queryClient.invalidateQueries();
    },
    onError: (e) =>
      setProblem({
        message: describeError(e),
        line: e instanceof ApiError ? e.line : null,
        column: e instanceof ApiError ? e.column : null,
      }),
  });
  const busy = validate.isPending || save.isPending;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Edit</CardTitle>
        <CardDescription>
          Secrets show as <code className="font-mono">{view.placeholder}</code>; leave one as it is to keep the
          file's value, or write a new value or an <code className="font-mono">env:NAME</code> reference. Saving
          keeps the previous file as <code className="font-mono">.bak</code> and re-renders the YAML, so comments are
          not kept.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-3">
        <Editor value={text} onChange={edit} problemLine={problem?.line ?? null} />
        {problem && (
          <p role="alert" className="flex items-start gap-2 text-sm text-destructive">
            <CircleAlert className="mt-0.5 size-4 shrink-0" />
            <span className="min-w-0 break-words">
              {where(problem) && <strong>{where(problem)}: </strong>}
              {problem.message}
            </span>
          </p>
        )}
        {valid && (
          <p role="status" className="flex items-center gap-2 text-sm">
            <CircleCheck className="size-4 text-emerald-600 dark:text-emerald-400" />
            Valid: this config would reload.
          </p>
        )}
      </CardContent>
      <CardFooter className="justify-end gap-2 border-t">
        <Button variant="ghost" onClick={() => edit(view.yaml)} disabled={busy || text === view.yaml}>
          Reset
        </Button>
        <Button variant="outline" onClick={() => validate.mutate()} disabled={busy}>
          Validate
        </Button>
        <Button onClick={() => save.mutate()} disabled={busy || text === view.yaml}>
          Save &amp; reload
        </Button>
      </CardFooter>
    </Card>
  );
}

function source(view: ConfigView) {
  return view.source === "file" ? (view.path ?? "config file") : `the ${view.source}`;
}

export function ConfigPage() {
  const { data, error, isPending } = useConfig();
  return (
    <div className="grid max-w-4xl grid-cols-1 gap-6">
      <PageHeader
        title="Config"
        description="The running gateway's configuration, secrets redacted."
        actions={<ReloadButton />}
      />
      {isPending && <Skeleton className="h-64 w-full" />}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          Couldn't load the config: {describeError(error)}
        </p>
      )}
      {data && (
        <>
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Current config</CardTitle>
              <CardDescription>
                From <span className="font-mono">{source(data)}</span>. Keys and other secrets are shown as{" "}
                <code className="font-mono">{data.placeholder}</code>; <code className="font-mono">env:NAME</code>{" "}
                references are shown as written.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <CodeBlock code={data.yaml} className="max-h-[28rem] overflow-auto" />
            </CardContent>
          </Card>
          {data.writes_enabled ? (
            <EditCard key={data.yaml} view={data} />
          ) : (
            <p className="text-sm text-muted-foreground">
              Editing is off: {data.writes_disabled_reason ?? "writes are disabled"}. Set{" "}
              <code className="font-mono">admin_writes: true</code> in a config file with client keys to edit here.
            </p>
          )}
        </>
      )}
    </div>
  );
}
