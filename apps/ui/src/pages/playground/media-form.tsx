import { Loader2 } from "lucide-react";
import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import type { RequestRecord } from "@/lib/api";
import type { ApiChoice } from "@/hooks/use-playground";
import { ConfigCard, PlaygroundLayout } from "./layout";

/**
 * The shared shape of the media tabs: the configuration card as a form, its
 * one submit button showing progress, and results beside it.
 */
export function MediaLayout({
  id,
  choice,
  record,
  onSubmit,
  fields,
  submitLabel,
  pending,
  disabled,
  children,
}: {
  id: string;
  choice: ApiChoice;
  record?: RequestRecord;
  onSubmit: () => void;
  fields: ReactNode;
  submitLabel: string;
  pending: boolean;
  disabled?: boolean;
  children: ReactNode;
}) {
  const config = (
    <ConfigCard
      id={id}
      kind={choice.kind}
      apis={choice.apis}
      api={choice.api}
      onApi={choice.choose}
      record={record}
      onSubmit={() => !pending && !disabled && onSubmit()}
    >
      {fields}
      <Button type="submit" disabled={pending || disabled}>
        {pending && <Loader2 className="animate-spin" />}
        {submitLabel}
      </Button>
    </ConfigCard>
  );
  return (
    <PlaygroundLayout config={config}>
      <div className="min-w-0" aria-live="polite">
        {children}
      </div>
    </PlaygroundLayout>
  );
}
