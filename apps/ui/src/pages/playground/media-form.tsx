import { Loader2 } from "lucide-react";
import type { FormEvent, ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

/**
 * The shared shape of the media tabs: a form card on the left, results on
 * the right, and one submit button that shows progress.
 */
export function MediaLayout({
  onSubmit,
  fields,
  submitLabel,
  pending,
  disabled,
  children,
}: {
  onSubmit: () => void;
  fields: ReactNode;
  submitLabel: string;
  pending: boolean;
  disabled?: boolean;
  children: ReactNode;
}) {
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (!pending && !disabled) onSubmit();
  };
  return (
    <div className="grid gap-4 lg:grid-cols-[22rem_1fr]">
      <Card className="h-fit p-4 shadow-none">
        <form onSubmit={submit} className="grid gap-5">
          {fields}
          <Button type="submit" disabled={pending || disabled}>
            {pending && <Loader2 className="animate-spin" />}
            {submitLabel}
          </Button>
        </form>
      </Card>
      <div className="min-w-0">{children}</div>
    </div>
  );
}
