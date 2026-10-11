import { RadioGroup } from "radix-ui";
import { useId, type ReactNode } from "react";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

export interface Choice {
  value: string;
  label: string;
  /** Shown under the label, in small type. */
  hint?: ReactNode;
}

/** A few options as cards, one chosen; arrow keys move between them. */
export function ChoiceGroup({
  label,
  value,
  onChange,
  choices,
  className,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  choices: Choice[];
  /** The grid's columns, as Tailwind classes. */
  className?: string;
}) {
  const id = useId();
  return (
    <div className="grid gap-2">
      <Label id={id}>{label}</Label>
      <RadioGroup.Root
        value={value}
        onValueChange={onChange}
        aria-labelledby={id}
        className={cn("grid gap-2", className)}
      >
        {choices.map((c) => (
          <RadioGroup.Item
            key={c.value}
            value={c.value}
            className="flex min-w-0 flex-col items-start gap-0.5 rounded-lg border px-3 py-2 text-left text-sm outline-none transition-colors hover:bg-muted/60 focus-visible:ring-[3px] focus-visible:ring-ring/50 data-[state=checked]:border-brand data-[state=checked]:bg-brand-soft/60"
          >
            <span className="w-full truncate font-medium">{c.label}</span>
            {c.hint && <span className="text-xs text-muted-foreground">{c.hint}</span>}
          </RadioGroup.Item>
        ))}
      </RadioGroup.Root>
    </div>
  );
}
