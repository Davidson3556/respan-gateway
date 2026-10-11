import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

export function StatCard({
  label,
  value,
  hint,
  icon: Icon,
}: {
  label: string;
  /** undefined while loading */
  value: ReactNode | undefined;
  hint?: ReactNode;
  icon: LucideIcon;
}) {
  return (
    <Card className="gap-0 px-4 py-4">
      <div className="flex items-center justify-between text-muted-foreground">
        <span className="text-xs font-medium">{label}</span>
        <Icon className="size-3.5" aria-hidden="true" />
      </div>
      <div className="mt-2 text-2xl font-semibold tracking-tight tabular">
        {value === undefined ? <Skeleton className="h-8 w-20" /> : value}
      </div>
      {hint && <div className="mt-1 truncate text-xs text-muted-foreground tabular">{hint}</div>}
    </Card>
  );
}
