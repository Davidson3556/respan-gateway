import { ProviderIcon } from "@/components/provider-icon";
import { Badge } from "@/components/ui/badge";

/** Which provider served a call, with its icon. */
export function ServedBadge({ provider }: { provider: string }) {
  return (
    <Badge variant="outline" className="gap-1.5 font-normal">
      served by <ProviderIcon name={provider} size="sm" />
      <span className="font-mono">{provider}</span>
    </Badge>
  );
}
