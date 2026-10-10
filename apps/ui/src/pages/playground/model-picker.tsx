import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import type { ModelChoice } from "@/hooks/use-model-choice";

/** A model from /v1/models, or a typed name when the gateway lists none for the kind. */
export function ModelPicker({ id, choice }: { id: string; choice: ModelChoice }) {
  const { model, chosen, setModel, models, isPending, fallback } = choice;
  return (
    <div className="grid gap-2">
      <Label htmlFor={id}>Model</Label>
      {isPending ? (
        <Skeleton className="h-9 w-full" />
      ) : models?.length ? (
        <Select value={model} onValueChange={setModel}>
          <SelectTrigger id={id} className="w-full">
            <SelectValue placeholder="Choose a model" />
          </SelectTrigger>
          <SelectContent>
            {models.map((m) => (
              <SelectItem key={m.id} value={m.id}>
                <span className="font-mono text-[13px]">{m.id}</span>
                <span className="text-xs text-muted-foreground">{m.owned_by}</span>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ) : (
        <Input
          id={id}
          value={chosen}
          placeholder={fallback}
          onChange={(e) => setModel(e.target.value)}
          className="font-mono"
        />
      )}
    </div>
  );
}
