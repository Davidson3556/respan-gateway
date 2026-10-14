import { ModelCombobox } from "@/components/model-combobox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import type { ModelChoice } from "@/hooks/use-model-choice";

/**
 * Choose a listed model, searching by its ID or provider, or use whatever
 * ID is typed. With no models listed, a plain ID field.
 */
export function ModelPicker({
  id,
  choice,
  disabled,
}: {
  id: string;
  choice: ModelChoice;
  disabled?: boolean;
}) {
  const { model, chosen, setModel, models, isPending, fallback } = choice;
  return (
    <div className="grid min-w-0 gap-2">
      <Label htmlFor={id}>Model</Label>
      {isPending ? (
        <Skeleton className="h-9 w-full" />
      ) : models?.length ? (
        <ModelCombobox id={id} value={model} onChange={setModel} models={models} custom disabled={disabled} />
      ) : (
        <Input
          id={id}
          value={chosen}
          placeholder={fallback}
          onChange={(e) => setModel(e.target.value)}
          className="min-w-0 font-mono"
          autoComplete="off"
          spellCheck={false}
          disabled={disabled}
        />
      )}
    </div>
  );
}
