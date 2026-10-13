import { useState } from "react";
import { Button } from "@/components/ui/button";
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

/** Choose a listed model or enter any custom model ID. */
export function ModelPicker({ id, choice }: { id: string; choice: ModelChoice }) {
  const { model, chosen, setModel, models, isPending, fallback } = choice;
  const [custom, setCustom] = useState(false);
  return (
    <div className="grid min-w-0 gap-2">
      <div className="flex min-w-0 items-center justify-between gap-2">
        <Label htmlFor={id}>{custom ? "Model ID" : "Model"}</Label>
        {!!models?.length && (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="-my-2 h-7 px-2 text-xs text-muted-foreground"
            onClick={() => {
              if (!custom) setModel(model);
              else if (!models.some((m) => m.id === model)) setModel(models[0].id);
              setCustom(!custom);
            }}
          >
            {custom ? "Choose model" : "Custom ID"}
          </Button>
        )}
      </div>
      {isPending ? (
        <Skeleton className="h-9 w-full" />
      ) : models?.length && !custom ? (
        <Select value={model} onValueChange={setModel}>
          <SelectTrigger id={id} className="w-full min-w-0 [&_[data-slot=select-value]]:min-w-0 [&_[data-slot=select-value]]:truncate">
            <SelectValue placeholder="Choose a model"><span className="block truncate font-mono text-[13px]">{model}</span></SelectValue>
          </SelectTrigger>
          <SelectContent>
            {models.map((m) => (
              <SelectItem key={m.id} value={m.id}>
                <span className="min-w-0 truncate font-mono text-[13px]">{m.id}</span>
                <span className="text-xs text-muted-foreground">{m.owned_by}</span>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ) : (
        <Input
          id={id}
          aria-label={custom ? "Custom model ID" : undefined}
          value={chosen}
          placeholder={fallback}
          onChange={(e) => setModel(e.target.value)}
          className="min-w-0 font-mono"
          autoComplete="off"
          spellCheck={false}
          autoFocus={custom}
          onFocus={(e) => custom && e.target.select()}
        />
      )}
    </div>
  );
}
