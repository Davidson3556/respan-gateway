import { useState } from "react";
import { CheckIcon, ChevronsUpDownIcon, CornerDownLeftIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { ProviderIcon } from "@/components/provider-icon";
import { byProvider, modelFacts, type Listed } from "@/lib/models";
import { cn } from "@/lib/utils";

/**
 * A searchable list of models, grouped by provider, for when there are too
 * many to scroll: search matches an ID or a provider. With `custom`, the
 * typed ID can be used as it is; with `all`, a first entry stands for any
 * model, whose value is `allValue`.
 */
export function ModelCombobox({
  id,
  label,
  value,
  onChange,
  models,
  custom = false,
  all,
  allValue = "",
  size = "default",
  className,
  disabled,
}: {
  id?: string;
  /** Its accessible name, if no `<Label>` names it by `id`. */
  label?: string;
  value: string;
  onChange: (value: string) => void;
  models: Listed[];
  custom?: boolean;
  all?: string;
  allValue?: string;
  size?: "sm" | "default";
  className?: string;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const pick = (next: string) => {
    onChange(next);
    setOpen(false);
    setSearch("");
  };
  const typed = search.trim();
  const offerTyped = custom && typed && !models.some((m) => m.id === typed);
  const shown = all && value === allValue ? all : value;
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          id={id}
          aria-label={label}
          variant="outline"
          size={size}
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className={cn("w-full min-w-0 justify-between px-3 font-normal", className)}
        >
          <span className={cn("truncate", !(all && value === allValue) && "font-mono text-[13px]")}>
            {shown}
          </span>
          <ChevronsUpDownIcon className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-(--radix-popover-trigger-width) min-w-72 p-0">
        <Command className="[&_[cmdk-group-heading]]:px-2.5 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:text-muted-foreground [&_[cmdk-group]]:px-1.5">
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={`Search ${models.length} models${custom ? " or enter an ID" : ""}`}
            className="font-mono text-[13px]"
          />
          <CommandList>
            <CommandEmpty>No model matches.</CommandEmpty>
            {all && (
              <CommandGroup>
                <CommandItem value={`\0all:${all}`} onSelect={() => pick(allValue)}>
                  <span className="flex-1">{all}</span>
                  {value === allValue && <CheckIcon />}
                </CommandItem>
              </CommandGroup>
            )}
            {byProvider(models).map(([provider, group]) => (
              <CommandGroup
                key={provider}
                heading={
                  provider ? (
                    <span className="flex items-center gap-1.5">
                      <ProviderIcon name={provider} size="sm" />
                      {provider} · {group.length}
                    </span>
                  ) : undefined
                }
              >
                {group.map((m) => (
                  <CommandItem
                    key={m.id}
                    value={m.id}
                    keywords={[provider, m.display_name ?? ""].filter(Boolean)}
                    onSelect={() => pick(m.id)}
                  >
                    <span className="grid min-w-0 flex-1">
                      <span className="truncate font-mono text-[13px]">{m.id}</span>
                      {modelFacts(m) && (
                        <span className="truncate text-[11px] text-muted-foreground">{modelFacts(m)}</span>
                      )}
                    </span>
                    {m.id === value && <CheckIcon />}
                  </CommandItem>
                ))}
              </CommandGroup>
            ))}
            {offerTyped && (
              <CommandGroup forceMount heading="Not listed">
                <CommandItem
                  forceMount
                  value={`\0custom:${typed}`}
                  aria-label={`Use ${typed}`}
                  onSelect={() => pick(typed)}
                >
                  <CornerDownLeftIcon />
                  <span className="text-muted-foreground">Use</span>
                  <span className="min-w-0 truncate font-mono text-[13px]">{typed}</span>
                </CommandItem>
              </CommandGroup>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
