import { Search, Server } from "lucide-react";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { Button } from "@/components/ui/button";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { useProviders } from "@/hooks/queries";
import { useRequests } from "@/hooks/use-live-requests";
import { themeOptions, useTheme } from "@/hooks/use-theme";
import { pages } from "./nav";


/** The search button in the header, and the ⌘K palette it opens. */
export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();
  const { setTheme } = useTheme();
  const { data: providers } = useProviders();
  const { data: requests } = useRequests();

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key.toLowerCase() === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const run = (action: () => void) => () => {
    setOpen(false);
    action();
  };

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        className="h-8 gap-2 px-2.5 font-normal text-muted-foreground sm:w-56 sm:justify-start"
        onClick={() => setOpen(true)}
        aria-label="Search and jump to"
      >
        <Search />
        <span className="hidden sm:inline">Search…</span>
        <kbd className="ml-auto hidden rounded border px-1.5 font-sans text-[10px] sm:inline">⌘K</kbd>
      </Button>
      <CommandDialog
        open={open}
        onOpenChange={setOpen}
        title="Command palette"
        description="Jump to a page, provider or recent request."
      >
        <CommandInput placeholder="Jump to a page, provider or request…" />
        <CommandList>
          <CommandEmpty>Nothing matches.</CommandEmpty>
          <CommandGroup heading="Pages">
            {pages.map(({ to, label, icon: Icon }) => (
              <CommandItem key={to} value={`page ${label}`} onSelect={run(() => navigate(to))}>
                <Icon />
                {label}
              </CommandItem>
            ))}
          </CommandGroup>
          {providers && providers.length > 0 && (
            <CommandGroup heading="Providers">
              {providers.map((p) => (
                <CommandItem key={p.name} value={`provider ${p.name}`} onSelect={run(() => navigate("/providers"))}>
                  <Server />
                  {p.name}
                  <span className="ml-auto text-xs text-muted-foreground">{p.kinds.join(", ")}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
          {requests && requests.length > 0 && (
            <CommandGroup heading="Recent requests">
              {requests.slice(0, 8).map((r) => (
                <CommandItem
                  key={r.id}
                  value={`request ${r.id} ${r.model ?? ""} ${r.path}`}
                  onSelect={run(() => navigate(`/requests/${encodeURIComponent(r.id)}`))}
                >
                  <span className="truncate font-mono text-[13px]">{r.model ?? r.path}</span>
                  <span className="ml-auto font-mono text-xs text-muted-foreground">
                    {r.status ?? "…"}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
          <CommandGroup heading="Theme">
            {themeOptions.map(({ value, label, icon: Icon }) => (
              <CommandItem key={value} value={`theme ${label}`} onSelect={run(() => setTheme(value))}>
                <Icon />
                {label}
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </>
  );
}
