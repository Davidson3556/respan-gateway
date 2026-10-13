import { useQueryClient } from "@tanstack/react-query";
import { CircleAlert, KeyRound, LoaderCircle } from "lucide-react";
import { useEffect, useState, type FormEvent } from "react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError, getGatewayKey, onForbidden, onUnauthorized, setGatewayKey, verifyGatewayKey } from "@/lib/api";

export function KeyDialog() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const prompt = (message: string | null) => {
      if (open) return;
      setError(message);
      setOpen(true);
    };
    const offUnauthorized = onUnauthorized(() => prompt(getGatewayKey() ? "Invalid key. Try another." : null));
    const offForbidden = onForbidden(() => prompt("This key has no admin access. Use an admin key."));
    return () => { offUnauthorized(); offForbidden(); };
  }, [open]);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    const key = value.trim();
    if (!key || busy) return;
    setBusy(true);
    setError(null);
    try {
      await verifyGatewayKey(key);
      setGatewayKey(key);
      setValue("");
      setOpen(false);
      void queryClient.invalidateQueries();
    } catch (e) {
      setError(e instanceof ApiError && e.status === 401
        ? "Invalid key. Try another."
        : e instanceof ApiError && e.status === 403
          ? "This key has no admin access. Use an admin key."
          : "Couldn't connect to the gateway. Try again.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open}>
      <DialogContent className="sm:max-w-sm" showCloseButton={false}
        onEscapeKeyDown={(e) => e.preventDefault()} onInteractOutside={(e) => e.preventDefault()}>
        <form onSubmit={submit} className="grid gap-5">
          <DialogHeader>
            <KeyRound className="mb-1 size-6 text-muted-foreground" />
            <DialogTitle>Gateway key required</DialogTitle>
            <DialogDescription>Enter an admin key to continue.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Label htmlFor="gateway-key">Gateway key</Label>
            <Input id="gateway-key" type="password" autoComplete="off" autoFocus
              placeholder="Paste your key" value={value} disabled={busy}
              onChange={(e) => { setValue(e.target.value); setError(null); }}
              aria-invalid={!!error} aria-describedby={error ? "gateway-key-error" : undefined} />
            {error && (
              <div id="gateway-key-error" role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm leading-relaxed">
                <CircleAlert className="mt-0.5 size-4 shrink-0 text-destructive" />
                <p>{error}</p>
              </div>
            )}
          </div>
          <DialogFooter>
            <Button type="submit" className="w-full" disabled={!value.trim() || busy}>
              {busy && <LoaderCircle className="animate-spin" />}
              {busy ? "Checking…" : "Connect"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
