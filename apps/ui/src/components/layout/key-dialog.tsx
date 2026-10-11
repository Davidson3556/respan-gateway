import { useQueryClient } from "@tanstack/react-query";
import { KeyRound } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { getGatewayKey, onForbidden, onUnauthorized, setGatewayKey, subscribeGatewayKey } from "@/lib/api";

/** Opens when a call is refused with 401 (asks for a key) or 403 (the key may not open the admin UI). */
export function KeyDialog() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [rejected, setRejected] = useState(false);
  const [forbidden, setForbidden] = useState(false);
  const [value, setValue] = useState("");
  // Once dismissed, background polling must not keep reopening the dialog;
  // it asks again only after the key changes (for instance in Settings).
  const dismissed = useRef(false);

  useEffect(() => {
    const offKey = subscribeGatewayKey(() => (dismissed.current = false));
    const offUnauthorized = onUnauthorized(() => {
      if (dismissed.current) return;
      setRejected(getGatewayKey() !== null);
      setForbidden(false);
      setOpen(true);
    });
    const offForbidden = onForbidden(() => {
      if (dismissed.current) return;
      setForbidden(true);
      setOpen(true);
    });
    return () => {
      offKey();
      offUnauthorized();
      offForbidden();
    };
  }, []);

  const onOpenChange = (next: boolean) => {
    if (!next) dismissed.current = true;
    setOpen(next);
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (!value.trim()) return;
    setGatewayKey(value.trim());
    setValue("");
    setOpen(false);
    void queryClient.invalidateQueries();
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <form onSubmit={submit} className="grid gap-5">
          <DialogHeader>
            <div className="mb-2 flex size-10 items-center justify-center rounded-full bg-brand-soft text-brand">
              <KeyRound className="size-5" />
            </div>
            <DialogTitle>{forbidden
                ? "This key can't open the admin UI"
                : rejected
                  ? "That key was refused"
                  : "This gateway needs a key"}
            </DialogTitle>
            <DialogDescription>
              {forbidden
                ? "The key is valid, but it isn't an admin key. Enter an admin key to use the dashboard."
                : "The gateway is configured with client keys. Enter one to use the dashboard and the playground; it's kept in this browser only."}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Label htmlFor="gateway-key">Gateway key</Label>
            <Input
              id="gateway-key"
              type="password"
              autoComplete="off"
              autoFocus
              placeholder="sk-gw-…"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              aria-invalid={rejected || forbidden || undefined}
            />
          </div>
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>
              Not now
            </Button>
            <Button type="submit" disabled={!value.trim()}>
              Save key
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
