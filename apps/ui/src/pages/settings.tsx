import { useQueryClient } from "@tanstack/react-query";
import { Eye, EyeOff } from "lucide-react";
import { useState, type FormEvent } from "react";
import { toast } from "sonner";
import { CodeBlock } from "@/components/code-block";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useGatewayKey, useOverview } from "@/hooks/queries";
import { themeOptions, useTheme } from "@/hooks/use-theme";
import { setGatewayKey } from "@/lib/api";
import { formatDateTime, formatUptime } from "@/lib/format";
import { cn } from "@/lib/utils";

function KeyCard({ stored }: { stored: string | null }) {
  const { data } = useOverview();
  const queryClient = useQueryClient();
  const [value, setValue] = useState(stored ?? "");
  const [visible, setVisible] = useState(false);

  const save = (e: FormEvent) => {
    e.preventDefault();
    setGatewayKey(value.trim() || null);
    void queryClient.invalidateQueries();
    toast.success(value.trim() ? "Gateway key saved" : "Gateway key cleared");
  };

  return (
    <Card>
      <form onSubmit={save} className="grid gap-6">
        <CardHeader>
          <CardTitle className="text-base">Gateway key</CardTitle>
          <CardDescription>
            {data?.auth_required
              ? "This gateway requires a client key. "
              : "This gateway is open; a key is only needed if you configure client keys. "}
            It's sent as a bearer token on every call from this browser, including the playground,
            and stored only in this browser.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-2">
          <Label htmlFor="settings-key">Key</Label>
          <div className="flex gap-2">
            <Input
              id="settings-key"
              type={visible ? "text" : "password"}
              autoComplete="off"
              placeholder="No key set"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              className="font-mono"
            />
            <Button
              type="button"
              variant="outline"
              size="icon"
              onClick={() => setVisible((v) => !v)}
              aria-label={visible ? "Hide key" : "Show key"}
            >
              {visible ? <EyeOff /> : <Eye />}
            </Button>
          </div>
        </CardContent>
        <CardFooter className="justify-end gap-2 border-t">
          {stored && (
            <Button
              type="button"
              variant="ghost"
              onClick={() => {
                setValue("");
                setGatewayKey(null);
                void queryClient.invalidateQueries();
                toast.success("Gateway key cleared");
              }}
            >
              Clear
            </Button>
          )}
          <Button type="submit" disabled={value.trim() === (stored ?? "")}>
            Save
          </Button>
        </CardFooter>
      </form>
    </Card>
  );
}

function ThemeCard() {
  const { theme, setTheme } = useTheme();
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Appearance</CardTitle>
        <CardDescription>System follows your operating system's setting.</CardDescription>
      </CardHeader>
      <CardContent>
        <div role="radiogroup" aria-label="Theme" className="grid grid-cols-3 gap-2 sm:max-w-md">
          {themeOptions.map(({ value, label, icon: Icon }) => (
            <button
              key={value}
              type="button"
              role="radio"
              aria-checked={theme === value}
              onClick={() => setTheme(value)}
              className={cn(
                "flex flex-col items-center gap-2 rounded-lg border p-3 text-sm transition-colors outline-none hover:bg-muted/60 focus-visible:ring-[3px] focus-visible:ring-ring/50",
                theme === value && "border-brand bg-brand-soft/60 text-foreground",
              )}
            >
              <Icon className="size-4" />
              {label}
            </button>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

const sources: Record<string, string> = {
  environment: "Environment variables",
  proxy: "Proxy mode (no providers configured)",
  inline: "Built in code",
};

function describeSource(source: string) {
  return sources[source] ?? `Config file (${source})`;
}

function ConnectCard() {
  const base = `${window.location.origin}/v1`;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Use this gateway</CardTitle>
        <CardDescription>
          Point an OpenAI or Anthropic client here; the SDK adds the endpoint paths itself.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <CodeBlock code={base} />
      </CardContent>
    </Card>
  );
}

function AboutCard() {
  const { data } = useOverview();
  if (!data) return null;
  const rows: [string, string][] = [
    ["Version", data.version],
    ["Listening on", data.listen ?? "not listening yet"],
    ["Configuration", describeSource(data.config_source)],
    ["Client keys", data.auth_required ? "Required" : "Not required"],
    ["Uptime", formatUptime(data.uptime_seconds)],
    ["Started", formatDateTime(data.started_at)],
  ];
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">About this gateway</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="grid gap-x-4 gap-y-2.5 text-sm sm:grid-cols-[10rem_1fr]">
          {rows.map(([k, v]) => (
            <div key={k} className="grid gap-0.5 sm:contents">
              <dt className="text-muted-foreground">{k}</dt>
              <dd className="min-w-0 font-mono text-[13px] break-words">{v}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}

export function SettingsPage() {
  const stored = useGatewayKey();
  return (
    <div className="grid max-w-3xl grid-cols-1 gap-6">
      <PageHeader title="Settings" description="Preferences for this browser." />
      {/* Remount when the key changes elsewhere (the key dialog) to show it. */}
      <KeyCard key={stored ?? ""} stored={stored} />
      <ThemeCard />
      <ConnectCard />
      <AboutCard />
    </div>
  );
}
