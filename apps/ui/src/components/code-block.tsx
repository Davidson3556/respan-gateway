import { CopyButton } from "@/components/copy-button";
import { TomlCode } from "@/components/toml-code";
import { cn } from "@/lib/utils";

/** A copyable snippet, used for commands and raw JSON. */
export function CodeBlock({ code, className, language }: { code: string; className?: string; language?: "toml" }) {
  return (
    <div className={cn("group relative rounded-lg border bg-muted/40", className)}>
      <pre className="overflow-x-auto p-3 pr-11 text-[12.5px] leading-relaxed">
        <code>{language === "toml" ? <TomlCode code={code} /> : code}</code>
      </pre>
      <CopyButton value={code} className="absolute top-2 right-2" />
    </div>
  );
}
