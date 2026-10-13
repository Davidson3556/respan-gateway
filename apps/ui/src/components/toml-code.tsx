import Prism from "prismjs";
import "prismjs/components/prism-toml";
import type { ReactNode } from "react";

function tokens(value: string | Prism.Token | (string | Prism.Token)[]): ReactNode {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map((part, i) => <span key={i}>{tokens(part)}</span>);
  return <span className={`token ${value.type}`}>{tokens(value.content)}</span>;
}

export function TomlCode({ code }: { code: string }) {
  return <span className="toml-code">{tokens(Prism.tokenize(code, Prism.languages.toml))}</span>;
}
