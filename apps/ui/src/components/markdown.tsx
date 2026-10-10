import type { ReactNode } from "react";
import { CopyButton } from "@/components/copy-button";

// A deliberately small Markdown subset for model replies: fenced code,
// headings, lists, quotes, paragraphs, and inline code, bold, italics and
// links. It builds React elements, never HTML strings, and tolerates the
// half-finished text of a stream (an open fence renders as code).

type Block =
  | { type: "code"; lang: string; text: string }
  | { type: "heading"; level: number; text: string }
  | { type: "list"; ordered: boolean; items: string[] }
  | { type: "quote"; text: string }
  | { type: "paragraph"; text: string };

const listItem = /^\s*(?:[-*+]|(\d+)[.)])\s+(.*)$/;

function parseBlocks(source: string): Block[] {
  const lines = source.split("\n");
  const blocks: Block[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const fence = line.match(/^\s*```(\S*)/);
    if (fence) {
      const body: string[] = [];
      i += 1;
      while (i < lines.length && !/^\s*```\s*$/.test(lines[i])) body.push(lines[i++]);
      i += 1;
      blocks.push({ type: "code", lang: fence[1], text: body.join("\n") });
      continue;
    }
    const heading = line.match(/^(#{1,4})\s+(.*)$/);
    if (heading) {
      blocks.push({ type: "heading", level: heading[1].length, text: heading[2] });
      i += 1;
      continue;
    }
    if (listItem.test(line)) {
      const ordered = !!line.match(listItem)?.[1];
      const items: string[] = [];
      while (i < lines.length && listItem.test(lines[i])) items.push(lines[i++].match(listItem)![2]);
      blocks.push({ type: "list", ordered, items });
      continue;
    }
    if (line.startsWith(">")) {
      const body: string[] = [];
      while (i < lines.length && lines[i].startsWith(">")) body.push(lines[i++].replace(/^>\s?/, ""));
      blocks.push({ type: "quote", text: body.join("\n") });
      continue;
    }
    if (line.trim() === "") {
      i += 1;
      continue;
    }
    const body: string[] = [];
    while (
      i < lines.length &&
      lines[i].trim() !== "" &&
      !/^\s*```|^#{1,4}\s|^>/.test(lines[i]) &&
      !listItem.test(lines[i])
    ) {
      body.push(lines[i++]);
    }
    blocks.push({ type: "paragraph", text: body.join("\n") });
  }
  return blocks;
}

const inlinePattern = /(`[^`\n]+`)|(\*\*[^*\n]+\*\*)|(\*[^*\n]+\*|_[^_\n]+_)|(\[[^\]\n]+\]\(https?:\/\/[^)\s]+\))/g;

function renderInline(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  for (const m of text.matchAll(inlinePattern)) {
    const at = m.index;
    if (at > last) out.push(text.slice(last, at));
    const [token] = m;
    const key = `${at}`;
    if (m[1]) {
      out.push(
        <code key={key} className="rounded bg-muted px-1 py-0.5 text-[0.85em]">
          {token.slice(1, -1)}
        </code>,
      );
    } else if (m[2]) {
      out.push(<strong key={key}>{renderInline(token.slice(2, -2))}</strong>);
    } else if (m[3]) {
      out.push(<em key={key}>{renderInline(token.slice(1, -1))}</em>);
    } else {
      const [, label, href] = token.match(/^\[([^\]]+)\]\((.+)\)$/)!;
      out.push(
        <a key={key} href={href} target="_blank" rel="noreferrer" className="text-brand underline underline-offset-2">
          {label}
        </a>,
      );
    }
    last = at + token.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

function renderBlock(block: Block, key: number): ReactNode {
  switch (block.type) {
    case "code":
      return (
        <div key={key} className="group relative my-3 rounded-lg border bg-muted/50">
          {block.lang && (
            <div className="border-b px-3 py-1.5 font-mono text-[11px] text-muted-foreground">
              {block.lang}
            </div>
          )}
          <pre className="overflow-x-auto p-3 text-[12.5px] leading-relaxed">
            <code>{block.text}</code>
          </pre>
          <CopyButton
            value={block.text}
            label="Copy code"
            className="absolute top-1 right-1 opacity-0 group-hover:opacity-100 focus-visible:opacity-100"
          />
        </div>
      );
    case "heading":
      return (
        <p key={key} className={block.level <= 2 ? "mt-4 mb-2 text-base font-semibold" : "mt-3 mb-1 font-semibold"}>
          {renderInline(block.text)}
        </p>
      );
    case "list": {
      const Tag = block.ordered ? "ol" : "ul";
      return (
        <Tag key={key} className={`my-2 grid gap-1 pl-5 ${block.ordered ? "list-decimal" : "list-disc"}`}>
          {block.items.map((item, i) => (
            <li key={i}>{renderInline(item)}</li>
          ))}
        </Tag>
      );
    }
    case "quote":
      return (
        <blockquote key={key} className="my-2 border-l-2 pl-3 text-muted-foreground">
          {renderInline(block.text)}
        </blockquote>
      );
    case "paragraph":
      return (
        <p key={key} className="my-2 whitespace-pre-wrap first:mt-0 last:mb-0">
          {renderInline(block.text)}
        </p>
      );
  }
}

export function Markdown({ source }: { source: string }) {
  return <div className="text-sm leading-relaxed">{parseBlocks(source).map(renderBlock)}</div>;
}
