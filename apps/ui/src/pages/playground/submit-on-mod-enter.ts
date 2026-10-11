import type { KeyboardEvent } from "react";

/** ⌘/Ctrl + Enter in a textarea submits its form. */
export function submitOnModEnter(e: KeyboardEvent<HTMLTextAreaElement>) {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) e.currentTarget.form?.requestSubmit();
}
