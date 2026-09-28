# respan-gateway-text

Read and rewrite the chat kind's text safely across stream deltas, and
refuse what cannot be read.

- `Readable` (for `Conversation` and `Reply`): `edit_all` runs an edit on
  every text, tools and tool-call JSON included, or fails with `UNREADABLE`
  on content with no text form, such as an image.
- `Holdback::new(rewrite)`: a `Reply` filter that holds back trailing text
  that may start a match, so a `Rewrite` sees each match whole.
  `partial_match` is the helper for term lists.
- `Placeholders`: `hide(label, value)` returns `<LABEL_n>`. It is a
  `Rewrite`, so `Holdback::new(placeholders)` restores values across deltas.

JSON inside a string, such as a tool call's arguments, is read string by
string, its numbers and keys too, so an edit may rename a key.

Not handled: a match split across streamed tool-argument fragments, or
across two text blocks (Anthropic's citations make many); Anthropic tool
blocks, which are refused as content with no text form.

Used by: [guardrail](../../examples/guardrail.rs), [pii](../../examples/pii.rs).
