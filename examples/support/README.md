# Private example support

`attachments/pdf.rs` holds the bounded PDF converter, also used by the separate
Poppler validation test.

These are source modules included by the runnable examples, not library or SDK
APIs: `billing/` holds accounting and ledger policies, `jobs/` holds worker and
store implementations, `live/` holds WebSocket serving, budget and test
helpers, `http.rs` holds local provider helpers, and `realtime/` holds audio handling. See the
[example index](../README.md) for their runnable entrypoints.

`text.rs` is shared by the guardrail, PII and realtime examples. Its four
regression suites are automatically discovered `respan-gateway-examples` test targets
(`text_*`).

Read and rewrite the chat kind's text safely across stream deltas, and
refuse what cannot be read.

- `Readable` (for `Conversation` and `Reply`): `edit_all` runs an edit on
  every text, tools and tool-call JSON included, or fails with `UNREADABLE`
  on content with no text form, such as an image. A `Reply::Delta` is read
  as plain text, so only a `Holdback` reads a `Delta::Arguments` as JSON
  text.
- `Holdback::new(rewrite)`: a `Reply` filter that holds back trailing
  text that may start a match, so a `Rewrite` sees each match whole. Text and
  reasoning deltas are text; a tool call's `Delta::Arguments` are JSON text,
  held per block, so a match split
  across them, escapes included, is seen whole and what replaces it is
  escaped. Held text is released before another block opens, at its
  `BlockEnd`, at the `Finish` and at the end. Events that show nothing yet
  (the start, a block opening) wait, in order, until one that shows
  something, so a refusal comes before any byte; a keep-alive or usage alone
  passes at once unless events wait before it. `partial_match` is the helper
  for term lists.
- `Placeholders`: `hide(label, value)` returns `<LABEL_n>`. It is a
  `Rewrite`, so `Holdback::new(placeholders)` restores values across deltas.

JSON inside a string, such as a tool call's arguments, is read string by
string, its numbers and keys too, so an edit may rename a key.

Order: input filters run in registration order, so register any plugin
that adds to the input BEFORE the plugin that checks or redacts it, or
its text goes unchecked.

Not handled: a match split across two text blocks (Anthropic's citations
make many); an edit of a bare number in a streamed tool-argument fragment,
which is written unquoted; several choices (the kind has one: an answer with
another choice is refused as unreadable). Tool calls and text tool results
are readable in requests; a tool result containing an image or another opaque
payload is refused.

Used by: [guardrail](../policies/guardrail.rs), [pii](../policies/pii.rs), [realtime](../realtime/realtime.rs).
