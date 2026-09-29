# 0021. The kind owns the fault split

- Status: accepted
- Date: 2026-09-29

## Context

An input a format cannot write is the client's fault (`bad_request`) if it
says what the client's bytes said, else a plugin's (`config`). 0020 had the
core carry that answer: every input reached `dial` as `(input, bool)`, the
bool set by comparing it with its decoding, and the format applied it with
`Fault::of_input`. Loop 1 kept the bool, since the core cannot apply the
split itself: an input fails where the format writes it, and an HTTP body's
frames reach the transport, not the gateway.

That argument shows only that the format must apply the split. It does not
show that the core must carry it. The bool cost the core a lot for one
comparison:

- `PartialEq` on every `Input`;
- a clone of every filtered input, kept to compare with;
- the internal `AnyFormat::same`;
- the public `Fault::of_input`;
- a tuple in both of `dial`'s input parameters, which no format written by
  hand used.

The kind already remembers what each value was decoded to: `relayed::Came`
keeps `said` next to the bytes, which is how a format writes an unchanged
value as its bytes. So "the client's as decoded" is `said == now`, the same
comparison the core made.

## Decision

1. **`dial` takes `opening: Option<&Input>` and `later:
   Option<Events<Input>>`.** No bool is passed with an input. `Format::Input`
   and `Codec::Input` lose `PartialEq`.
2. **The format classifies what it cannot write.** It fails with
   `bad_request` if the input says what the client's bytes said, else
   `config`. A kind that keeps its decoding tells them apart, as the
   formats crate's kinds do: `Relayed::whose(now, fault)` returns `fault`
   under the right code. The HTTP driver recodes nothing.
3. **Removed:**
   - from the core: `Fault::of_input`, `Fault::recoded`, `AnyFormat::same`
     and the clone per filtered input;
   - from the HTTP driver: `Whose`, and the flag carried beside each value
     in its writer.

## Consequences

- The split is classified by what the input says, not by the path it
  took. This changes two cases that the core used to call a plugin's:
  - a call's own input (`with_input`) that is the client's value, unchanged;
  - a client's input that a filter held back and released unchanged.

  Both are now the client's, as a translated call of the client's inputs
  already was. A plugin's edit is still `config`.
- A value a plugin replays from another request keeps that request's
  decoding, so the core would call its failure the client's. No use case
  does this.
- A kind that keeps no decoded copy cannot tell the two apart. It picks a
  code itself, and that choice is its own.
- A codec that does not classify its failure keeps the code it gave. The
  driver no longer turns it into `bad_request` or `config`.
- Relay does not change. It still goes by provenance, not value: a call
  with its own input never relays, however equal that input is.
- Pinned by: testkit `codec::r8`, `line::v9_1`, `custom::an_input_that_cannot_be_translated_*`,
  `translate::an_edited_request_that_cannot_be_translated_is_the_gateways_fault`,
  and formats `relayed` unit tests.
- Amends 0020 (reverses its loop-1 "kept" note on the bool) and 0004 (the
  `PartialEq` bound on a kind's types).
