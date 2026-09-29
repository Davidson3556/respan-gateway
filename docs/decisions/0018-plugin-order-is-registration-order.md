# 0018. Plugin order is registration order

- Status: accepted
- Date: 2026-09-29

## Context

`Gateway::after(name, before)` let a plugin run after others whatever the
order of registration. The kernel kept a second list, `chain`, derived from
the registered plugins after each call, a cycle check (`runs_after`), and a
`Registered` record holding each plugin's `after` indexes. That is a
scheduler in a kernel that has no other policy (0001), and one order can
be had by registering plugins in it.

## Decision

1. **Registration order is the plugin order**, on both edges and for end
   hooks. The kernel keeps one `Vec` and no ordering solver.
2. **`Gateway::after` is removed**, and with it the cycle and unknown-name
   errors of the kernel. `Gateway::plugin(name, plugin)` is unchanged, and
   still fails `CONFIG` on a taken name. Names serve diagnostics.
3. **The binary's `after:` config key stays.** `config.rs` sorts the
   configured plugins before registering them: next is the earliest listed
   plugin whose `after` plugins are all placed. An unknown name or a cycle
   is a config error.
4. **The hazard, as the examples state it:** register any plugin that adds
   to the input BEFORE the plugin that checks or redacts it.

## Consequences

- Amends 0005 (one registry: `after` and the derived order) and 0003
  (safety "depends on the explicit order (`after`)": it depends on
  registration order).
- One public method fewer (`Gateway::after`); no new public types.
- Tests of the kernel's solver went; config's tests cover order, cycle and
  unknown name.
