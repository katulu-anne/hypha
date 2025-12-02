---
name: Task
about: A specific, assignable unit of work (dev, docs, or ops)
title: "Task: "
labels: 'type: task'
assignees: ''

---

## Description

<!--
What specifically needs to be done?
Example: "Update the `hypha-inspect` CLI to support looking up peers by their secp256k1 public key."
-->

## Context

<!--
Link to the parent Epic or related RFC.
-->

## Acceptance Criteria

<!--
How do we know this is done?

- [ ] CLI accepts `--pubkey` argument.
- [ ] Output matches the format of `cert-info`.
- [ ] Unit tests covering the key conversion are passing.
-->

## Implementation Hints (Optional)

<!--
Pointers to relevant code paths or libraries.
Example: "See `crates/inspect/src/main.rs`. Use `libp2p::identity::Keypair::from_protobuf_encoding`."
-->