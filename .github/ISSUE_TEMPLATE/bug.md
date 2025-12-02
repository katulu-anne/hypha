---
name: Bug Report
about: Report a failure signature, crash, or unexpected behavior in Hypha
title: "Bug: "
labels: 'bug'
assignees: ''

---

## Description

<!--
Provide a concise summary of the bug.
Example: "The worker node fails to sync datasets larger than 5GB when using the QUIC transport."
-->

## Symptoms

<!--
Describe what you see. Include error messages, crash logs, or unexpected state changes.

Example:
- The worker log shows `StreamReset(Code(1))` repeatedly.
- `hypha-inspect probe` times out.
- The scheduler reports the worker as "unhealthy".
-->

## Steps to Reproduce

<!--
How can we trigger this issue?

1. Start a gateway and scheduler.
2. Run a worker with `exclude_cidr = ["0.0.0.0/0"]`.
3. Submit a training job with a 6GB dataset.
4. Observe the worker logs.
-->

## Environment

<!--
- **Hypha Version**: (e.g. `v0.1.0` or commit hash)
- **OS/Platform**: (e.g. Ubuntu 22.04, macOS Sonoma)
- **Deployment**: (e.g. Local, AWS, Docker Compose)
-->

## Logs & Evidence

<!--
Please attach relevant logs. 
Tip: Run with `RUST_LOG=debug` or `RUST_LOG=hypha_worker=trace` to capture details.
If applicable, include output from `hypha-inspect lookup <peer-id>` or `hypha-inspect probe`.
-->

```log
Paste logs here...
```

## Possible Fix (Optional)

<!--
If you have identified the root cause (e.g., "Kademlia DHT entry expiration is too short"), describe it here.
-->