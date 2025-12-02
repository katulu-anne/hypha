---
name: Bug Report
about: Report a failure, crash, or unexpected behavior in Hypha
title: "Bug: "
labels: 'bug'
assignees: ''

---

## Description

<!--
Provide a concise summary of the bug.
Example: "The worker node fails to fetch datasets larger than 5GB when using the TCP transport."
-->

## Symptoms

<!--
Describe what you see. Include error messages, crash logs, or unexpected state changes.

Example:
- The worker log shows `StreamReset(Code(1))` repeatedly.
- `hypha-inspect probe` for /address/ times out.
- The scheduler reports the worker lease as "expired".
-->

## Steps to Reproduce

<!--
Detailed description of all steps you took, so others can reproduce the issue.

Example:
1. Start a gateway using this config:
    
    ```toml
    # Gateway config... 
    ```
2. Run a worker with `exclude_cidr = ["0.0.0.0/0"]`.
3. Submit a training job with a 6GB dataset.
4. Observe the worker logs.

IMPORTANT: While you should be as detailed as possible, avoid including sensitive information such as tokens, private keys or IP addresses!
-->

## Evidence

<!--
Please provide the following details to help us diagnose the issue:

**Environment:**
- **Hypha Version:** (e.g., `v0.1.0`, `git rev-parse HEAD`)
- **OS/Platform:** (e.g., Ubuntu 22.04, macOS Sonoma, Windows 11)
- **Deployment:** (e.g., Local binary, Docker, Kubernetes, AWS)

**Logs & Context:**
- Attach relevant logs. **Tip:** Run with `RUST_LOG=debug` or `RUST_LOG=hypha_worker=trace` to capture actionable details.
- If this is a connectivity issue, include output from:
  - `hypha-inspect lookup <peer-id>`
  - `hypha-inspect probe <address>`
- Screenshots or architectural diagrams if relevant.
-->

```log
Paste logs here...
```
