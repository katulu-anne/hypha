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
Please share any evidence you have collected that helps us understand, reproduce, and triage this bug.
The combination of this evidence and the steps above should allow us to rapidly identify if this is a new issue, a duplicate, or a configuration problem.

We welcome any context you can provide. This is not a strict checklist, but helpful examples include:
- **Logs:** (Tip: `RUST_LOG=debug` often captures the right level of detail)
- **Environment:** OS, Hypha version, deployment type (Local, Docker, Cloud).
- **Visuals:** Screenshots, recordings, or diagrams.
- **Diagnostics:** Output from `hypha-inspect lookup` or `probe`.

Feel free to share whatever helps us see what you see!
-->

```log
Paste logs here...
```
