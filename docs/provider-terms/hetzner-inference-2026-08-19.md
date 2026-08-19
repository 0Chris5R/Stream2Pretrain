# Hetzner Inference provider snapshot

Audited: 2026-08-19

Primary source: https://docs.hetzner.com/general/company-and-policy/experiments/inference/

Recorded operating facts:

- OpenAI-compatible API base: `https://inference.hetzner.com/api/v1`.
- Authenticated `GET /models` is authoritative for the account's current catalogue.
- The public experiment documentation lists `Qwen3.8-27B` with a 262,144-token context window and Apache-2.0 licence.
- Published per-key limits are 10 requests, 4,000,000 input tokens, and 100,000 output tokens per 60 seconds. The provider does not publish a daily cap.
- The experiment has no production availability guarantee.
- The selected Qwen route must remain pinned to the project's exact Apache-2.0 model-license registry and every returned model identifier is recorded.

This is an internal technical snapshot, not legal advice or a copy of the provider terms.
