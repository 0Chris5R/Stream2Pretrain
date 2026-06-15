"""``s2p-submit-api`` CLI entrypoint.

Wraps ``uvicorn`` so the container image has a single ENTRYPOINT.
"""

from __future__ import annotations

import os


def main() -> None:
    import uvicorn

    host = os.environ.get("S2P_BIND_HOST", "0.0.0.0")
    port = int(os.environ.get("S2P_BIND_PORT", "8000"))
    uvicorn.run(
        "ingest.submit_api.app:app",
        host=host,
        port=port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
