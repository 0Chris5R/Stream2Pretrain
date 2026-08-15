"""HTML to plain-text extraction backed by Resiliparse.

Resiliparse is the DCLM-Baseline default extractor (Trafilatura runner-up).
It is roughly an order of magnitude faster than Trafilatura on real-world
HTML at comparable extraction quality - see arXiv 2602.19548.

This module wraps the library in a tiny, deterministic class so unit tests
can exercise it on fixture HTML and so the import-time cost is paid once per
worker. If Resiliparse is unavailable (e.g. dev shell without the wheel),
the wrapper falls back to a regex-based stripper that is intentionally
lossy: production paths must always carry the Resiliparse extra installed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resiliparse.parse.html import HTMLTree


_RESILIPARSE_VERSION = "0.14"
_FALLBACK_TAG = re.compile(r"<[^>]+>")
_FALLBACK_WS = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class ExtractedDocument:
    """Output of HTML extraction.

    ``extracted_with`` is the exact string written to the Silver record's
    ``extracted_with`` column. Keeping this verbatim ensures Iceberg snapshot
    consumers can replay the same extractor revision deterministically.
    """

    title: str | None
    text: str
    extracted_with: str


class ResiliparseExtractor:
    """Stateless wrapper around Resiliparse's plain-text extractor.

    Parameters
    ----------
    main_content
        If True (default), drops boilerplate using Resiliparse's mainContent
        heuristic. Matches DCLM-Baseline configuration.
    preserve_formatting
        Resiliparse "preserve formatting" toggle - keeps newlines that bracket
        paragraphs, which Gopher/C4 filters expect.
    """

    def __init__(
        self,
        *,
        main_content: bool = True,
        preserve_formatting: bool = True,
        allow_fallback: bool = True,
    ) -> None:
        self._main_content = main_content
        self._preserve_formatting = preserve_formatting
        self._available = self._probe_resiliparse()
        if not allow_fallback and not self._available:
            raise RuntimeError("Resiliparse is required when fallbacks are disabled")

    @staticmethod
    def _probe_resiliparse() -> bool:
        try:
            import resiliparse.extract.html2text
            import resiliparse.parse.html  # noqa: F401

            return True
        except Exception:
            return False

    @property
    def extracted_with(self) -> str:
        """Identifier persisted into ``SilverRecord.extracted_with``."""
        return f"resiliparse-{_RESILIPARSE_VERSION}" if self._available else "fallback-regex-0.1"

    def extract(self, html: str | bytes) -> ExtractedDocument:
        """Return plain text + best-effort title from a single HTML document."""
        if not html:
            return ExtractedDocument(title=None, text="", extracted_with=self.extracted_with)
        if self._available:
            return self._extract_resiliparse(html)
        return self._extract_fallback(html)

    def _extract_resiliparse(self, html: str | bytes) -> ExtractedDocument:
        """Resiliparse-backed implementation."""
        from resiliparse.extract.html2text import extract_plain_text
        from resiliparse.parse.html import HTMLTree

        if isinstance(html, bytes):
            tree: HTMLTree = HTMLTree.parse_from_bytes(html, "utf-8")
        else:
            tree = HTMLTree.parse(html)
        text = extract_plain_text(
            tree,
            main_content=self._main_content,
            preserve_formatting=self._preserve_formatting,
            list_bullets=False,
            alt_texts=False,
            links=False,
            form_fields=False,
            noscript=False,
        )
        title = _read_title(tree)
        return ExtractedDocument(
            title=title,
            text=text or "",
            extracted_with=self.extracted_with,
        )

    def _extract_fallback(self, html: str | bytes) -> ExtractedDocument:
        """Lossy regex fallback for environments without Resiliparse.

        Production never goes here - the Dockerfile always installs the wheel
        - but unit tests can run on plain CI.
        """
        if isinstance(html, bytes):
            html = html.decode("utf-8", errors="replace")
        title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else None
        text = _FALLBACK_TAG.sub(" ", html)
        text = _FALLBACK_WS.sub(" ", text).strip()
        return ExtractedDocument(title=title, text=text, extracted_with=self.extracted_with)


def _read_title(tree: HTMLTree) -> str | None:
    """Best-effort title extraction; returns None when document has none."""
    try:
        title_el = tree.title
        if title_el:
            stripped = title_el.strip()
            return stripped or None
    except Exception:
        return None
    return None
