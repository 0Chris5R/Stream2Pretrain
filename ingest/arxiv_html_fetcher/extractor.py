"""Resiliparse-based extractor for arXiv native HTML and ar5iv mirror pages.

Both sources expose the same MathML-flavoured HTML5 contract: the paper body
lives inside ``<article>`` (arXiv 2026 layout) or ``<div class="ltx_page_main">``
(ar5iv / older arXiv). We rely on Resiliparse for boilerplate removal but
override its math handling so display math renders as ``$$...$$`` and inline
math renders as ``$...$``. Section headings keep their level (``# title``,
``## section``) so downstream extractors can recover document structure.

The extractor is deliberately defensive: every call returns an
:class:`ExtractedDocument`. If Resiliparse is unavailable (dev path without
the ``[fast]`` extra installed) a pure-stdlib fallback is used so unit tests
can run against frozen-input HTML samples without a heavy dependency.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser

# Pipeline tags emitted on the Bronze record. Kept short and dated so a
# replay can pin to the exact extractor revision.
ARXIV_PIPELINE = "arxiv-html-2026-06"
AR5IV_PIPELINE = "ar5iv-2026-06"

# arXiv per-paper licenses are surfaced via a ``<meta name="citation_license">``
# tag (or, on ar5iv, in a footer link). We map the most common URLs to SPDX
# ids; unmapped URLs are passed through verbatim so the curate stage can
# decide whether to risk-tier them.
_LICENSE_URL_TO_SPDX: dict[str, str] = {
    "http://arxiv.org/licenses/nonexclusive-distrib/1.0/": ("arxiv-non-exclusive-distribution"),
    "https://arxiv.org/licenses/nonexclusive-distrib/1.0/": ("arxiv-non-exclusive-distribution"),
    "http://creativecommons.org/licenses/by/4.0/": "CC-BY-4.0",
    "https://creativecommons.org/licenses/by/4.0/": "CC-BY-4.0",
    "http://creativecommons.org/licenses/by-sa/4.0/": "CC-BY-SA-4.0",
    "https://creativecommons.org/licenses/by-sa/4.0/": "CC-BY-SA-4.0",
    "http://creativecommons.org/licenses/by-nc/4.0/": "CC-BY-NC-4.0",
    "https://creativecommons.org/licenses/by-nc/4.0/": "CC-BY-NC-4.0",
    "http://creativecommons.org/licenses/by-nc-sa/4.0/": "CC-BY-NC-SA-4.0",
    "https://creativecommons.org/licenses/by-nc-sa/4.0/": "CC-BY-NC-SA-4.0",
    "http://creativecommons.org/publicdomain/zero/1.0/": "CC0-1.0",
    "https://creativecommons.org/publicdomain/zero/1.0/": "CC0-1.0",
}


@dataclass(slots=True)
class ExtractedDocument:
    """Structured result of an HTML extraction pass."""

    text: str
    title: str | None = None
    headings: list[str] = field(default_factory=list)
    spdx_license: str | None = None
    license_url: str | None = None
    submission_date: datetime | None = None
    extraction_pipeline: str = ARXIV_PIPELINE
    word_count: int = 0


def map_license_url(url: str | None) -> str | None:
    """Map an arXiv license URL to a known SPDX id; pass-through otherwise."""
    if not url:
        return None
    return _LICENSE_URL_TO_SPDX.get(url.strip(), url.strip())


def parse_submission_date(value: str | None) -> datetime | None:
    """Parse an arXiv ``citation_date`` / ``citation_publication_date`` string.

    arXiv emits ``YYYY/MM/DD`` here; older mirrors use ``YYYY-MM-DD``.
    Returns ``None`` when the value is absent or unparseable.
    """
    if not value:
        return None
    raw = value.strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y/%m", "%Y-%m"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


class _MetaLicenseParser(HTMLParser):
    """Pull ``<meta>`` and ``<title>`` tags without spinning up a real DOM."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title: str | None = None
        self.meta: dict[str, str] = {}
        self._in_title: bool = False
        self._title_buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title":
            self._in_title = True
            return
        if tag != "meta":
            return
        a = {k.lower(): (v or "") for k, v in attrs}
        name = a.get("name") or a.get("property") or ""
        content = a.get("content") or ""
        if name and content:
            self.meta[name.lower()] = content

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
            self.title = "".join(self._title_buf).strip() or None

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_buf.append(data)


def extract_metadata(html: str) -> tuple[str | None, dict[str, str]]:
    """Parse ``<title>`` and ``<meta>`` tags from ``html``."""
    p = _MetaLicenseParser()
    with suppress(Exception):
        p.feed(html)
    return p.title, p.meta


# Replace MathML with TeX-like delimiters. Resiliparse strips MathML by
# default; we recover it from inline annotations when present.
_INLINE_MATH = re.compile(
    r"<math[^>]*?display=\"inline\"[^>]*?>(.*?)</math>",
    re.IGNORECASE | re.DOTALL,
)
_DISPLAY_MATH = re.compile(
    r"<math[^>]*?display=\"block\"[^>]*?>(.*?)</math>",
    re.IGNORECASE | re.DOTALL,
)
_ANY_MATH = re.compile(r"<math[^>]*?>(.*?)</math>", re.IGNORECASE | re.DOTALL)
_ANNOTATION = re.compile(
    r"<annotation[^>]*?encoding=\"application/x-tex\"[^>]*?>(.*?)</annotation>",
    re.IGNORECASE | re.DOTALL,
)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t\f\v]+")
_BLANK_LINES = re.compile(r"\n{3,}")


def _math_to_tex(html: str, *, display: bool) -> str:
    """Replace a MathML element with ``$$...$$`` (display) or ``$...$`` (inline)."""
    ann = _ANNOTATION.search(html)
    body = ann.group(1).strip() if ann else _TAG.sub("", html).strip()
    body = body.replace("\n", " ").strip()
    if not body:
        return ""
    return f"\n\n$$\n{body}\n$$\n\n" if display else f" ${body}$ "


def preserve_math(html: str) -> str:
    """Rewrite MathML into TeX delimiters so the boilerplate stripper keeps them."""

    def _disp(m: re.Match[str]) -> str:
        return _math_to_tex(m.group(0), display=True)

    def _inl(m: re.Match[str]) -> str:
        return _math_to_tex(m.group(0), display=False)

    out = _DISPLAY_MATH.sub(_disp, html)
    out = _INLINE_MATH.sub(_inl, out)
    # Catch the ambiguous (no display attr) MathML; treat as inline.
    out = _ANY_MATH.sub(_inl, out)
    return out


def _resiliparse_extract(html: str) -> tuple[str, list[str]]:
    """Try the fast Resiliparse path, falling back to a stdlib stripper."""
    try:
        from resiliparse.extract.html2text import (  # type: ignore[import-not-found]
            extract_plain_text,
        )
    except Exception:
        text = _stdlib_extract(html)
        return text, _stdlib_headings(html)
    text = extract_plain_text(
        html,
        preserve_formatting=True,
        main_content=True,
        list_bullets=True,
        alt_texts=False,
        links=False,
        form_fields=False,
        noscript=False,
    )
    return text, _stdlib_headings(html)


def _stdlib_headings(html: str) -> list[str]:
    """Collect H1-H4 inner text. Used by both extractor branches."""
    out: list[str] = []
    for m in re.finditer(r"<h([1-4])\b[^>]*?>(.*?)</h\1>", html, flags=re.IGNORECASE | re.DOTALL):
        level = int(m.group(1))
        body = _TAG.sub("", m.group(2)).strip()
        body = re.sub(r"\s+", " ", body)
        if body:
            out.append(("#" * level) + " " + body)
    return out


def _stdlib_extract(html: str) -> str:
    """Tag-stripping fallback used when Resiliparse is unavailable."""
    body_match = (
        re.search(
            r"<article\b[^>]*?>(.*?)</article>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        or re.search(
            r"<div\b[^>]*?class=\"[^\"]*?ltx_page_main[^\"]*?\"[^>]*?>(.*?)</div>\s*</body>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        or re.search(r"<body\b[^>]*?>(.*?)</body>", html, flags=re.IGNORECASE | re.DOTALL)
    )
    body = body_match.group(1) if body_match else html
    body = re.sub(r"<script\b.*?</script>", "", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<style\b.*?</style>", "", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<nav\b.*?</nav>", "", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<footer\b.*?</footer>", "", body, flags=re.IGNORECASE | re.DOTALL)
    text = _TAG.sub("", body)
    text = _WS.sub(" ", text)
    lines = [ln.strip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln)
    return _BLANK_LINES.sub("\n\n", text).strip()


def extract_arxiv_html(
    html: str | bytes,
    *,
    pipeline: str = ARXIV_PIPELINE,
) -> ExtractedDocument:
    """Convert raw arXiv (or ar5iv) HTML into a structured :class:`ExtractedDocument`.

    The pipeline:

    1. Decode bytes to UTF-8 (best-effort, replacing invalid sequences).
    2. Pull ``<meta>`` and ``<title>`` first - those carry the license URL,
       submission date, and human-readable title.
    3. Substitute MathML elements with TeX delimiters so the boilerplate
       stripper preserves them.
    4. Run Resiliparse (or the stdlib fallback) against the rewritten HTML.
    """
    text_html = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html

    title, meta = extract_metadata(text_html)

    license_url = (
        meta.get("citation_license") or meta.get("dc.rights") or meta.get("license") or None
    )
    submission_date = parse_submission_date(
        meta.get("citation_publication_date") or meta.get("citation_date")
    )

    rewritten = preserve_math(text_html)
    body_text, headings = _resiliparse_extract(rewritten)
    body_text = _BLANK_LINES.sub("\n\n", body_text).strip()

    word_count = sum(1 for _ in re.finditer(r"\w+", body_text))

    return ExtractedDocument(
        text=body_text,
        title=title,
        headings=headings,
        spdx_license=map_license_url(license_url),
        license_url=license_url,
        submission_date=submission_date,
        extraction_pipeline=pipeline,
        word_count=word_count,
    )
