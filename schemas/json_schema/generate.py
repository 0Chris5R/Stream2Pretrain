"""Regenerate the JSON Schema sidecars from the Pydantic v2 models.

Run with:

    uv run python -m schemas.json_schema.generate

Writes one ``<snake_case>.schema.json`` per model into this package directory.
The output is deterministic (sorted keys, trailing newline) so checked-in
copies diff cleanly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final

from pydantic import BaseModel

from schemas.bronze import BronzeRecord
from schemas.code import CodeFileRecord
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord
from schemas.scientific import ScientificDocument
from schemas.silver import SilverRecord
from schemas.sourcefeed import MixtureRecipeSpec, SourceFeedSpec

OUT_DIR: Final[Path] = Path(__file__).parent

EXPORTS: Final[tuple[tuple[type[BaseModel], str], ...]] = (
    (BronzeRecord, "bronze_record"),
    (SilverRecord, "silver_record"),
    (GoldRecord, "gold_record"),
    (ScientificDocument, "scientific_document"),
    (CodeFileRecord, "code_file_record"),
    (DeconAttestation, "decon_attestation"),
    (SourceFeedSpec, "source_feed_spec"),
    (MixtureRecipeSpec, "mixture_recipe_spec"),
)


def main() -> None:
    """Write all JSON Schemas to disk."""
    for model_cls, slug in EXPORTS:
        schema = model_cls.model_json_schema(mode="serialization")
        out_path = OUT_DIR / f"{slug}.schema.json"
        out_path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {out_path.relative_to(OUT_DIR.parent.parent)}")


if __name__ == "__main__":
    main()
