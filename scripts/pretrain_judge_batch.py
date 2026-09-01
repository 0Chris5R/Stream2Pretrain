"""Build and submit GPT-5.6 Luna Batch requests for pretraining labels."""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

MODEL = "gpt-5.6-luna"
ENDPOINT = "/v1/responses"
PROMPT_REVISION = "pretrain-section-judge-v1"
MAX_BATCH_BYTES = 190_000_000
MAX_BATCH_REQUESTS = 45_000

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")

ARXIV_DEFECTS = (
    "none",
    "wrong_title",
    "author_or_affiliation_block",
    "references_or_bibliography",
    "acknowledgements_or_declarations",
    "template_or_stub",
    "truncated_or_incomplete",
    "table_or_figure_without_context",
    "repeated_content",
    "extraction_noise",
    "insufficient_context",
)
HF_DEFECTS = (
    "none",
    "placeholder_template",
    "quantization_or_checkpoint_mirror",
    "inventory_only",
    "access_only",
    "marketing_only",
    "insufficient_technical_content",
    "wrong_repository_type",
    "repeated_content",
    "extraction_noise",
)

ARXIV_PROMPT = """You are labeling scientific-paper text for training small, source-specific quality classifiers.

Evaluation date: {evaluation_date}

The input is the complete post-extraction text from Stream2Pretrain's unique processed full-text decision pool. It is exactly the population to be classified by the replacement learned model. No current route, licence, rejection, or classifier outcome has been used to select it. Do not assume it is high quality merely because it reached this stage.

Evaluate every supplied section relative to its actual role. An abstract should concisely state the problem, method, evidence, and result; an introduction should motivate and position the contribution; related work should synthesize rather than list citations; methods should be precise and reproducible; results should contain interpretable evidence and controlled comparisons; discussion and limitations should reason honestly about implications and failure modes; appendices should add substantive derivations, proofs, algorithms, or experimental detail. Do not penalize an abstract, introduction, conclusion, or limitations section merely for lacking equations when equations are not appropriate to that role.

For every section assign:
- pretrain_quality, integer 0-5: 0 unusable/no substantive content; 1 mostly noise, metadata, template, or unsupported fragments; 2 limited or weakly grounded utility; 3 solid usable scientific prose; 4 strong, precise, evidence-rich content; 5 exceptional, technically dense, coherent, and reusable scientific content.
- math_reasoning, integer 0-5: mathematical or quantitative reasoning value, not the mere presence of symbols. Reward explicit derivations, assumptions, intermediate steps, proofs, scaling relations, controlled quantitative inference, and equations tied to prose. Use 0 when mathematics is absent or irrelevant.
- posttrain_suitability, integer 0-5: potential to construct difficult, objectively checkable SFT or RL tasks from the supplied evidence. Reward multi-step derivations, causal/ablation reasoning, numerical reconstruction, assumption-consequence analysis, algorithmic dependencies, and grounded figure/table reasoning. Do not reward trivia, direct lookup, citation recall, or schema/ID completion.
- confidence from 0 to 1, concise rationale, and applicable defect flags.

Then provide whole-document scores based on the complete set of sections, not a blind mean. A severe extraction defect may reduce document quality; a small number of exceptionally useful sections may raise posttraining suitability. Return the section IDs that best support mathematical reasoning and posttraining tasks. Judge only supplied content and never infer missing figures, equations, tables, or references."""

HF_PROMPT = """You are labeling Hugging Face model-card and dataset-card text for a source-specific pretraining-quality classifier.

Evaluation date: {evaluation_date}

The input is the complete post-extraction README projection from Stream2Pretrain's unique processed full-text decision pool. It is exactly the population to be classified by the replacement learned model. No current route, licence, rejection, or classifier outcome has been used to select it. Do not assume it is high quality merely because it reached this stage.

Evaluate every section according to its purpose. Summaries should identify the artifact and its substantive contribution; architecture and data sections should provide concrete structure; training sections should give reproducible details; evaluation sections should name tasks, metrics, comparisons, and limitations; usage sections should provide technically meaningful instructions; limitations and bias sections should describe real constraints. Concise technical documentation may be excellent. Do not reward length, popularity, YAML metadata, repository names, generic templates, checkpoint inventories, quantization mirrors, marketing, access instructions, or copied boilerplate by themselves.

For every section assign pretrain_quality as an integer 0-5: 0 empty or unusable; 1 boilerplate, mirror, inventory, marketing, or almost no reusable information; 2 limited documentation with some concrete value; 3 solid technically useful documentation; 4 strong, grounded, reproducible technical content; 5 exceptional, precise, evidence-rich documentation with broad reusable value. Also return confidence from 0 to 1, a concise rationale, and applicable defect flags.

Then provide a whole-card pretrain_quality score based on the complete card, not a blind mean. Judge only supplied content and do not infer details from the repository name or linked artifacts."""


@dataclass(frozen=True)
class Section:
    section_id: str
    title: str
    section_type: str
    text: str


def _section_type(title: str, *, source: str) -> str:
    value = title.casefold()
    candidates: Sequence[tuple[str, Sequence[str]]]
    if source == "arxiv-html-fetcher":
        candidates = (
            ("abstract", ("abstract",)),
            ("introduction", ("introduction", "motivation")),
            ("background", ("background", "related work", "preliminar")),
            ("methods", ("method", "approach", "architecture", "algorithm", "model")),
            ("results", ("result", "experiment", "evaluation", "ablation", "analysis")),
            ("discussion", ("discussion",)),
            ("limitations", ("limitation", "ethic", "broader impact")),
            ("conclusion", ("conclusion",)),
            ("appendix", ("appendix", "supplement")),
        )
    else:
        candidates = (
            ("summary", ("summary", "description", "overview", "model card", "dataset card")),
            ("architecture", ("architecture", "model details")),
            ("training", ("training", "fine-tun")),
            ("evaluation", ("evaluation", "results", "benchmark", "performance")),
            ("usage", ("usage", "use", "inference", "how to")),
            ("data", ("dataset structure", "data fields", "data instances", "collection")),
            ("limitations", ("limitation", "bias", "risk", "out-of-scope")),
        )
    for role, markers in candidates:
        if any(marker in value for marker in markers):
            return role
    return "other"


def parse_sections(text: str, *, source: str) -> tuple[str | None, list[Section]]:
    title: str | None = None
    sections: list[Section] = []
    current_title = "Document body"
    current_lines: list[str] = []

    def flush() -> None:
        body = "\n".join(current_lines).strip()
        if not body:
            return
        section_id = f"section-{len(sections) + 1}"
        sections.append(
            Section(
                section_id=section_id,
                title=current_title,
                section_type=_section_type(current_title, source=source),
                text=body,
            )
        )

    for line in text.splitlines():
        match = _HEADING.match(line)
        if not match:
            current_lines.append(line)
            continue
        level = len(match.group(1))
        heading = match.group(2).strip()
        if level == 1 and title is None and not current_lines and not sections:
            title = heading
            current_title = heading
            continue
        flush()
        current_lines = []
        current_title = heading
    flush()
    if not sections and text.strip():
        sections.append(
            Section(
                section_id="section-1",
                title=title or "Document body",
                section_type="other",
                text=text.strip(),
            )
        )
    return title, sections


def _object(properties: Mapping[str, Any], required: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": dict(properties),
        "required": list(required),
        "additionalProperties": False,
    }


def response_schema(source: str) -> dict[str, Any]:
    common_section: dict[str, Any] = {
        "section_id": {"type": "string"},
        "pretrain_quality": {"type": "integer", "minimum": 0, "maximum": 5},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "defect_flags": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": list(ARXIV_DEFECTS if source == "arxiv-html-fetcher" else HF_DEFECTS),
            },
        },
        "rationale": {"type": "string"},
    }
    common_document: dict[str, Any] = {
        "pretrain_quality": {"type": "integer", "minimum": 0, "maximum": 5},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "rationale": {"type": "string"},
    }
    if source == "arxiv-html-fetcher":
        common_section.update(
            {
                "math_reasoning": {"type": "integer", "minimum": 0, "maximum": 5},
                "posttrain_suitability": {"type": "integer", "minimum": 0, "maximum": 5},
            }
        )
        common_document.update(
            {
                "math_reasoning": {"type": "integer", "minimum": 0, "maximum": 5},
                "posttrain_suitability": {"type": "integer", "minimum": 0, "maximum": 5},
                "best_math_section_ids": {"type": "array", "items": {"type": "string"}},
                "best_posttrain_section_ids": {"type": "array", "items": {"type": "string"}},
            }
        )
    section_required = list(common_section)
    document_required = list(common_document)
    return _object(
        {
            "document_id": {"type": "string"},
            "document_labels": _object(common_document, document_required),
            "sections": {"type": "array", "items": _object(common_section, section_required)},
        },
        ("document_id", "document_labels", "sections"),
    )


def build_request(row: Mapping[str, Any], *, index: int, evaluation_date: str) -> dict[str, Any]:
    source = str(row["source_feed"])
    title, sections = parse_sections(str(row["text"]), source=source)
    if not sections:
        raise ValueError(f"{row['doc_id']} has no non-empty sections")
    payload = {
        "evaluation_date": evaluation_date,
        "document_id": row["doc_id"],
        "source_feed": source,
        "source_format": row.get("source_format"),
        "title": title,
        "complete_input": True,
        "pipeline": {
            "valid_from": row.get("valid_from"),
            "tokens": row.get("tokens"),
            "projection_version": row.get("projection_version"),
            "scoring_version": row.get("scoring_version"),
        },
        "sections": [section.__dict__ for section in sections],
    }
    prompt = ARXIV_PROMPT if source == "arxiv-html-fetcher" else HF_PROMPT
    custom_source = "arxiv" if source == "arxiv-html-fetcher" else "hf"
    digest = hashlib.sha256(str(row["doc_id"]).encode()).hexdigest()[:16]
    return {
        "custom_id": f"{custom_source}-{index:05d}-{digest}",
        "method": "POST",
        "url": ENDPOINT,
        "body": {
            "model": MODEL,
            "reasoning": {"effort": "none"},
            "store": False,
            "max_output_tokens": 16_384,
            "input": [
                {
                    "role": "developer",
                    "content": [
                        {
                            "type": "input_text",
                            "text": prompt.format(evaluation_date=evaluation_date),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                        }
                    ],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "arxiv_training_labels"
                    if source == "arxiv-html-fetcher"
                    else "hf_training_labels",
                    "strict": True,
                    "schema": response_schema(source),
                }
            },
        },
    }


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not a JSON object")
            yield value


def prepare(input_path: Path, output_dir: Path, *, evaluation_date: str) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_index = 0
    request_count = 0
    batch_count = 0
    batch_bytes = 0
    handle: Any = None
    files: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    source_tokens: dict[str, int] = {}

    def close_file() -> None:
        nonlocal handle, batch_count, batch_bytes
        if handle is None:
            return
        path = Path(handle.name)
        handle.close()
        files.append(
            {
                "path": path.name,
                "requests": batch_count,
                "bytes": batch_bytes,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
        handle = None
        batch_count = 0
        batch_bytes = 0

    for row in _iter_jsonl(input_path):
        source = str(row["source_feed"])
        source_counts[source] = source_counts.get(source, 0) + 1
        source_tokens[source] = source_tokens.get(source, 0) + int(row.get("tokens") or 0)
        request = build_request(row, index=request_count + 1, evaluation_date=evaluation_date)
        encoded = (json.dumps(request, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        if len(encoded) > MAX_BATCH_BYTES:
            raise ValueError(f"single request exceeds batch size: {row['doc_id']}")
        if (
            handle is None
            or batch_count >= MAX_BATCH_REQUESTS
            or batch_bytes + len(encoded) > MAX_BATCH_BYTES
        ):
            close_file()
            batch_index += 1
            handle = (output_dir / f"judge-{batch_index:03d}.jsonl").open("wb")
        handle.write(encoded)
        batch_count += 1
        batch_bytes += len(encoded)
        request_count += 1
    close_file()
    manifest = {
        "schema_version": "pretrain-judge-batch-v1",
        "created_at": datetime.now(UTC).isoformat(),
        "evaluation_date": evaluation_date,
        "model": MODEL,
        "endpoint": ENDPOINT,
        "prompt_revision": PROMPT_REVISION,
        "requests": request_count,
        "source_counts": source_counts,
        "source_tokens": source_tokens,
        "files": files,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def _api_request(request: urllib.request.Request, *, attempts: int = 6) -> dict[str, Any]:
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                payload = json.load(response)
                if not isinstance(payload, dict):
                    raise RuntimeError("OpenAI API returned a non-object response")
                return payload
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            if exc.code not in {408, 409, 429, 500, 502, 503, 504} or attempt + 1 == attempts:
                raise RuntimeError(f"OpenAI API returned {exc.code}: {body[:1000]}") from exc
        except urllib.error.URLError as exc:
            if attempt + 1 == attempts:
                raise RuntimeError(f"OpenAI API unavailable: {exc.reason}") from exc
        time.sleep(min(2**attempt, 30))
    raise AssertionError("unreachable")


def _auth_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def upload_file(path: Path, *, api_key: str) -> dict[str, Any]:
    boundary = f"----s2p-{uuid.uuid4().hex}"
    mime = mimetypes.guess_type(path.name)[0] or "application/jsonl"
    prefix = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n'
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode()
    suffix = f"\r\n--{boundary}--\r\n".encode()
    body = prefix + path.read_bytes() + suffix
    request = urllib.request.Request(
        "https://api.openai.com/v1/files",
        data=body,
        headers={
            **_auth_headers(api_key),
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    return _api_request(request)


def retrieve_file(file_id: str, *, api_key: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"https://api.openai.com/v1/files/{file_id}",
        headers=_auth_headers(api_key),
        method="GET",
    )
    return _api_request(request)


def wait_for_file(
    file_id: str,
    *,
    api_key: str,
    timeout_seconds: float = 900,
    poll_seconds: float = 2,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        file = retrieve_file(file_id, api_key=api_key)
        status = str(file.get("status") or "")
        if status == "processed":
            return file
        if status in {"error", "failed", "cancelled"}:
            raise RuntimeError(f"OpenAI input file {file_id} entered status {status}")
        time.sleep(poll_seconds)
    raise TimeoutError(f"OpenAI input file {file_id} was not processed within the deadline")


def create_batch(*, input_file_id: str, api_key: str, batch_index: int) -> dict[str, Any]:
    body = {
        "input_file_id": input_file_id,
        "endpoint": ENDPOINT,
        "completion_window": "24h",
        "metadata": {
            "project": "stream2pretrain",
            "purpose": "classifier-labels",
            "prompt_revision": PROMPT_REVISION,
            "batch_index": str(batch_index),
        },
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/batches",
        data=json.dumps(body).encode(),
        headers={**_auth_headers(api_key), "Content-Type": "application/json"},
        method="POST",
    )
    return _api_request(request)


def batch_status(batch_ids: Sequence[str], *, api_key: str) -> dict[str, Any]:
    batches: list[dict[str, Any]] = []
    for batch_id in batch_ids:
        request = urllib.request.Request(
            f"https://api.openai.com/v1/batches/{batch_id}",
            headers=_auth_headers(api_key),
            method="GET",
        )
        batch = _api_request(request)
        batches.append(
            {
                "batch_id": batch["id"],
                "status": batch["status"],
                "request_counts": batch.get("request_counts"),
                "created_at": batch.get("created_at"),
                "in_progress_at": batch.get("in_progress_at"),
                "completed_at": batch.get("completed_at"),
                "expires_at": batch.get("expires_at"),
                "output_file_id": batch.get("output_file_id"),
                "error_file_id": batch.get("error_file_id"),
                "errors": batch.get("errors"),
            }
        )
    return {"batches": batches}


def submit(input_dir: Path, output_path: Path, *, api_key: str) -> dict[str, Any]:
    manifest = json.loads((input_dir / "manifest.json").read_text())
    submitted: list[dict[str, Any]] = []
    for index, item in enumerate(manifest["files"], start=1):
        path = input_dir / item["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"batch file changed after preparation: {path}")
        uploaded = upload_file(path, api_key=api_key)
        input_file_id = str(uploaded["id"])
        wait_for_file(input_file_id, api_key=api_key)
        batch = create_batch(
            input_file_id=input_file_id,
            api_key=api_key,
            batch_index=index,
        )
        submitted.append(
            {
                "batch_id": batch["id"],
                "status": batch["status"],
                "input_file_id": input_file_id,
                "requests": item["requests"],
                "sha256": item["sha256"],
            }
        )
    result = {**manifest, "submitted_at": datetime.now(UTC).isoformat(), "batches": submitted}
    output_path.write_text(json.dumps(result, indent=2) + "\n")
    return result


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--input", type=Path, required=True)
    prepare_parser.add_argument("--output-dir", type=Path, required=True)
    prepare_parser.add_argument("--evaluation-date", default=date.today().isoformat())
    submit_parser = subparsers.add_parser("submit")
    submit_parser.add_argument("--input-dir", type=Path, required=True)
    submit_parser.add_argument("--output", type=Path, required=True)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--batch-id", action="append", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv or sys.argv[1:])
    if args.command == "prepare":
        result = prepare(args.input, args.output_dir, evaluation_date=args.evaluation_date)
    else:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required")
        if args.command == "submit":
            result = submit(args.input_dir, args.output, api_key=api_key)
        else:
            result = batch_status(args.batch_id, api_key=api_key)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
