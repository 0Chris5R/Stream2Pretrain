from __future__ import annotations

import pytest

from processor.foundry.providers import ProviderOutputError, _parse_json_content


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ('{"answer": 1}', {"answer": 1}),
        ("[1, 2, 3]", [1, 2, 3]),
        ('Here is the result:\n```json\n{"answer": 2}\n```', {"answer": 2}),
        (
            '<think>An example is {"draft": true}.</think>\nFinal answer: {"answer": 3}\nDone.',
            {"answer": 3},
        ),
    ],
)
def test_parse_json_content_accepts_complete_structured_root(
    content: str, expected: object
) -> None:
    assert _parse_json_content(content) == expected


@pytest.mark.parametrize("content", ['{"answer":', "plain text", "42"])
def test_parse_json_content_rejects_unusable_output(content: str) -> None:
    with pytest.raises(ProviderOutputError):
        _parse_json_content(content)
