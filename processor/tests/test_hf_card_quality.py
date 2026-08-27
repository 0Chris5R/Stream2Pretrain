"""Tests for the source-aware Hugging Face card structure gate."""

from processor.operators.hf_card_quality import assess_hf_card
from schemas.silver import SilverSegment


def _segment(title: str, text: str) -> SilverSegment:
    return SilverSegment(
        segment_id=title.lower().replace(" ", "-"),
        title=title,
        text=text,
        word_count=len(text.split()),
    )


def test_substantive_model_card_is_accepted() -> None:
    segments = [
        _segment("Model description", "This model uses a transformer architecture."),
        _segment("Training Details", "Training used AdamW and a documented learning rate."),
        _segment("Evaluation Results", "Evaluation reports accuracy on a named benchmark."),
    ]
    result = assess_hf_card(
        kind="model",
        title="Research model",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )

    assert result.accepted
    assert "dense_scientific_card" in result.categories


def test_template_model_card_is_rejected() -> None:
    segments = [_segment("Model description", "More information needed.")]

    result = assess_hf_card(
        kind="model",
        title="Checkpoint",
        text=segments[0].text,
        segments=segments,
    )

    assert not result.accepted
    assert "template_boilerplate" in result.categories


def test_dataset_card_requires_dataset_specific_substance() -> None:
    segments = [
        _segment("Dataset Description", "This dataset contains annotated scientific samples."),
        _segment("Data Fields", "Each instance contains text, label, and provenance fields."),
    ]

    result = assess_hf_card(
        kind="dataset",
        title="Scientific annotations",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )

    assert result.accepted
    assert "substantive_technical_card" in result.categories


def test_quantization_mirror_is_rejected() -> None:
    segments = [
        _segment(
            "About",
            "Static quants of a source checkpoint with weighted/imatrix quants of every size.",
        ),
        _segment("Usage", "Download the desired quantization file."),
    ]

    result = assess_hf_card(
        kind="model",
        title="Converted checkpoint",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )

    assert not result.accepted
    assert "stub_checkpoint_upload" in result.categories


def test_generated_trainer_shell_without_unique_evidence_is_rejected() -> None:
    segments = [
        _segment(
            "Training procedure",
            "This model is a fine-tuned version of a base model. It has been trained using TRL.",
        ),
        _segment("Framework versions", "Transformers 4 and Datasets 3."),
    ]

    result = assess_hf_card(
        kind="model",
        title="Uploaded checkpoint",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )

    assert not result.accepted
    assert "template_boilerplate" in result.categories


def test_generic_unidentified_benchmark_claims_are_rejected() -> None:
    segments = [
        _segment(
            "Introduction",
            "The model approaches other leading models after benchmark evaluations, including "
            "mathematics, programming, and general logic.",
        ),
        _segment("Evaluation Results", "The comparison reports Model1-v2 without provenance."),
    ]

    result = assess_hf_card(
        kind="model",
        title="MyAwesomeModel",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )

    assert not result.accepted
    assert "generic_marketing_benchmark" in result.categories


def test_concise_checkpoint_format_and_runtime_documentation_is_accepted() -> None:
    segments = [
        _segment(
            "ConceptLM checkpoint",
            "This artifact stores SafeTensors with dedicated projection keys. It loads through "
            "AutoModelForCausalLM and the vLLM backend with lossless key-conversion evidence.",
        )
    ]

    result = assess_hf_card(
        kind="model",
        title="ConceptLM checkpoint",
        text=segments[0].text,
        segments=segments,
    )

    assert result.accepted
    assert "substantive_technical_card" in result.categories
