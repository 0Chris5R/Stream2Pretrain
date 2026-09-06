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
        _segment("Training Details", "Training used AdamW for 12 epochs on org/research-data."),
        _segment("Evaluation Results", "Evaluation reports 84.2% accuracy on NamedBench."),
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
        _segment("Dataset Description", "This dataset contains 12,000 annotated samples."),
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


def test_long_technical_vocabulary_without_concrete_evidence_is_rejected() -> None:
    text = (
        "This model describes architecture, training, evaluation, inference, and limitations "
        "in polished but entirely generic terms without a source, measurement, revision, "
        "metric, or executable example. "
    ) * 12

    result = assess_hf_card(
        kind="model",
        title="Generic technical model",
        text=text,
        segments=[_segment("Overview", text)],
    )

    assert not result.accepted
    assert "insufficient_card_documentation" in result.categories


def test_concise_checkpoint_format_and_runtime_documentation_is_accepted() -> None:
    segments = [
        _segment(
            "ConceptLM checkpoint",
            "This 82M-parameter artifact stores SafeTensors with dedicated projection keys at "
            "revision 093f9f388b31de276ce2de164bdc2081324b9767. It loads through "
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


def test_synthetic_script_inventory_is_rejected() -> None:
    text = """A nano-scale implementation of poolformer for multitask tasks.
    Architecture: poolformer. Attention: flash. Fusion strategy: concat mlp.
    Task head: multitask. Initialization: orthogonal. Optimizer: rmsprop.
    inference.py is the main artifact of this repository."""

    result = assess_hf_card(
        kind="model",
        title="inference.py",
        text=text,
        segments=[_segment("Model Overview", text)],
    )

    assert not result.accepted
    assert "synthetic_script_card" in result.categories


def test_measured_concise_training_record_is_not_lost() -> None:
    text = (
        "A 27.3M-parameter BitNet model with a 4096-token context, trained from scratch on "
        "openbmb/Ultra-FineWeb-L1. The best validation checkpoint was step 3400 with loss "
        "4.480278; packed weights occupy 7 MB."
    )

    result = assess_hf_card(
        kind="model",
        title="Boopit 1",
        text=text,
        segments=[_segment("Boopit 1", text)],
    )

    assert result.accepted
    assert "grounding:measured" in result.evidence


def test_substantive_legacy_card_without_template_headings_is_accepted() -> None:
    text = (
        "BiMTokenizer is a bidirectional Mamba speech tokenizer for low-bitrate coding. "
        "Its 24-layer backbone was trained on org/speech-corpus with AdamW for 80 epochs. "
        "The released checkpoint contains 92M parameters and reaches 0.71 PESQ on VoiceBench. "
        "The encoder accepts 24 kHz waveforms and the decoder returns reconstructed audio. "
        "Evaluation scripts and immutable revision 27d67f1b5f57dc0953326b2601d68371d40ea8da "
        "are included for reproduction. Known limitations include noisy far-field speech."
    )

    result = assess_hf_card(
        kind="model",
        title="BiMTokenizer",
        text=text,
        segments=[_segment("BiMTokenizer", text)],
    )

    assert result.accepted
    assert (
        "substantive_technical_card" in result.categories
        or "dense_scientific_card" in result.categories
    )


def test_grounded_environment_dataset_card_without_rigid_headings_is_accepted() -> None:
    text = """Multi-turn tool-use environments pooled across seven training runs. The release
    contains 2,231 environments split across API orchestration, state modification,
    data retrieval, error recovery, tool selection, and multi-step workflows.

    Each manifest entry records its source run, model scale, recipe, skill, and
    training step. The capture covers 4-6% of each run and has no outcome labels.
    Use the project loader because the environments inherit base classes injected
    at runtime."""

    result = assess_hf_card(
        kind="dataset",
        title="SPADE generated environments",
        text=text,
        segments=[_segment("Layout", text)],
    )

    assert result.accepted


def test_minimal_checkpoint_listing_without_measurement_is_rejected() -> None:
    text = (
        "This repository contains the checkpoints for a training run. Available model files "
        "are model.gguf and an Ollama Modelfile. Use the latest checkpoint for inference."
    )

    result = assess_hf_card(
        kind="model",
        title="checkpoint archive",
        text=text,
        segments=[_segment("Files", text)],
    )

    assert not result.accepted
    assert "minimal_artifact_listing" in result.categories


def test_placeholder_paper_title_dataset_card_is_rejected() -> None:
    text = (
        "This should be a paper Title. This is the resource page of our collection. "
        "The dataset link and model links are listed below."
    )

    result = assess_hf_card(
        kind="dataset",
        title="Annoy: This should be a paper Title",
        text=text,
        segments=[_segment("Introduction", text)],
    )

    assert not result.accepted
    assert "stub_checkpoint_upload" in result.categories
