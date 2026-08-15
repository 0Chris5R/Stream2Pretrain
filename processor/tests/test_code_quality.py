from processor.operators.code_quality import CodeQualityPolicy


def test_documented_parseable_python_scores_high() -> None:
    text = '''"""Small training example."""

class Counter:
    """Count observed values."""

    def __init__(self) -> None:
        self.value = 0

    def add(self, amount: int) -> int:
        # Preserve the new total for the caller.
        self.value += amount
        return self.value
'''
    result = CodeQualityPolicy().score(text * 3, path="src/counter.py")
    assert result.edu_score >= 4.0


def test_generated_or_minified_path_is_zero() -> None:
    result = CodeQualityPolicy().score("function x(){return 1;}" * 100, path="dist/app.min.js")
    assert result.edu_score == 0.0


def test_invalid_python_loses_syntax_signal() -> None:
    text = "# example\ndef broken(:\n    pass\n" * 10
    valid = CodeQualityPolicy().score(text.replace("broken(:", "working():"), path="a.py")
    invalid = CodeQualityPolicy().score(text, path="a.py")
    assert valid.edu_score > invalid.edu_score
