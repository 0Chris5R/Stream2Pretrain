"""Tests for :mod:`processor.operators.pii`."""

from __future__ import annotations

from processor.operators.pii import PiiScanner, is_valid_ipv4, luhn_ok


def test_luhn_validator() -> None:
    # Valid Visa test number from the standard ISO test set.
    assert luhn_ok("4111111111111111")
    # Random invalid digit string.
    assert not luhn_ok("4111111111111112")
    assert not luhn_ok("")
    assert not luhn_ok("abc")


def test_ipv4_validator() -> None:
    assert is_valid_ipv4("10.0.0.1")
    assert not is_valid_ipv4("10.0.0.999")
    assert not is_valid_ipv4("10.0.0")


def test_email_detection() -> None:
    flags = PiiScanner().flags("contact me at user@example.com please")
    assert "email" in flags


def test_credit_card_with_luhn() -> None:
    flags = PiiScanner().flags("card number 4111 1111 1111 1111 expires soon")
    assert "credit_card" in flags


def test_ssn_detection() -> None:
    flags = PiiScanner().flags("My SSN is 123-45-6789, please don't share.")
    assert "ssn" in flags


def test_ipv4_detection() -> None:
    flags = PiiScanner().flags("connect to 192.168.1.10 over ssh")
    assert "ipv4" in flags
    assert "ipv4" not in PiiScanner().blocking_flags("connect to 192.168.1.10 over ssh")


def test_scientific_tensor_shape_is_not_a_phone_number() -> None:
    scanner = PiiScanner(use_presidio=False)

    assert "phone" not in scanner.flags("The 2023-06 checkpoint uses tensors of size 32 32 32.")


def test_explicit_international_phone_number_is_blocking() -> None:
    scanner = PiiScanner(use_presidio=False)

    assert "phone" in scanner.blocking_flags("Telephone: +49 30 1234 5678")


def test_clean_text_has_no_flags(long_english_text: str) -> None:
    flags = PiiScanner().flags(long_english_text)
    assert flags == []


def test_credit_card_invalid_luhn_not_flagged() -> None:
    flags = PiiScanner().flags("number 1234 5678 9012 3456 is just text")
    assert "credit_card" not in flags
