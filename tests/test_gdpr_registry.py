"""P1-2 — append-only GDPR registry seeding: alias determinism + row schema."""

from scripts.seed_gdpr_requests import build_request_row


def test_row_schema_is_append_only_registry_columns():
    row = build_request_row("C0000000001", "gdpr-suggestionfix-001")
    assert set(row) == {"account_id", "account_alias", "request_id", "requested_at"}
    assert row["account_id"] == "C0000000001"
    assert row["request_id"] == "gdpr-suggestionfix-001"
    assert isinstance(row["requested_at"], str) and "T" in row["requested_at"]


def test_alias_matches_the_p1_1_erasure_alias():
    # Determinism across the erasure path (src/api/gdpr.py via
    # scripts/anonymize_batch_gdpr.py) and the registry path — the SAME
    # account must map to the SAME alias so warehouse masking lines up with
    # the streaming fan-in purge (verified live during P1-1).
    assert build_request_row("C0000000001", "any")["account_alias"] == "ANON_fbad4ba4428e3311"


def test_alias_is_deterministic_and_salted_pattern():
    row_a = build_request_row("C0000000002", "req-1")
    row_b = build_request_row("C0000000002", "req-2")
    assert row_a["account_alias"] == row_b["account_alias"]
    alias = row_a["account_alias"]
    assert alias.startswith("ANON_")
    assert len(alias) == 5 + 16
    int(alias[5:], 16)  # hex suffix


def test_distinct_accounts_get_distinct_aliases():
    assert build_request_row("C0000000001", "r")["account_alias"] != (
        build_request_row("C0000000002", "r")["account_alias"]
    )
