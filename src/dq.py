"""Bronze-layer DQ pass (P2-2) — light, non-exhaustive quality checks.

These run in the live streaming consumer (per transaction) and are mirrored by
the PySpark Bronze writer's quarantine filter. This module is the single source
of truth for the check field sets: the consumer applies them to dicts, the
Spark job builds equivalent column expressions from the same constants.

Scope is deliberately light (2–3 checks, not exhaustive) — enough to catch
upstream feed drift without becoming a second schema engine (schema-contract
checking lives in ``src.consumer.schema``).
"""

DQ_REQUIRED_FIELDS = ("step", "type", "amount", "nameOrig", "nameDest")
DQ_BALANCE_FIELDS = (
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
)


def dq_reject_reasons(txn) -> list[str]:
    """Return the list of DQ violations for a transaction (empty = clean)."""
    if not isinstance(txn, dict):
        return ["not a dict"]
    problems = []
    for field in DQ_REQUIRED_FIELDS:
        if txn.get(field) is None:
            problems.append(f"missing or null required field: {field}")
    amount = txn.get("amount")
    if isinstance(amount, (int, float)) and amount < 0:
        problems.append("negative amount")
    for field in DQ_BALANCE_FIELDS:
        value = txn.get(field)
        if isinstance(value, (int, float)) and value < 0:
            problems.append(f"negative balance: {field}")
    return problems


def is_dq_clean(txn) -> bool:
    """True when the transaction passes every DQ check."""
    return not dq_reject_reasons(txn)
