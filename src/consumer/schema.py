"""PaySim / StreamGuard message schema contracts (P1-3).

The streaming consumer reads fields defensively with ``.get()`` so one bad
record can never take the loop down — but that means a schema change upstream
(deleted key, renamed field, wrong type) is silently swallowed. These contracts
flag drift so a contract break is caught instead of silently dropping fields.

The PaySim contract mirrors ``pyspark_consumer.PAYSIM_SCHEMA`` (and the Phase 6
API's ``TransactionPayload``). Note the PaySim field name is ``oldbalanceOrg``,
not ``oldbalanceOrig`` — a classic rename-drifty footgun.
"""

# (field -> expected python type), mirroring the Spark StructType.
PAYSIM_FIELD_TYPES = {
    "step": int,
    "type": str,
    "amount": float,
    "nameOrig": str,
    "oldbalanceOrg": float,
    "newbalanceOrig": float,
    "nameDest": str,
    "oldbalanceDest": float,
    "newbalanceDest": float,
    "isFraud": int,
    "isFlaggedFraud": int,
}

PAYSIM_TYPES = frozenset({"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"})

# Alert record published to the alerts topic by ``handle_transaction``.
ALERT_FIELD_TYPES = {
    "txn": dict,
    "fraud_probability": float,
    "threshold": float,
    "model_version": str,
}


def validate_paysim_txn(txn) -> list[str]:
    """Return a list of contract problems for a PaySim transaction (empty = valid)."""
    if not isinstance(txn, dict):
        return ["transaction is not a dict"]
    problems = []
    for field, expected in PAYSIM_FIELD_TYPES.items():
        if field not in txn:
            problems.append(f"missing field: {field}")
        elif txn[field] is not None and not isinstance(txn[field], expected):
            problems.append(
                f"field {field}: expected {expected.__name__}, "
                f"got {type(txn[field]).__name__}"
            )
    ttype = txn.get("type")
    if ttype is not None and ttype not in PAYSIM_TYPES:
        problems.append(f"unknown type: {ttype!r}")
    return problems


def validate_alert(alert) -> list[str]:
    """Return a list of contract problems for an alert record (empty = valid)."""
    if not isinstance(alert, dict):
        return ["alert is not a dict"]
    problems = []
    for field, expected in ALERT_FIELD_TYPES.items():
        if field not in alert:
            problems.append(f"missing field: {field}")
        elif not isinstance(alert[field], expected):
            problems.append(
                f"field {field}: expected {expected.__name__}, "
                f"got {type(alert[field]).__name__}"
            )
    return problems
