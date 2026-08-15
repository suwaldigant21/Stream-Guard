"""P2-3 — dbt guardrails: source/model naming rule + `dbt parse` in the gate.

Regression protection for the two Phase 4 incidents and the config they fixed:

1. **Ghost deletions (incidents 1–2):** a model name colliding with a source
   table name made dbt-athena purge the raw Bronze S3 directory on drop/replace.
   Rule: raw tables are **Sources, never models** — no model name may equal a
   source table name, and Bronze is referenced via ``source()``, never ``ref()``.
2. **Broken DAG (Jinja-in-comment, incident 3-side):** a ``dbt parse`` in the
   gate catches a broken project before it ever touches AWS.
3. **Config pins:** ``s3_data_dir`` stays isolated under ``dbt_data/``, the mart
   uses ``external_location`` (not ``location``), and Gold reads Silver via
   ``ref()`` — the exact fixes that resolved the gold-bucket incident.
"""

import subprocess
import sys
from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"
PROJECT_DIR = DBT_DIR.parent


def _dbt_exe():
    """The venv's dbt console script (uv-managed interpreters can lie about
    sys.executable, so fall back to the project venv)."""
    candidates = [
        Path(sys.executable).parent / "Scripts" / "dbt.exe",
        PROJECT_DIR / ".venv" / "Scripts" / "dbt.exe",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    raise RuntimeError(
        "dbt.exe not found — dbt-athena-community is a declared dependency; "
        "the gate requires it (install via `uv sync`)"
    )


def _load_yaml(rel):
    with open(DBT_DIR / rel) as f:
        return yaml.safe_load(f)


def _yml_data():
    for path in sorted((DBT_DIR / "models").rglob("*.yml")):
        yield path, yaml.safe_load(path.read_text())


def _source_table_names():
    tables = set()
    for _, data in _yml_data():
        for source in (data or {}).get("sources") or []:
            for table in source.get("tables") or []:
                tables.add(table["name"])
    return tables


def _model_names():
    names = set()
    for _, data in _yml_data():
        for model in (data or {}).get("models") or []:
            names.add(model["name"])
    return names


def test_raw_tables_are_sources_never_models():
    sources = _source_table_names()
    models = _model_names()
    collision = sorted(sources & models)
    assert not collision, (
        "model names must not collide with source table names "
        f"(ghost-deletion class): {collision}"
    )


def test_bronze_is_source_referenced_via_source_not_ref():
    assert "bronze_transactions" in _source_table_names()
    assert "bronze_transactions" not in _model_names()

    for sql in (DBT_DIR / "models").rglob("*.sql"):
        text = sql.read_text()
        if "bronze_transactions" in text:
            assert (
                "source('streamguard_db', 'bronze_transactions')" in text
            ), f"{sql.relative_to(DBT_DIR)} must read Bronze via source(), not ref()"
            assert "ref('bronze_transactions')" not in text


def test_gdpr_registry_is_source_never_model():
    # P1-2: the gdpr_requests registry is an S3-mapped external table — it must
    # obey the same ghost-deletion rule as Bronze (source, never a model).
    assert "gdpr_requests" in _source_table_names()
    assert "gdpr_requests" not in _model_names()


def test_stg_masks_erased_accounts():
    # P1-2: stg_transactions must LEFT JOIN the registry and project the alias
    # (never the raw id) for erased accounts at query time.
    sql = (DBT_DIR / "models" / "staging" / "stg_transactions.sql").read_text()
    assert "source('streamguard_db', 'gdpr_requests')" in sql
    assert "LEFT JOIN" in sql
    assert "account_alias" in sql
    assert "COALESCE" in sql


def test_output_isolation_config_pinned():
    project = _load_yaml("dbt_project.yml")
    marts = project["models"]["streamguard"]["marts"]
    assert "+external_location" in marts, (
        "mart config key must be `+external_location`, not `location` (incident 3)"
    )
    assert marts["+external_location"].startswith("s3://streamguard-gold-")

    profile = _load_yaml("profiles.yml")
    data_dir = profile["streamguard"]["outputs"]["dev"]["s3_data_dir"]
    assert data_dir.rstrip("/").endswith("/dbt_data"), (
        f"s3_data_dir must stay isolated under dbt_data/, got {data_dir}"
    )


def test_gold_model_reads_silver_via_ref():
    gold = (DBT_DIR / "models" / "marts" / "gold_transactions.sql").read_text()
    assert "ref('stg_transactions')" in gold


def test_dbt_parse_is_green():
    proc = subprocess.run(
        [str(_dbt_exe()), "parse", "--no-partial-parse"],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert proc.returncode == 0, (
        f"dbt parse failed (a broken DAG must never reach AWS):\n{proc.stdout}\n{proc.stderr}"
    )
    assert "WARNING" not in proc.stdout, f"dbt parse emitted warnings:\n{proc.stdout}"
